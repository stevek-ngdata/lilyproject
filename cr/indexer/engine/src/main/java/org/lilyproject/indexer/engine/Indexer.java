/*
 * Copyright 2010 Outerthought bvba
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lilyproject.indexer.engine;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrInputDocument;
import org.lilyproject.indexer.derefmap.DependencyEntry;
import org.lilyproject.indexer.derefmap.DerefMap;
import org.lilyproject.indexer.model.indexerconf.DynamicFieldNameTemplateResolver;
import org.lilyproject.indexer.model.indexerconf.DynamicIndexField;
import org.lilyproject.indexer.model.indexerconf.DynamicIndexField.DynamicIndexFieldMatch;
import org.lilyproject.indexer.model.indexerconf.IndexCase;
import org.lilyproject.indexer.model.indexerconf.IndexerConf;
import org.lilyproject.indexer.model.sharding.ShardSelectorException;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.IdRecord;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.RecordNotFoundException;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.RepositoryManager;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.api.ValueType;
import org.lilyproject.repository.api.VersionNotFoundException;
import org.lilyproject.repository.impl.id.AbsoluteRecordIdImpl;
import org.lilyproject.util.repo.SystemFields;
import org.lilyproject.util.repo.VTaggedRecord;


// IMPORTANT: each call to solrClient should be followed by a corresponding metrics update.

/**
 * The Indexer adds records to, or removes records from, the index.
 */
public class Indexer {
    private final String indexName;
    private final IndexerConf conf;
    private final RepositoryManager repositoryManager;
    private final TypeManager typeManager;
    private final SystemFields systemFields;
    private final SolrShardManager solrShardMgr;
    private final IndexLocker indexLocker;
    private final ValueEvaluator valueEvaluator;
    private final IndexerMetrics metrics;

    /**
     * Deref map used to store dependencies introduced via dereference expressions. It is <code>null</code> in case the
     * indexer configuration doesn't contain dereference expressions or the index was configured not to maintain a deref
     * map.
     */
    private final DerefMap derefMap;

    private final Log log = LogFactory.getLog(getClass());

    public Indexer(String indexName, IndexerConf conf, RepositoryManager repositoryManager, SolrShardManager solrShardMgr,
                   IndexLocker indexLocker, IndexerMetrics metrics, DerefMap derefMap) {
        this.indexName = indexName;
        this.conf = conf;
        this.repositoryManager = repositoryManager;
        this.solrShardMgr = solrShardMgr;
        this.indexLocker = indexLocker;
        this.typeManager = repositoryManager.getTypeManager();
        this.systemFields = SystemFields.getInstance(typeManager, repositoryManager.getIdGenerator());
        this.valueEvaluator = new ValueEvaluator(conf);
        this.metrics = metrics;
        this.derefMap = derefMap;
    }

    public IndexerConf getConf() {
        return conf;
    }

    public String getIndexName() {
        return indexName;
    }

    /**
     * Performs a complete indexing of the given record, supposing the record is not yet indexed
     * (existing entries are not explicitly removed).
     *
     * <p>This method requires you obtained the {@link IndexLocker} for the record.
     *
     * @param recordId
     * @throws IOException
     */
    public void index(String table, RecordId recordId) throws RepositoryException, SolrClientException,
            ShardSelectorException, InterruptedException, IOException {

        VTaggedRecord vtRecord = new VTaggedRecord(recordId, repositoryManager.getRepository(table));
        IdRecord record = vtRecord.getRecord();

        IndexCase indexCase = conf.getIndexCase(table, record);
        index(table, vtRecord, record);
    }

    public void index(String table, IdRecord idRecord) throws RepositoryException, SolrClientException,
            ShardSelectorException, InterruptedException, IOException {
        VTaggedRecord vtRecord = new VTaggedRecord(idRecord, null, repositoryManager.getRepository(table));
        index(table, vtRecord, idRecord);
    }

    private void index(String table, VTaggedRecord vtRecord, IdRecord record) throws RepositoryException, SolrClientException,
            ShardSelectorException, InterruptedException, IOException {
        IndexCase indexCase = conf.getIndexCase(table, record);
        if (indexCase == null) {
            return;
        }

        index(table, vtRecord, retainExistingVtagsOnly(indexCase.getVersionTags(), vtRecord));
    }

    void index(String table, IdRecord idRecord, Set<SchemaId> vtags)
            throws RepositoryException, IOException, ShardSelectorException, SolrClientException, InterruptedException {
        final VTaggedRecord vtRecord = new VTaggedRecord(idRecord, null, repositoryManager.getRepository(table));

        Set<SchemaId> vtagsToIndex = retainExistingVtagsOnly(vtags, vtRecord);

        index(table, vtRecord, vtagsToIndex);
    }

    /**
     * Indexes a record for a set of vtags.
     *
     * <p>This method requires you obtained the {@link IndexLocker} for the record.
     *
     * @param vtagsToIndex all vtags for which to index the record, not all vtags need to exist on the record,
     *                     but this should only contain appropriate vtags as defined by the IndexCase for this record.
     */
    protected void index(String table, VTaggedRecord vtRecord, Set<SchemaId> vtagsToIndex)
            throws RepositoryException, ShardSelectorException, InterruptedException, SolrClientException, IOException {

        RecordId recordId = vtRecord.getId();

        // One version might have multiple vtags, so to index we iterate the version numbers
        // rather than the vtags
        Map<Long, Set<SchemaId>> vtagsToIndexByVersion = getVtagsByVersion(vtagsToIndex, vtRecord.getVTags());
        for (Map.Entry<Long, Set<SchemaId>> entry : vtagsToIndexByVersion.entrySet()) {
            IdRecord version = null;
            try {
                version = vtRecord.getIdRecord(entry.getKey());
            } catch (VersionNotFoundException e) {
                // ok
            } catch (RecordNotFoundException e) {
                // ok
            }

            if (version == null) {
                // If the version does not exist, we pro-actively delete it, though the IndexUpdater should
                // do this any way when it later receives a message about the delete.
                for (SchemaId vtag : entry.getValue()) {
                    verifyLock(recordId);
                    solrShardMgr.getSolrClient(recordId).deleteById(getIndexId(table, recordId, vtag));
                    metrics.deletesById.inc();
                }

                if (log.isDebugEnabled()) {
                    log.debug(String.format("Record %1$s, version %2$s: does not exist, deleted index" +
                            " entries for vtags %3$s", recordId, entry.getKey(),
                            vtagSetToNameString(entry.getValue())));
                }
            } else {
                index(table, version, entry.getKey(), entry.getValue());
            }
        }
    }

    /**
     * The actual indexing: maps record fields to index fields, and send to Solr.
     *
     * @param record  the correct version of the record, which has the versionTag applied to it
     * @param version version of the record, for the nonversioned case this is 0 so is not necessarily the same as
     *                record.getVersion().
     * @param vtags   the version tags under which to index
     */
    protected void index(String table, IdRecord record, long version, Set<SchemaId> vtags)
            throws ShardSelectorException, RepositoryException, InterruptedException, SolrClientException, IOException {
        verifyLock(record.getId());

        // Note that it is important the the indexFields are evaluated in order, since multiple
        // indexFields can have the same name and the order of values for multi-value fields can be important.
        //
        // The value of the indexFields is re-evaluated for each vtag. It is only the value of
        // deref-values which can change from vtag to vtag, so we could optimize this by only
        // evaluating those after the first run, but again because we want to maintain order and
        // because a deref-field could share the same name with a non-deref field, we simply
        // re-evaluate all fields for each vtag.
        for (SchemaId vtag : vtags) {

            SolrDocumentBuilder solrDocumentBuilder =
                    new SolrDocumentBuilder(repositoryManager, getConf().getRecordFilter(), systemFields, valueEvaluator,
                            table, record, getIndexId(table, record.getId(), vtag), vtag, version);

            // By convention/definition, we first evaluate the static index fields and then the dynamic ones

            //
            // 1: evaluate the static index fields
            //
            conf.getIndexFields().collectIndexUpdate(solrDocumentBuilder);

            //
            // 2: evaluate dynamic index fields
            //
            if (!conf.getDynamicFields().isEmpty()) {
                for (Map.Entry<SchemaId, Object> field : record.getFieldsById().entrySet()) {
                    FieldType fieldType = typeManager.getFieldTypeById(field.getKey());
                    for (DynamicIndexField dynField : conf.getDynamicFields()) {
                        DynamicIndexFieldMatch match = dynField.matches(fieldType);
                        if (match.match) {
                            String fieldName = evalName(dynField, match, fieldType);

                            List<String> values = valueEvaluator.format(table, record, fieldType, dynField.extractContext(),
                                    dynField.getFormatter(), repositoryManager);

                            solrDocumentBuilder.addField(fieldName, values);

                            if (!dynField.getContinue()) {
                                // stop on first match, unless continue attribute is true
                                break;
                            }
                        }
                    }
                }
            }

            if (solrDocumentBuilder.isEmptyDocument()) {
                // No single field was added to the Solr document.
                // In this case we do not add it to the index.
                // Besides being somewhat logical, it should also be noted that if a record would not contain
                // any (modified) fields that serve as input to indexFields, we would never have arrived here
                // anyway. It is only because some fields potentially would resolve to a value (potentially:
                // because with deref-expressions we are never sure) that we did.

                // There can be a previous entry in the index which we should try to delete
                solrShardMgr.getSolrClient(record.getId()).deleteById(getIndexId(table, record.getId(), vtag));
                metrics.deletesById.inc();

                if (log.isDebugEnabled()) {
                    log.debug(String.format("Record %1$s, vtag %2$s: no index fields produced output, " +
                            "removed from index if present", record.getId(), safeLoadTagName(vtag)));
                }

                processDependencies(table, record, vtag, solrDocumentBuilder);
            } else {
                SolrInputDocument solrDoc = solrDocumentBuilder.build();

                processDependencies(table, record, vtag, solrDocumentBuilder);

                log.debug("index response " + solrShardMgr.getSolrClient(record.getId()).add(solrDoc).toString());
                metrics.adds.inc();

                if (log.isDebugEnabled()) {
                    log.debug(String.format("Record %1$s, vtag %2$s: indexed, doc = %3$s", record.getId(),
                            safeLoadTagName(vtag), solrDoc));
                }
            }
        }
    }

    private void processDependencies(String table, IdRecord record, SchemaId vtag, SolrDocumentBuilder solrDocumentBuilder)
            throws IOException, RepositoryException, InterruptedException {
        if (log.isDebugEnabled()) {
            log.debug("Constructed Solr doc: " + solrDocumentBuilder.build());
            log.debug("Updating dependencies for " + record.getId());
            logDependencies(record.getId(), solrDocumentBuilder.getDependencies());
        }

        if (derefMap != null) {
            derefMap.updateDependants(new AbsoluteRecordIdImpl(table, record.getId()), vtag, solrDocumentBuilder.getDependencies());
        }
    }

    private void logDependencies(RecordId recordId, Map<DependencyEntry, Set<SchemaId>> dependencies) {
        for (DependencyEntry entry : dependencies.keySet()) {
            StringBuilder line = new StringBuilder();
            line.append(recordId).append(" | ")
                    .append(entry.getDependency())
                    .append("|").append(entry.getMoreDimensionedVariants())
                    .append("|").append(dependencies.get(entry));
            log.debug(line.toString());
        }
    }

    // FIXME: merge with SolrDocumentBuilder.resolve(NameTemplate()?
    private String evalName(DynamicIndexField dynField, DynamicIndexFieldMatch match, FieldType fieldType) {
        // Calculate the name, then add the value
        Map<String, Object> nameContext = new HashMap<String, Object>();
        nameContext.put("namespace", fieldType.getName().getNamespace());
        nameContext.put("name", fieldType.getName().getName());

        ValueType valueType = fieldType.getValueType();
        nameContext.put("type", formatValueTypeName(valueType));
        nameContext.put("baseType", valueType.getBaseName().toLowerCase());

        // If there's no nested value type, revert to the current value type. This is practical for dynamic
        // fields that match on types like "*,LIST<+>".
        ValueType nestedValueType = valueType.getNestedValueType() != null ? valueType.getNestedValueType() : valueType;
        nameContext.put("nestedType", formatValueTypeName(nestedValueType));
        nameContext.put("nestedBaseType", nestedValueType.getBaseName().toLowerCase());

        nameContext.put("deepestNestedBaseType", valueType.getDeepestValueType().getBaseName().toLowerCase());

        boolean isList = valueType.getBaseName().equals("LIST");
        nameContext.put("multiValue", isList);
        nameContext.put("list", isList);

        if (match.nameMatch != null) {
            nameContext.put("nameMatch", match.nameMatch);
        }
        if (match.namespaceMatch != null) {
            nameContext.put("namespaceMatch", match.namespaceMatch);
        }
        return dynField.getNameTemplate().format(new DynamicFieldNameTemplateResolver(nameContext));
    }

    private String formatValueTypeName(ValueType valueType) {
        StringBuilder builder = new StringBuilder();

        while (valueType != null) {
            if (builder.length() > 0) {
                builder.append("_");
            }
            builder.append(valueType.getBaseName().toLowerCase());
            valueType = valueType.getNestedValueType();
        }

        return builder.toString();
    }

    /**
     * Deletes all index entries (for all vtags) for the given record.
     *
     * <p>This method requires you obtained the {@link IndexLocker} for the record.
     */
    public void delete(RecordId recordId) throws SolrClientException, ShardSelectorException,
            InterruptedException {
        verifyLock(recordId);
        UpdateResponse response = solrShardMgr.getSolrClient(recordId)
                .deleteByQuery("lily.id:" + ClientUtils.escapeQueryChars(recordId.toString()));
        log.debug(response.toString());
        metrics.deletesByQuery.inc();
    }

    /**
     * <p>This method requires you obtained the {@link IndexLocker} for the record.
     */
    public void delete(String table, RecordId recordId, SchemaId vtag) throws SolrClientException, ShardSelectorException,
            InterruptedException {
        verifyLock(recordId);
        solrShardMgr.getSolrClient(recordId).deleteById(getIndexId(table, recordId, vtag));
        metrics.deletesByQuery.inc();
    }

    private Map<Long, Set<SchemaId>> getVtagsByVersion(Set<SchemaId> vtagsToIndex, Map<SchemaId, Long> vtags) {
        Map<Long, Set<SchemaId>> result = new HashMap<Long, Set<SchemaId>>();

        for (SchemaId vtag : vtagsToIndex) {
            Long version = vtags.get(vtag);
            if (version != null) {
                Set<SchemaId> vtagsOfVersion = result.get(version);
                if (vtagsOfVersion == null) {
                    vtagsOfVersion = new HashSet<SchemaId>();
                    result.put(version, vtagsOfVersion);
                }
                vtagsOfVersion.add(vtag);
            }
        }

        return result;
    }

    /**
     * From the given vtags, retain only those that exist on the record.
     *
     * @param vtags    vtags to start with
     * @param vtRecord record to check vtag existence on
     * @return vtags of the orignal set that actually exist on the record
     */
    protected Set<SchemaId> retainExistingVtagsOnly(Set<SchemaId> vtags, VTaggedRecord vtRecord)
            throws InterruptedException, RepositoryException {
        final Set<SchemaId> result = new HashSet<SchemaId>();
        result.addAll(vtags);
        result.retainAll(vtRecord.getVTags().keySet()); // only keep the vtags which exist in the document
        return result;
    }

    protected String getIndexId(String table, RecordId recordId, SchemaId vtag) {
        return table + "-" + recordId + "-" + vtag.toString();
    }

    /**
     * Lookup name of field type, for use in debug logs. Beware, this might be slow.
     */
    protected String safeLoadTagName(SchemaId fieldTypeId) {
        if (fieldTypeId == null) {
            return "null";
        }

        try {
            return typeManager.getFieldTypeById(fieldTypeId).getName().getName();
        } catch (Throwable t) {
            return "failed to load name";
        }
    }

    protected String vtagSetToNameString(Set<SchemaId> vtags) {
        StringBuilder builder = new StringBuilder();
        for (SchemaId vtag : vtags) {
            if (builder.length() > 0) {
                builder.append(", ");
            }
            builder.append(safeLoadTagName(vtag));
        }
        return builder.toString();
    }

    private void verifyLock(RecordId recordId) {
        try {
            if (!indexLocker.hasLock(recordId)) {
                throw new RuntimeException("Thread does not own index lock for record " + recordId);
            }
        } catch (Throwable t) {
            throw new RuntimeException("Error checking if we own index lock for record " + recordId);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Indexer indexer = (Indexer) o;

        return !(indexName != null ? !indexName.equals(indexer.indexName) : indexer.indexName != null);
    }

    @Override
    public int hashCode() {
        return indexName != null ? indexName.hashCode() : 0;
    }
}
