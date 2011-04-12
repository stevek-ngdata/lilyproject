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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrInputDocument;
import org.lilyproject.indexer.model.indexerconf.*;
import org.lilyproject.indexer.model.indexerconf.DynamicIndexField.DynamicIndexFieldMatch;
import org.lilyproject.indexer.model.sharding.ShardSelectorException;
import org.lilyproject.repository.api.*;
import org.lilyproject.util.repo.VTaggedRecord;

import java.io.IOException;
import java.util.*;


// IMPORTANT: each call to solrServer should be followed by a corresponding metrics update.

/**
 * The Indexer adds records to, or removes records from, the index.
 */
public class Indexer {
    private IndexerConf conf;
    private Repository repository;
    private TypeManager typeManager;
    private SolrServers solrServers;
    private IndexLocker indexLocker;
    private ValueEvaluator valueEvaluator;
    private IndexerMetrics metrics;

    private Log log = LogFactory.getLog(getClass());

    public Indexer(IndexerConf conf, Repository repository, SolrServers solrServers, IndexLocker indexLocker,
            IndexerMetrics metrics) {
        this.conf = conf;
        this.repository = repository;
        this.solrServers = solrServers;
        this.indexLocker = indexLocker;
        this.typeManager = repository.getTypeManager();
        this.valueEvaluator = new ValueEvaluator(conf);
        this.metrics = metrics;
    }

    public IndexerConf getConf() {
        return conf;
    }

    /**
     * Performs a complete indexing of the given record, supposing the record is not yet indexed
     * (existing entries are not explicitly removed).
     *
     * <p>This method requires you obtained the {@link IndexLocker} for the record.
     *
     * @param recordId
     */
    public void index(RecordId recordId) throws RepositoryException, IOException,
            SolrServerException, ShardSelectorException, InterruptedException {

        VTaggedRecord vtRecord = new VTaggedRecord(recordId, repository);
        IdRecord record = vtRecord.getNonVersionedRecord();

        IndexCase indexCase = conf.getIndexCase(record.getRecordTypeName(), record.getId().getVariantProperties());
        if (indexCase == null) {
            return;
        }

        Set<SchemaId> vtagsToIndex = new HashSet<SchemaId>();
        setIndexAllVTags(vtagsToIndex, indexCase, vtRecord);

        index(vtRecord, vtagsToIndex);
    }

    /**
     * Indexes a record for a set of vtags.
     *
     * <p>This method requires you obtained the {@link IndexLocker} for the record.
     *
     * @param vtagsToIndex all vtags for which to index the record, not all vtags need to exist on the record,
     *                     but this should only contain appropriate vtags as defined by the IndexCase for this record.
     */
    protected void index(VTaggedRecord vtRecord, Set<SchemaId> vtagsToIndex)
            throws IOException, SolrServerException, RepositoryException, ShardSelectorException, InterruptedException {

        RecordId recordId = vtRecord.getNonVersionedRecord().getId();

        // One version might have multiple vtags, so to index we iterate the version numbers
        // rather than the vtags
        Map<Long, Set<SchemaId>> vtagsToIndexByVersion = getVtagsByVersion(vtagsToIndex, vtRecord.getVTags());
        for (Map.Entry<Long, Set<SchemaId>> entry : vtagsToIndexByVersion.entrySet()) {
            IdRecord version = null;
            try {
                version = vtRecord.getIdRecord(entry.getKey());
            } catch (VersionNotFoundException e) {
                // TODO
            } catch (RecordNotFoundException e) {
                // TODO handle this differently from version not found
            }

            if (version == null) {
                for (SchemaId vtag : entry.getValue()) {
                    verifyLock(recordId);
                    solrServers.getSolrServer(recordId).deleteById(getIndexId(recordId, vtag));
                    metrics.deletesById.inc();
                }

                if (log.isDebugEnabled()) {
                    log.debug(String.format("Record %1$s, version %2$s: does not exist, deleted index" +
                            " entries for vtags %3$s", recordId, entry.getKey(), vtagSetToNameString(entry.getValue())));
                }
            } else {
                index(version, entry.getValue());
            }
        }
    }

    /**
     * The actual indexing: maps record fields to index fields, and send to SOLR.
     *
     * @param record the correct version of the record, which has the versionTag applied to it
     * @param vtags the version tags under which to index
     */
    protected void index(IdRecord record, Set<SchemaId> vtags) throws IOException, SolrServerException,
            ShardSelectorException, RepositoryException, InterruptedException {

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
            SolrInputDocument solrDoc = new SolrInputDocument();

            boolean valueAdded = false;

            // By convention/definition, we first evaluate the static index fields and then the dynamic ones

            //
            // 1: evaluate the static index fields
            //
            for (IndexField indexField : conf.getIndexFields()) {
                List<String> values = valueEvaluator.eval(indexField.getValue(), record, repository, vtag);
                if (values != null) {
                    for (String value : values) {
                        solrDoc.addField(indexField.getName(), value);
                        valueAdded = true;
                    }
                }
            }

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

                            List<String> values = valueEvaluator.format(record, fieldType, dynField.extractContext(),
                                    dynField.getFormatter(), repository);

                            if (values != null) { // while field will always be there, formatter or content extraction
                                                  // can make it empty
                                for (String value : values) {
                                    solrDoc.addField(fieldName, value);
                                    valueAdded = true;
                                }
                            }

                            // Dynamic fields: stop on the first match
                            break;
                        }
                    }
                }
            }

            if (!valueAdded) {
                // No single field was added to the SOLR document.
                // In this case we do not add it to the index.
                // Besides being somewhat logical, it should also be noted that if a record would not contain
                // any (modified) fields that serve as input to indexFields, we would never have arrived here
                // anyway. It is only because some fields potentially would resolve to a value (potentially:
                // because with deref-expressions we are never sure) that we did.

                // There can be a previous entry in the index which we should try to delete
                solrServers.getSolrServer(record.getId()).deleteById(getIndexId(record.getId(), vtag));
                metrics.deletesById.inc();
                
                if (log.isDebugEnabled()) {
                    log.debug(String.format("Record %1$s, vtag %2$s: no index fields produced output, " +
                            "removed from index if present", record.getId(), safeLoadTagName(vtag)));
                }

                continue;
            }


            solrDoc.setField("@@id", record.getId().toString());
            solrDoc.setField("@@key", getIndexId(record.getId(), vtag));
            solrDoc.setField("@@vtag", vtag.toString());

            // Can be useful during development
            // if (log.isDebugEnabled()) {
            //    log.debug("Constructed SOLR doc: " + solrDoc);
            //}

            solrServers.getSolrServer(record.getId()).add(solrDoc);
            metrics.adds.inc();

            if (log.isDebugEnabled()) {
                log.debug(String.format("Record %1$s, vtag %2$s: indexed", record.getId(), safeLoadTagName(vtag)));
            }
        }
    }

    private String evalName(DynamicIndexField dynField, DynamicIndexFieldMatch match, FieldType fieldType) {
        // Calculate the name, then add the value
        Map<String, Object> nameContext = new HashMap<String, Object>();
        nameContext.put("namespace", fieldType.getName().getName());
        nameContext.put("name", fieldType.getName().getName());
        ValueType valueType = fieldType.getValueType();
        nameContext.put("primitiveType", valueType.getPrimitive().getName());
        nameContext.put("primitiveTypeLC", valueType.getPrimitive().getName().toLowerCase());
        nameContext.put("multiValue", valueType.isMultiValue());
        nameContext.put("hierarchical", valueType.isHierarchical());
        if (match.nameMatch != null) {
            nameContext.put("nameMatch", match.nameMatch);
        }
        if (match.namespaceMatch != null) {
            nameContext.put("namespaceMatch", match.namespaceMatch);
        }
        String fieldName = dynField.getNameTemplate().format(nameContext);
        return fieldName;
    }

    /**
     * Deletes all index entries (for all vtags) for the given record.
     *
     * <p>This method requires you obtained the {@link IndexLocker} for the record.
     */
    public void delete(RecordId recordId) throws IOException, SolrServerException, ShardSelectorException {
        verifyLock(recordId);
        solrServers.getSolrServer(recordId).deleteByQuery("@@id:" + ClientUtils.escapeQueryChars(recordId.toString()));
        metrics.deletesByQuery.inc();
    }

    /**
     *
     * <p>This method requires you obtained the {@link IndexLocker} for the record.
     */
    public void delete(RecordId recordId, SchemaId vtag) throws IOException, SolrServerException, ShardSelectorException {
        verifyLock(recordId);
        solrServers.getSolrServer(recordId).deleteById(getIndexId(recordId, vtag));
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

    protected void setIndexAllVTags(Set<SchemaId> vtagsToIndex, IndexCase indexCase, VTaggedRecord vtRecord)
            throws InterruptedException, RepositoryException {
        Set<SchemaId> tmp = new HashSet<SchemaId>();
        tmp.addAll(indexCase.getVersionTags());
        tmp.retainAll(vtRecord.getVTags().keySet()); // only keep the vtags which exist in the document
        vtagsToIndex.addAll(tmp);
    }

    protected String getIndexId(RecordId recordId, SchemaId vtag) {
        return recordId + "-" + vtag.toString();
    }

    /**
     * Lookup name of field type, for use in debug logs. Beware, this might be slow.
     */
    protected String safeLoadTagName(SchemaId fieldTypeId) {
        if (fieldTypeId == null)
            return "null";

        try {
            return typeManager.getFieldTypeById(fieldTypeId).getName().getName();
        } catch (Throwable t) {
            return "failed to load name";
        }
    }

    protected String vtagSetToNameString(Set<SchemaId> vtags) {
        StringBuilder builder = new StringBuilder();
        for (SchemaId vtag : vtags) {
            if (builder.length() > 0)
                builder.append(", ");
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
}
