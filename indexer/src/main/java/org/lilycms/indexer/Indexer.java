package org.lilycms.indexer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrInputDocument;
import org.lilycms.indexer.conf.IndexCase;
import org.lilycms.indexer.conf.IndexField;
import org.lilycms.indexer.conf.IndexerConf;
import org.lilycms.queue.api.LilyQueue;
import org.lilycms.queue.api.QueueListener;
import org.lilycms.queue.api.QueueMessage;
import org.lilycms.repository.api.*;
import org.lilycms.repository.api.exception.FieldTypeNotFoundException;
import org.lilycms.repository.api.exception.RecordNotFoundException;
import org.lilycms.repository.api.exception.RepositoryException;
import org.lilycms.repoutil.RecordEvent;
import org.lilycms.repoutil.VersionTag;

import static org.lilycms.repoutil.EventType.*;

import java.io.IOException;
import java.util.*;

/**
 * Updates SOLR index in response to repository events.
 */
public class Indexer {
    private IndexerConf conf;
    private LilyQueue queue;
    private Repository repository;
    private TypeManager typeManager;
    private SolrServer solrServer;
    private IndexerListener indexerListener = new IndexerListener();

    private Log log = LogFactory.getLog(getClass());

    public Indexer(IndexerConf conf, LilyQueue queue, Repository repository, TypeManager typeManager, SolrServer solrServer) {
        this.conf = conf;
        this.queue = queue;
        this.repository = repository;
        this.solrServer = solrServer;
        this.typeManager = typeManager;

        queue.addListener("indexer", indexerListener);
    }

    public void stop() {
        queue.removeListener(indexerListener);
    }

    private class IndexerListener implements QueueListener {
        public void processMessage(String id) {
            try {
                QueueMessage msg = queue.getMessage(id);

                if (!msg.getType().equals(EVENT_RECORD_CREATED) &&
                        !msg.getType().equals(EVENT_RECORD_UPDATED) &&
                        !msg.getType().equals(EVENT_RECORD_DELETED)) {
                    // It is not one of the kinds of events we are interested in
                    return;
                }

                if (msg.getType().equals(EVENT_RECORD_DELETED)) {
                    // For deleted records, we cannot determine the record type, so we do not know if there was
                    // an applicable index case, so we always send a delete to SOLR.
                    solrServer.deleteByQuery("@@id:" + ClientUtils.escapeQueryChars(msg.getRecordId().toString()));
                    // After this we can go to update denormalized data
                } else {
                    handleRecordCreateUpdate(msg);
                }

            } catch (Exception e) {
                // TODO
                e.printStackTrace();
            }
        }
    }

    private void handleRecordCreateUpdate(QueueMessage msg) throws Exception {
        Record record = repository.read(msg.getRecordId());
        Map<String, Long> vtags = VersionTag.getTagsByName(record, typeManager);
        Map<Long, Set<String>> vtagsByVersion = VersionTag.tagsByVersion(vtags);

        // Determine the IndexCase:
        //  The indexing of all versions is determined by the record type of the non-versioned scope.
        //  This makes that the indexing behavior of all versions is equal, and can be changed (since
        //  the record type of the versioned scope is immutable).
        IndexCase indexCase = conf.getIndexCase(record.getRecordTypeId(), record.getId().getVariantProperties());

        if (indexCase == null) {
            // The record should not be indexed
            // But data from this record might be denormalized into other record entries
            // After this we go to update denormalized data
        } else {
            RecordEvent event = new RecordEvent(msg.getType(), msg.getData());

            Set<String> vtagsToIndex = new HashSet<String>();

            if (msg.getType().equals(EVENT_RECORD_CREATED)) {
                // New record: just index everything
                setIndexAllVTags(vtagsToIndex, vtags, indexCase, record);
                // After this we go to the indexing

            } else if (event.getRecordTypeChanged()) {
                // When the record type changes, the rules to index (= the IndexCase) change

                // Delete everything: we do not know the previous record type, so we do not know what
                // version tags were indexed, so we simply delete everything
                solrServer.deleteByQuery("@@id:" + ClientUtils.escapeQueryChars(record.getId().toString()));

                // Reindex all needed vtags
                setIndexAllVTags(vtagsToIndex, vtags, indexCase, record);

                // After this we go to the indexing
            } else { // a normal update
                Map<Scope, Set<FieldType>> updatedFieldsByScope = getFieldTypeAndScope(event.getUpdatedFields());

                if (event.getVersionCreated() == 1
                        && msg.getType().equals(EVENT_RECORD_UPDATED)
                        && indexCase.getIndexVersionless()) {
                    // If the first version was created, but the record was not new, then there
                    // might already be an @@versionless index entry
                    solrServer.deleteById(getIndexId(record.getId(), VersionTag.VERSIONLESS_TAG));
                }

                //
                // Handle changes to non-versioned fields
                //
                if (updatedFieldsByScope.get(Scope.NON_VERSIONED).size() > 0) {
                    if (atLeastOneUsedInIndex(updatedFieldsByScope.get(Scope.NON_VERSIONED))) {
                        setIndexAllVTags(vtagsToIndex, vtags, indexCase, record);
                        // After this we go to the treatment of changed vtag fields
                    }
                }

                //
                // Handle changes to versioned(-mutable) fields
                //
                // If there were non-versioned fields changed, then we all already reindex all versions
                // so this can be skipped.
                //
                if (vtagsToIndex.isEmpty() && (event.getVersionCreated() != -1 || event.getVersionUpdated() != -1)) {
                    if (atLeastOneUsedInIndex(updatedFieldsByScope.get(Scope.VERSIONED))
                            || atLeastOneUsedInIndex(updatedFieldsByScope.get(Scope.VERSIONED_MUTABLE))) {

                        long version = event.getVersionCreated() != -1 ? event.getVersionCreated() : event.getVersionUpdated();
                        if (vtagsByVersion.containsKey(version)) {
                            Set<String> tmp = new HashSet<String>();
                            tmp.addAll(indexCase.getVersionTags());
                            tmp.retainAll(vtagsByVersion.get(version));
                            vtagsToIndex.addAll(tmp);
                        }
                    }
                }

                //
                // Handle changes to version vtag fields
                //
                Set<String> changedVTagFields = VersionTag.filterVTagFields(event.getUpdatedFields(), typeManager);
                // Remove the vtags which are going to be reindexed anyway
                changedVTagFields.removeAll(vtagsToIndex);
                for (String vtag : changedVTagFields) {
                    if (vtags.containsKey(vtag) && indexCase.getVersionTags().contains(vtag)) {
                        vtagsToIndex.add(vtag);
                    } else {
                        // The vtag does not exist anymore on the document: delete from index
                        solrServer.deleteById(getIndexId(record.getId(), vtag));
                    }
                }
            }


            //
            // Index
            //
            if (vtagsToIndex.contains(VersionTag.VERSIONLESS_TAG)) {
                // Usually when the @@versionless vtag should be indexed, the vtagsToIndex set will
                // not contain any other tags.
                // It could be that there are other tags however: for example if someone added and removed
                // vtag fields to the (versionless) document.
                // If we would ever support deleting of versions, then it could also be the case,
                // but then we'll have to extend this to delete these old versions from the index.
                index(record, Collections.singleton(VersionTag.VERSIONLESS_TAG));
            } else {
                // One version might have multiple vtags, so to index we iterate the version numbers
                // rather than the vtags
                Map<Long, Set<String>> vtagsToIndexByVersion = getVtagsByVersion(vtagsToIndex, vtags);
                for (Map.Entry<Long, Set<String>> entry : vtagsToIndexByVersion.entrySet()) {
                    Record version = null;
                    try {
                        version = repository.read(record.getId(), entry.getKey());
                    } catch (RecordNotFoundException e) {
                        // TODO handle this differently from version not found
                    }

                    if (version == null) {
                        for (String vtag : entry.getValue()) {
                            solrServer.deleteById(getIndexId(record.getId(), vtag));
                        }
                    } else {
                        index(version, entry.getValue());
                    }
                }
            }
        }
    }

    /**
     *
     * @param record the correct version of the record, which has the versionTag applied to it
     * @param vtags the version tags under which to index
     */
    private void index(Record record, Set<String> vtags) throws IOException, SolrServerException {

        // Note that it is important the the indexFields are evaluated in order, since multiple
        // indexFields can have the same name and the order of values for multivalue fields can be important.
        //
        // The value of the indexFields is re-evaluated for each vtag. It is only the value of
        // deref-values which can change from vtag to vtag, so we could optimize this by only
        // evaluating those after the first run, but again because we want to maintain order and
        // because a deref-field could share the same name with a non-deref field, we simply
        // re-evaluate all fields for each vtag.
        for (String vtag : vtags) {
            SolrInputDocument solrDoc = new SolrInputDocument();

            boolean valueAdded = false;
            for (IndexField indexField : conf.getIndexFields()) {
                List<String> values = indexField.getValue().eval(record, repository, vtag);
                if (values != null) {
                    for (String value : values) {
                        solrDoc.addField(indexField.getName(), value);
                        valueAdded = true;
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
                continue;
            }


            solrDoc.setField("@@id", record.getId().toString());
            solrDoc.setField("@@key", getIndexId(record.getId(), vtag));
            solrDoc.setField("@@vtag", vtag);

            if (vtag.equals(VersionTag.VERSIONLESS_TAG)) {
                solrDoc.setField("@@versionless", "true");
            }

            solrServer.add(solrDoc);
        }
    }

    private boolean atLeastOneUsedInIndex(Set<FieldType> fieldTypes) {
        for (FieldType type : fieldTypes) {
            if (conf.isIndexFieldDependency(type.getName())) {
                return true;
            }
        }
        return false;
    }

    private Map<Long, Set<String>> getVtagsByVersion(Set<String> vtagsToIndex, Map<String, Long> vtags) {
        Map<Long, Set<String>> result = new HashMap<Long, Set<String>>();

        for (String vtag : vtagsToIndex) {
            long version = vtags.get(vtag);
            Set<String> vtagsOfVersion = result.get(version);
            if (vtagsOfVersion == null) {
                vtagsOfVersion = new HashSet<String>();
                result.put(version, vtagsOfVersion);
            }
            vtagsOfVersion.add(vtag);
        }

        return result;
    }

    private Map<Scope, Set<FieldType>> getFieldTypeAndScope(Set<String> fieldIds) {
        Map<Scope, Set<FieldType>> result = new HashMap<Scope, Set<FieldType>>();
        for (Scope scope : Scope.values()) {
            result.put(scope, new HashSet<FieldType>());
        }

        for (String fieldId : fieldIds) {
            FieldType fieldType;
            try {
                fieldType = typeManager.getFieldTypeById(fieldId);
            } catch (FieldTypeNotFoundException e) {
                continue;
            } catch (RepositoryException e) {
                // TODO not sure what to do in these kinds of situations
                throw new RuntimeException(e);
            }
            result.get(fieldType.getScope()).add(fieldType);
        }

        return result;
    }

    /**
     * TODO this method is a temporary solution to detect that a record has versions,
     *      should be removed once issue #1 is solved.
     */
    private boolean recordHasVersions(Record record) {
        for (QName fieldName : record.getFields().keySet()) {
            Scope scope = typeManager.getFieldTypeByName(fieldName).getScope();
            if (scope == Scope.VERSIONED || scope == Scope.VERSIONED_MUTABLE) {
                return true;
            }
        }
        return false;
    }

    private void setIndexAllVTags(Set<String> vtagsToIndex, Map<String, Long> vtags, IndexCase indexCase, Record record) {
        if (recordHasVersions(record)) {
            Set<String> tmp = new HashSet<String>();
            tmp.addAll(indexCase.getVersionTags());
            tmp.retainAll(vtags.keySet()); // only keep the vtags which exist in the document
            vtagsToIndex.addAll(tmp);
        } else if (indexCase.getIndexVersionless()) {
            vtagsToIndex.add(VersionTag.VERSIONLESS_TAG);
        }
    }

    private String getIndexId(RecordId recordId, String vtag) {
        return recordId + "-" + vtag;
    }

}
