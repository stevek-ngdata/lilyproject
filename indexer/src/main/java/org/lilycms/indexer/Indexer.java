package org.lilycms.indexer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
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
                    // It's not one of the kinds of events we are interested in
                    return;
                }

                Record record = repository.read(msg.getRecordId());
                Map<String, Long> vtags = VersionTag.getTagsByName(record, typeManager);
                Map<Long, Set<String>> vtagsByVersion = VersionTag.tagsByVersion(vtags);

                IndexCase indexCase = conf.getIndexCase(record.getRecordTypeId(), record.getId().getVariantProperties());

                if (indexCase == null) {
                    // The record should not be indeed
                    // But data from this record might be denormalized into other record entries
                    // TODO therefore go to deref handling
                } else {
                    Set<String> vtagsToIndex = new HashSet<String>();

                    // TODO handle record type change

                    // TODO delete possibly old @@versionless entries

                    RecordEvent event = new RecordEvent(msg.getType(), msg.getData());

                    Map<Scope, Set<FieldType>> updatedFieldsByScope = getFieldTypeAndScope(event.getUpdatedFields());

                    // If any non-versioned fields are changed
                    if (updatedFieldsByScope.get(Scope.NON_VERSIONED).size() > 0) {
                        if (atLeastOneUsedInIndex(updatedFieldsByScope.get(Scope.NON_VERSIONED))) {
                            if (record.getVersion() != null) {
                                vtagsToIndex.addAll(indexCase.getVersionTags());
                            } else {
                                vtagsToIndex.add(VersionTag.VERSIONLESS_TAG);
                            }
                            // TODO go to handle vtags
                        }
                    }

                    if (vtagsToIndex.isEmpty() && (event.getVersionCreated() != -1 || event.getVersionUpdated() != -1)) {
                        if (atLeastOneUsedInIndex(updatedFieldsByScope.get(Scope.VERSIONED))
                                || atLeastOneUsedInIndex(updatedFieldsByScope.get(Scope.VERSIONED_MUTABLE))) {

                            long version = event.getVersionCreated() != -1 ? event.getVersionCreated() : event.getVersionUpdated();
                            Set<String> tmp = new HashSet<String>();
                            tmp.addAll(indexCase.getVersionTags());
                            tmp.retainAll(vtagsByVersion.get(version));
                            vtagsToIndex.addAll(tmp);
                        }
                    }

                    // TODO treat version tag changes

                    // Index
                    if (vtagsToIndex.contains(VersionTag.VERSIONLESS_TAG)) {
                        if (vtagsToIndex.size() > 1) {
                            // TODO this should never occur
                        }
                        // TODO get record + index
                    } else {
                        Map<Long, Set<String>> vtagsToIndexByVersion = getVtagsByVersion(vtagsToIndex, vtags);
                        for (Map.Entry<Long, Set<String>> entry : vtagsToIndexByVersion.entrySet()) {
                            Record version = null;
                            try {
                                version = repository.read(record.getId(), entry.getKey());
                            } catch (RecordNotFoundException e) {
                                // TODO handle this differently from version not found
                            }

                            if (version == null) {
                                // TODO send delete
                            } else {
                                // TODO index
                                index(version, entry.getValue());
                            }
                        }
                    }

                }


            } catch (Exception e) {
                // TODO
                e.printStackTrace();
            }
        }

        /**
         *
         * @param record the correct version of the record, which has the versionTag applied to it
         * @param vtags the version tags under which to index
         */
        private void index(Record record, Set<String> vtags) throws IOException, SolrServerException {
            SolrInputDocument solrDoc = new SolrInputDocument();

            for (IndexField indexField : conf.getIndexFields()) {
                String value = indexField.getValue().eval(record);
                if (value != null) {
                    solrDoc.addField(indexField.getName(), value);
                }
            }

            for (String vtag : vtags) {
                solrDoc.setField("@@key", record.getId().toString() + "-" + vtag);
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
    }
}
