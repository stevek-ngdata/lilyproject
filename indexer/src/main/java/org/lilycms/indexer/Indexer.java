package org.lilycms.indexer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.lilycms.indexer.conf.IndexFieldBinding;
import org.lilycms.indexer.conf.IndexerConf;
import org.lilycms.indexer.conf.RecordTypeMapping;
import org.lilycms.queue.api.LilyQueue;
import org.lilycms.queue.api.QueueListener;
import org.lilycms.queue.api.QueueMessage;
import org.lilycms.repository.api.Record;
import org.lilycms.repository.api.Repository;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Updates SOLR index in response to repository events.
 */
public class Indexer {
    private IndexerConf conf;
    private LilyQueue queue;
    private Repository repository;
    private SolrServer solrServer;
    private IndexerListener indexerListener = new IndexerListener();

    private Log log = LogFactory.getLog(getClass());

    public Indexer(IndexerConf conf, LilyQueue queue, Repository repository, SolrServer solrServer) {
        this.conf = conf;
        this.queue = queue;
        this.repository = repository;
        this.solrServer = solrServer;

        queue.addListener("indexer", indexerListener);
    }

    public void stop() {
        queue.removeListener(indexerListener);
    }

    private class IndexerListener implements QueueListener {
        public void processMessage(String id) {
            try {
                QueueMessage msg = queue.getMessage(id);

                if (msg.getType().equals("document-created") || msg.getType().equals("variant-created")) {
                    Record record = repository.read(msg.getRecordId());

                    // TODO: do a for-each over the actual version tags of this record
                    // TODO: what if the record has only non-versioned data
                    //         (imply 'last' tag?, cfr. also version/nonversioned record type discussion: maybe such
                    //          documents will always have at least 1 version, which could then also be used for tagging)
                    index(record, "live");

                } else if (msg.getType().equals("document-updated") || msg.getType().equals("variant-updated")) {
                    // The cases we need to handle are:
                    //  - a new version is created, and the version has a version tag which is indexed according to the conf
                    //  - the above + non-versioned data is changed
                    //  - no version created + non-versioned data is changed
                    //  - no version created + no non-versioned data changed: bogus event: there will be no changed fields
                    //  - the created version has some tags which were previously owned by another version:
                    //      no problem, these index entries will be overwritten

                    byte[] rawMsgData = msg.getData();
                    JsonNode msgData = new ObjectMapper().readValue(rawMsgData, 0, rawMsgData.length, JsonNode.class);

                    Set<String> changedFields = getChangedFields(msgData);
                    if (changedFields.size() == 0) {
                        // TODO: maybe this can be the case when just the document type changed?
                        log.debug("strange: document-update event without changed fields.");
                        return;
                    }

                    int versionCreated = -1;
                    if (msgData.get("versionCreated") != null) {
                        versionCreated = msgData.get("versionCreated").getIntValue();
                    }

                    // Read the record at this point because we need the record type id
                    Record record;
                    if (versionCreated != -1) {
                        record = repository.read(msg.getRecordId(), new Long(versionCreated));
                    } else {
                        record = repository.read(msg.getRecordId());
                    }

                    Set<String> versionTagsToIndex = new HashSet<String>();

                    if (getIndexedNonVersionedFieldsChanged(record.getRecordTypeId(), changedFields)) {
                        // indexed non-versioned fields changed: re-index all indexed versions
                        versionTagsToIndex.addAll(conf.getIndexedVersionTags(record.getRecordTypeId()));
                    } else if (versionCreated != -1) {
                        // index only the changed/created version
                        // TODO change once version tags are available
                        // Set<String> tags = record.getVersionTags();
                        versionTagsToIndex.add("live");
                    }

                    for (String versionTag : versionTagsToIndex) {
                        // TODO activate this once version tags exist
                        // TODO could be optimized for the case the same version implements multiple tags
                        //record = repository.read(msg.getRecordId(), versionTag);
                        index(record, versionTag);
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
         * @param versionTag the version tag under which to index
         */
        private void index(Record record, String versionTag) throws IOException, SolrServerException {
            SolrInputDocument solrDoc = new SolrInputDocument();
            solrDoc.addField("@@id", record.getId().toString() + "-" + versionTag);

            // TODO: think about non-versioned-content-only situations
            RecordTypeMapping mapping = conf.getVersionedContentMapping(record.getRecordTypeId(), versionTag);
            if (mapping != null) {
                for (IndexFieldBinding indexFieldBinding : mapping.getBindings()) {
                    String name = indexFieldBinding.getField().getName();
                    String value = indexFieldBinding.getValue().eval(record);
                    solrDoc.addField(name, value);
                }

                RecordTypeMapping nvMapping = conf.getNonVersionedContentMapping(record.getRecordTypeId());
                for (IndexFieldBinding indexFieldBinding : nvMapping.getBindings()) {
                    String name = indexFieldBinding.getField().getName();
                    String value = indexFieldBinding.getValue().eval(record);
                    solrDoc.addField(name, value);
                }

                solrServer.add(solrDoc);
            }
        }

        private Set<String> getChangedFields(JsonNode msgData) {
            JsonNode changedFields = msgData.get("changedFields");
            if (changedFields == null || changedFields.size() == 0) {
                return Collections.emptySet();
            }

            Set<String> result = new HashSet<String>();
            for (int i = 0; i < msgData.size(); i++) {
                result.add(changedFields.get(i).getTextValue());
            }

            return result;
        }

        /**
         * Checks if any of the changed fields are non-versioned fields, and if so if any
         * of those are used in the indexer configuration.
         */
        private boolean getIndexedNonVersionedFieldsChanged(String recordTypeId, Set<String> changedFields) {
            // TODO: should use the non-versioned record type
            RecordTypeMapping nvMapping = conf.getNonVersionedContentMapping(recordTypeId);
            Set<String> changedNvFields = new HashSet<String>();
            changedNvFields.addAll(nvMapping.getReferencedFields());
            changedNvFields.retainAll(changedFields);
            return changedNvFields.size() > 0;
        }
    }
}
