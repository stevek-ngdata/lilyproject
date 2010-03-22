package org.lilycms.indexer;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.lilycms.indexer.conf.IndexFieldBinding;
import org.lilycms.indexer.conf.IndexerConf;
import org.lilycms.indexer.conf.RecordTypeMapping;
import org.lilycms.queue.api.LilyQueue;
import org.lilycms.queue.api.QueueListener;
import org.lilycms.queue.api.QueueMessage;
import org.lilycms.repository.api.Record;
import org.lilycms.repository.api.Repository;

/**
 * Updates SOLR index in response to repository events.
 */
public class Indexer {
    private IndexerConf conf;
    private LilyQueue queue;
    private Repository repository;
    private SolrServer solrServer;
    private IndexerListener indexerListener = new IndexerListener();

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

                if (msg.getType().equals("document-created")) {
                    Record record = repository.read(msg.getRecordId());

                    SolrInputDocument solrDoc = new SolrInputDocument();
                    solrDoc.addField("@@id", record.getId().toString());

                    RecordTypeMapping mapping = conf.getVersionedContentMapping(record.getRecordTypeId(), /* TODO */ "live");
                    for (IndexFieldBinding indexFieldBinding : mapping.getBindings()) {
                        String name = indexFieldBinding.getField().getName();
                        String value = indexFieldBinding.getValue().eval(record);
                        solrDoc.addField(name, value);
                    }

                    solrServer.add(solrDoc);
                }
            } catch (Exception e) {
                // TODO
                e.printStackTrace();
            }
        }
    }
}
