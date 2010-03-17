package org.lilycms.indexer;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.lilycms.queue.api.LilyQueue;
import org.lilycms.queue.api.QueueListener;
import org.lilycms.queue.api.QueueMessage;
import org.lilycms.repository.api.Record;
import org.lilycms.repository.api.Repository;

/**
 * Updates SOLR index in response to repository events.
 */
public class Indexer {
    private LilyQueue queue;
    private Repository repository;
    private SolrServer solrServer;
    private IndexerListener indexerListener = new IndexerListener();

    public Indexer(LilyQueue queue, Repository repository, SolrServer solrServer) {
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
                    Record record = repository.read(repository.getIdGenerator().newRecordId(msg.getRecordId(), msg.getVariantProperties()));

                    // TODO find out mapping

                    // TODO map to SOLR document

                    // TODO store in SOLR
                    SolrInputDocument solrDoc = new SolrInputDocument();
                    solrDoc.addField("id", record.getId().toString());
                    solrDoc.addField("title", Bytes.toString(record.getField("title").getValue()));
                    solrServer.add(solrDoc);                    
                }
            } catch (Exception e) {
                // TODO
                e.printStackTrace();
            }
        }
    }
}
