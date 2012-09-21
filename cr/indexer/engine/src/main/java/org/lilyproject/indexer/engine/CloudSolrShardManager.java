package org.lilyproject.indexer.engine;

import java.io.IOException;
import java.net.MalformedURLException;

import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.lilyproject.indexer.model.sharding.ShardSelectorException;
import org.lilyproject.repository.api.RecordId;

/**
 * Solr shard manager for Solr "cloud".
 */
public class CloudSolrShardManager implements SolrShardManager {
    private final CloudSolrServer solrServer;

    private final SolrClient solrClient;

    public CloudSolrShardManager(String zkHost, String collection) throws MalformedURLException {
        solrServer = new CloudSolrServer(zkHost);
        solrClient = new SolrClientImpl(solrServer, collection, "Solr Cloud Client");
    }

    @Override
    public SolrClient getSolrClient(RecordId recordId) throws ShardSelectorException {
        return solrClient;
    }

    @Override
    public void close() throws IOException {
        solrServer.shutdown();
    }
}
