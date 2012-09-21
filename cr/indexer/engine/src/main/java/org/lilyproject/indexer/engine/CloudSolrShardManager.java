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

    private SolrClientMetrics solrClientMetrics;

    public CloudSolrShardManager(String indexName, String zkHost, String collection, boolean blockOnIOProblem)
            throws MalformedURLException {
        solrServer = new CloudSolrServer(zkHost);

        solrClient = createSolrClient(solrServer, collection, blockOnIOProblem, indexName);
    }

    private SolrClient createSolrClient(CloudSolrServer solrServer, String collection, boolean blockOnIOProblem,
                                        String indexName) {
        final SolrClientImpl solrClient = new SolrClientImpl(solrServer, collection, "Solr Cloud Client");

        if (blockOnIOProblem) {
            solrClientMetrics = new SolrClientMetrics(indexName, "cloud");
            return RetryingSolrClient.wrap(solrClient, solrClientMetrics);
        } else {
            return solrClient;
        }
    }

    @Override
    public SolrClient getSolrClient(RecordId recordId) throws ShardSelectorException {
        return solrClient;
    }

    @Override
    public void close() throws IOException {
        solrServer.shutdown();
        if (solrClientMetrics != null) {
            solrClientMetrics.shutdown();
        }
    }
}
