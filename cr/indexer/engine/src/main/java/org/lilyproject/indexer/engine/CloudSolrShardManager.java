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

    /**
     * @param swallowUnrecoverableExceptions If true, SolrClients will swallow and report all exceptions that cannot be corrected by a change in configuration
     */
    public CloudSolrShardManager(String indexName, String zkHost, String collection, boolean swallowUnrecoverableExceptions)
            throws MalformedURLException {
        solrServer = new CloudSolrServer(zkHost);

        solrClient = createSolrClient(solrServer, collection, swallowUnrecoverableExceptions, indexName);
    }

    private SolrClient createSolrClient(CloudSolrServer solrServer, String collection, boolean swallowUnrecoverableExceptions,
                                        String indexName) {
        final SolrClientImpl solrClient = new SolrClientImpl(solrServer, collection, "Solr Cloud Client");

        if (swallowUnrecoverableExceptions) {
            solrClientMetrics = new SolrClientMetrics(indexName, "cloud");
            return ErrorSwallowingSolrClient.wrap(solrClient, solrClientMetrics);
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
