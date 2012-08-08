package org.lilyproject.indexer.engine;

import java.net.MalformedURLException;

import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.lilyproject.indexer.model.sharding.ShardSelectorException;
import org.lilyproject.repository.api.RecordId;

public class CloudSolrShardManager implements SolrShardManager {
    private final SolrClient solrClient;
    public CloudSolrShardManager (String zkHost, String collection) throws MalformedURLException{
        CloudSolrServer solr = new CloudSolrServer(zkHost);
        solrClient = new SolrClientImpl(solr, collection, "Solr Cloud Client");
    }

    @Override
    public SolrClient getSolrClient(RecordId recordId) throws ShardSelectorException {
        return solrClient;
    }

}
