/*
 * Copyright 2012 NGDATA nv
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
