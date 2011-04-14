/*
 * Copyright 2010 Outerthought bvba
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

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BinaryRequestWriter;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.lilyproject.indexer.model.sharding.DefaultShardSelectorBuilder;
import org.lilyproject.indexer.model.sharding.ShardSelector;
import org.lilyproject.indexer.model.sharding.ShardSelectorException;
import org.lilyproject.indexer.model.sharding.ShardingConfigException;
import org.lilyproject.repository.api.RecordId;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.*;

public class SolrShardManager {
    /** Key = shard name, Value = SOLR URL */
    private Map<String, String> shards;
    private Map<String, SolrClient> shardConnections;
    private ShardSelector selector;
    private HttpClient httpClient;
    private RequestWriter requestWriter;
    private ResponseParser responseParser;

    public SolrShardManager(Map<String, String> shards, ShardSelector selector, HttpClient httpClient,
            SolrClientConfig solrClientConfig) throws MalformedURLException {
        this(shards, selector, httpClient, solrClientConfig, false);
    }

    public SolrShardManager(Map<String, String> shards, ShardSelector selector, HttpClient httpClient,
            SolrClientConfig solrClientConfig, boolean blockOnIOProblem) throws MalformedURLException {
        this.shards = shards;
        this.selector = selector;
        this.httpClient = httpClient;

        if (solrClientConfig.getRequestWriter() != null) {
            try {
                this.requestWriter = (RequestWriter)Class.forName(solrClientConfig.getRequestWriter()).newInstance();
            } catch (Exception e) {
                throw new RuntimeException("Problem instantiating SOLR request writer", e);
            }
        } else {
            this.requestWriter = new BinaryRequestWriter();
        }

        if (solrClientConfig.getResponseParser() != null) {
            try {
                this.responseParser = (ResponseParser)Class.forName(solrClientConfig.getResponseParser()).newInstance();
            } catch (Exception e) {
                throw new RuntimeException("Problem instantiating SOLR response parser", e);
            }
        } else {
            this.responseParser = new BinaryResponseParser();
        }

        init(blockOnIOProblem);
    }

    /**
     * This method is only meant for use by test cases.
     */
    public static SolrShardManager createForOneShard(String uri) throws URISyntaxException, ShardingConfigException,
            MalformedURLException {
        SortedMap<String, String> shards = new TreeMap<String, String>();
        shards.put("shard1", uri);
        ShardSelector selector = DefaultShardSelectorBuilder.createDefaultSelector(shards);
        return new SolrShardManager(shards, selector, new HttpClient(new MultiThreadedHttpConnectionManager()),
                new SolrClientConfig());
    }

    /**
     * This method is only meant for use by test cases.
     */
    public void commit(boolean waitFlush, boolean waitSearcher) throws IOException, SolrServerException,
            InterruptedException {
        for (SolrClient server : shardConnections.values()) {
            server.commit(waitFlush, waitSearcher);
        }
    }

    /**
     * This method is only meant for use by test cases. Currently queries the first shard only.
     */
    public QueryResponse query(SolrQuery query) throws SolrServerException, InterruptedException {
        return shardConnections.values().iterator().next().query(query);
    }

    private void init(boolean blockOnIOProblem) throws MalformedURLException {
        shardConnections = new HashMap<String, SolrClient>();
        for (Map.Entry<String, String> shard : shards.entrySet()) {
            CommonsHttpSolrServer solr = new CommonsHttpSolrServer(shard.getValue(), httpClient);
            solr.setRequestWriter(requestWriter);
            solr.setParser(responseParser);
            SolrClient solrClient = new SolrClientImpl(solr, shard.getValue());
            if (blockOnIOProblem) {
                solrClient = RetryingSolrClient.create(solrClient);
            }
            shardConnections.put(shard.getKey(), solrClient);
        }
    }

    public SolrClient getSolrClient(RecordId recordId) throws ShardSelectorException {
        String shardName = selector.getShard(recordId);
        return shardConnections.get(shardName);
    }
}
