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
package org.lilyproject.lilyservertestfw;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.zookeeper.KeeperException;
import org.lilyproject.client.LilyClient;
import org.lilyproject.client.NoServersException;
import org.lilyproject.indexer.model.api.*;
import org.lilyproject.indexer.model.impl.IndexerModelImpl;
import org.lilyproject.indexer.model.indexerconf.IndexerConfBuilder;
import org.lilyproject.indexer.model.indexerconf.IndexerConfException;
import org.lilyproject.solrtestfw.SolrTestingUtility;
import org.lilyproject.testfw.HBaseProxy;
import org.lilyproject.util.zookeeper.StateWatchingZooKeeper;
import org.lilyproject.util.zookeeper.ZkConnectException;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

public class LilyTestUtility {
    private static HBaseProxy HBASE_PROXY;
    private KauriTestUtility kauriTestUtility;
    private SolrTestingUtility solrTestUtility;
    private ZooKeeperItf zooKeeper;
    private final String solrSchema;
    private CommonsHttpSolrServer solrServer;
    private final String kauriHome;

    public LilyTestUtility(String kauriHome, String solrSchema) {
        this.kauriHome = kauriHome;
        this.solrSchema = solrSchema;
    }

    public void start() throws Exception {
        kauriTestUtility = new KauriTestUtility(kauriHome);
        HBASE_PROXY = new HBaseProxy();
        HBASE_PROXY.start();
        zooKeeper = new StateWatchingZooKeeper(HBASE_PROXY.getZkConnectString(), 10000);

        kauriTestUtility.createDefaultConf(HBASE_PROXY);
        kauriTestUtility.start();

        solrTestUtility = new SolrTestingUtility(null);
        solrTestUtility.setSchemaLocation("classpath:" + solrSchema);
        solrTestUtility.start();
        MultiThreadedHttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();
        connectionManager.getParams().setDefaultMaxConnectionsPerHost(5);
        connectionManager.getParams().setMaxTotalConnections(50);
        HttpClient httpClient = new HttpClient(connectionManager);
        solrServer = new CommonsHttpSolrServer(solrTestUtility.getUri(), httpClient);
    }

    public void stop() throws Exception {
        try {
            if (solrTestUtility != null)
                solrTestUtility.stop();
        } catch (Throwable t) {
            t.printStackTrace();
        }
        try {
            if (kauriTestUtility != null)
            kauriTestUtility.stop();
        } catch (Throwable t) {
            t.printStackTrace();
        }

        try {
            if (HBASE_PROXY != null)
                HBASE_PROXY.stop();
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    public LilyClient getClient() throws IOException, InterruptedException, KeeperException, ZkConnectException,
            NoServersException {
        return new LilyClient(HBASE_PROXY.getZkConnectString(), 10000);
    }
    
    public SolrServer getSolrServer() {
        return solrServer;
    }

    public void addIndexFromResource(String indexName, String indexerConf) throws IOException, IndexerConfException, InterruptedException, KeeperException, ZkConnectException, NoServersException, IndexExistsException, IndexModelException, IndexValidityException {
        InputStream is = getClass().getClassLoader().getResourceAsStream(indexerConf);
        byte[] indexerConfiguration = IOUtils.toByteArray(is);
        is.close();
        addIndex(indexName, indexerConfiguration);
    }
    
    public void addIndexFromFile(String indexName, String indexerConf) throws IOException, IndexerConfException, InterruptedException, KeeperException, ZkConnectException, NoServersException, IndexExistsException, IndexModelException, IndexValidityException {
        byte[] indexerConfiguration = FileUtils.readFileToByteArray(new File(indexerConf));
        addIndex(indexName, indexerConfiguration);
    }
    
    public void addIndex(String indexName, byte[] indexerConfiguration) throws IndexerConfException, IOException, InterruptedException, KeeperException, ZkConnectException, NoServersException, IndexExistsException, IndexModelException, IndexValidityException {
        IndexerConfBuilder.build(new ByteArrayInputStream(indexerConfiguration), getClient().getRepository());

        IndexerModelImpl model = new IndexerModelImpl(zooKeeper);
        IndexDefinition index = model.newIndex(indexName);
        Map<String, String> solrShards = new HashMap<String, String>();
        solrShards.put("testshard", solrTestUtility.getUri());
        index.setSolrShards(solrShards);
        index.setConfiguration(indexerConfiguration);
        model.addIndex(index);
    }

}
