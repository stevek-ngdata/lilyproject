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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.lilyproject.util.zookeeper.ZkConnectException;
import org.lilyproject.util.zookeeper.ZkUtil;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

public class LilyProxy {
    private HBaseProxy hbaseProxy;
    private LilyServerProxy lilyServerProxy;
    private SolrTestingUtility solrTestUtility;
    private CommonsHttpSolrServer solrServer;

    public LilyProxy(String solrSchema) {
        // TODO Auto-generated constructor stub
        hbaseProxy = new HBaseProxy();

        // TODO : move to solr proxy
        solrTestUtility = new SolrTestingUtility(solrSchema);
        lilyServerProxy = new LilyServerProxy();
    }

    public void start() throws Exception {
        hbaseProxy.start();
        startSolr();
        lilyServerProxy.start(hbaseProxy.getZkConnectString());
    }
    
    public void stop() throws Exception {
        if (lilyServerProxy != null)
            lilyServerProxy.stop();
        solrServer = null;
        if (solrTestUtility != null)
            solrTestUtility.stop();
        if (hbaseProxy != null)
            hbaseProxy.stop();
    }
    
    private void startSolr() throws Exception {
        solrTestUtility.start();
        MultiThreadedHttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();
        connectionManager.getParams().setDefaultMaxConnectionsPerHost(5);
        connectionManager.getParams().setMaxTotalConnections(50);
        HttpClient httpClient = new HttpClient(connectionManager);
        solrServer = new CommonsHttpSolrServer(solrTestUtility.getUri(), httpClient);
    }
    
    public LilyClient getLilyClient() throws IOException, InterruptedException, KeeperException, ZkConnectException, NoServersException {
        return lilyServerProxy.getClient();
    }
    
    public SolrServer getSolrServer() {
        return solrServer;
    }

    //
    // Add Index
    //
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
    
    private void addIndex(String indexName, byte[] indexerConfiguration) throws IndexerConfException, IOException, InterruptedException, KeeperException, ZkConnectException, NoServersException, IndexExistsException, IndexModelException, IndexValidityException {
        IndexerConfBuilder.build(new ByteArrayInputStream(indexerConfiguration), getLilyClient().getRepository());
        ZooKeeperItf zk = ZkUtil.connect(hbaseProxy.getZkConnectString(), 10000);
        IndexerModelImpl model = new IndexerModelImpl(zk);
        IndexDefinition index = model.newIndex(indexName);
        Map<String, String> solrShards = new HashMap<String, String>();
        solrShards.put("testshard", solrTestUtility.getUri());
        index.setSolrShards(solrShards);
        index.setConfiguration(indexerConfiguration);
        model.addIndex(index);
    }
}