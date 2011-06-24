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

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.zookeeper.KeeperException;
import org.lilyproject.client.LilyClient;
import org.lilyproject.client.NoServersException;
import org.lilyproject.indexer.model.api.*;
import org.lilyproject.indexer.model.impl.IndexerModelImpl;
import org.lilyproject.indexer.model.indexerconf.IndexerConfBuilder;
import org.lilyproject.indexer.model.indexerconf.IndexerConfException;
import org.lilyproject.solrtestfw.SolrProxy;
import org.lilyproject.testfw.HBaseProxy;
import org.lilyproject.util.test.TestHomeUtil;
import org.lilyproject.util.zookeeper.ZkConnectException;
import org.lilyproject.util.zookeeper.ZkUtil;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

public class LilyProxy {
    private HBaseProxy hbaseProxy;
    private LilyServerProxy lilyServerProxy;
    private SolrProxy solrProxy;
    private Mode mode;
    private File testHome;

    public enum Mode { EMBED, CONNECT }
    private static String MODE_PROP_NAME = "lily.lilyproxy.mode";

    public LilyProxy() throws IOException {
        this(null);
    }

    public LilyProxy(Mode mode) throws IOException {
        if (mode == null) {
            String modeProp = System.getProperty(MODE_PROP_NAME);
            if (modeProp == null || modeProp.equals("") || modeProp.equals("embed")) {
                this.mode = Mode.EMBED;
            } else if (modeProp.equals("connect")) {
                this.mode = Mode.CONNECT;
            } else {
                throw new RuntimeException("Unexpected value for " + MODE_PROP_NAME + ": " + modeProp);
            }
        } else {
            this.mode = mode;
        }

        if (this.mode == Mode.EMBED) {
            testHome = TestHomeUtil.createTestHome("lily-proxy-");
        }

        // We imply our mode on all of the specific Proxy's. This is because certain behavior (the state reset)
        // requires that they all be in the same mode.
        hbaseProxy = new HBaseProxy(this.mode == Mode.EMBED ? HBaseProxy.Mode.EMBED : HBaseProxy.Mode.CONNECT, testHome);
        hbaseProxy.setCleanStateOnConnect(false);
        solrProxy = new SolrProxy(this.mode == Mode.EMBED ? SolrProxy.Mode.EMBED : SolrProxy.Mode.CONNECT, testHome);
        lilyServerProxy = new LilyServerProxy(this.mode == Mode.EMBED ?
                LilyServerProxy.Mode.EMBED : LilyServerProxy.Mode.CONNECT, testHome);
    }

    public void start(String solrSchema) throws Exception {
        System.out.println("LilyProxy mode: " + mode);

        if (mode == Mode.CONNECT) {
            // First reset the state
            System.out.println("Calling reset state flag on externally launched Lily...");
            try {
                String hostport = "localhost:10102";
                JMXServiceURL url = new JMXServiceURL("service:jmx:rmi://" + hostport + "/jndi/rmi://" + hostport + "/jmxrmi");
                JMXConnector connector = JMXConnectorFactory.connect(url);
                connector.connect();
                ObjectName lilyLauncher = new ObjectName("LilyLauncher:name=Launcher");
                connector.getMBeanServerConnection().invoke(lilyLauncher, "resetLilyState", new Object[0], new String[0]);
            } catch (Exception e) {
                throw new Exception("Resetting Lily state failed.", e);
            }
            System.out.println("State reset done.");
        }

        if (mode == Mode.EMBED) {
            FileUtils.forceMkdir(testHome);
            FileUtils.cleanDirectory(testHome);
        }

        hbaseProxy.start();
        solrProxy.start(solrSchema);
        lilyServerProxy.start(hbaseProxy.getZkConnectString());
    }
    
    public void stop() throws Exception {
        if (lilyServerProxy != null)
            lilyServerProxy.stop();
        if (solrProxy != null)
            solrProxy.stop();
        if (hbaseProxy != null)
            hbaseProxy.stop();

        if (testHome != null) {
            FileUtils.deleteDirectory(testHome);
        }
    }
    
    public LilyClient getLilyClient() throws IOException, InterruptedException, KeeperException, ZkConnectException, NoServersException {
        return lilyServerProxy.getClient();
    }
    
    public SolrServer getSolrServer() {
        return solrProxy.getSolrServer();
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
        solrShards.put("testshard", solrProxy.getUri());
        index.setSolrShards(solrShards);
        index.setConfiguration(indexerConfiguration);
        model.addIndex(index);
    }
}