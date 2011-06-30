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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.*;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.kauriproject.runtime.module.javaservice.JavaServiceManager;
import org.lilyproject.client.LilyClient;
import org.lilyproject.client.NoServersException;
import org.lilyproject.indexer.model.api.*;
import org.lilyproject.indexer.model.impl.IndexerModelImpl;
import org.lilyproject.indexer.model.indexerconf.IndexerConfBuilder;
import org.lilyproject.solrtestfw.SolrProxy;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.test.TestHomeUtil;
import org.lilyproject.util.zookeeper.ZkConnectException;
import org.lilyproject.util.zookeeper.ZkUtil;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

public class LilyServerProxy {
    public static final String LILY_CONF_DIR = "lily.conf.dir";
    public static final String LILY_CONF_CUSTOMDIR = "lily.conf.customdir";

    private Log log = LogFactory.getLog(getClass());

    private Mode mode;

    public enum Mode { EMBED, CONNECT }
    private static String LILY_MODE_PROP_NAME = "lily.lilyserverproxy.mode";

    private LilyServerTestUtility lilyServerTestUtility;

    private File testHome;

    private LilyClient lilyClient;
    private WriteableIndexerModel indexerModel;
    private ZooKeeperItf zooKeeper;

    public LilyServerProxy() throws IOException {
        this(null);
    }

    /**
     * LilyServerProxy starts a proxy for lily which either :
     *   - connects to an already running lily (using launch-test-lily) (CONNECT mode)
     *   - starts a new LilyServerTestUtility (EMBED mode)
     *   
     * <p>In the EMBED mode the default configuration files used are the files present 
     *    in the resource "org/lilyproject/lilyservertestfw/conf/".
     *    These files are copied into the resource at build-time from "cr/process/server/conf".  
     * <br>On top of these default configuration files, custom configuration files can be used. 
     *     The path to these custom configuration files should be put in the system property 
     *     "lily.conf.customdir". 
     * @param mode the mode (CONNECT or EMBED) in which to start the proxy.
     */
    public LilyServerProxy(Mode mode) throws IOException {
        if (mode == null) {
            String lilyModeProp = System.getProperty(LILY_MODE_PROP_NAME);
            if (lilyModeProp == null || lilyModeProp.equals("") || lilyModeProp.equals("embed")) {
                this.mode = Mode.EMBED;
            } else if (lilyModeProp.equals("connect")) {
                this.mode = Mode.CONNECT;
            } else {
                throw new RuntimeException("Unexpected value for " + LILY_MODE_PROP_NAME + ": " + lilyModeProp);
            }
        } else {
            this.mode = mode;
        }
    }

    public void setTestHome(File testHome) throws IOException {
        if (mode != Mode.EMBED) {
            throw new RuntimeException("testHome should only be set when mode is EMBED");
        }
        this.testHome = testHome;
    }

    private void initTestHome() throws IOException {
        if (testHome == null) {
            testHome = TestHomeUtil.createTestHome("lily-serverproxy-");
        }

        FileUtils.forceMkdir(testHome);
        FileUtils.cleanDirectory(testHome);
    }

    public void start() throws Exception {
        System.out.println("LilyServerProxy mode: " + mode);

        switch (mode) {
            case EMBED:
                initTestHome();
                System.out.println("LilySeverProxy embedded mode temp dir: " + testHome.getAbsolutePath());
                // Setup default conf dir : extract conf from resources into testHome
                File defaultConfDir = new File(testHome, "conf");
                FileUtils.forceMkdir(defaultConfDir);
                extractTemplateConf(defaultConfDir);
                // Get custom conf dir 
                String customConfDir = System.getProperty(LILY_CONF_CUSTOMDIR);
                
                lilyServerTestUtility = new LilyServerTestUtility(defaultConfDir.getAbsolutePath(), customConfDir);
                lilyServerTestUtility.start();
                break;
            case CONNECT:
                break;
            default:
                throw new RuntimeException("Unexpected mode: " + mode);
        }
    }
    
    public void stop() {
        Closer.close(lilyServerTestUtility);

        if (mode == Mode.CONNECT) {
            Closer.close(zooKeeper);
            Closer.close(indexerModel);
        }

        Closer.close(lilyClient);
    }
    
    public synchronized LilyClient getClient() throws IOException, InterruptedException, KeeperException,
            ZkConnectException, NoServersException {
        if (lilyClient == null) {
            lilyClient = new LilyClient("localhost:2181", 30000);
        }
        return lilyClient;
    }

    /**
     * Get ZooKeeper.
     *
     * <p></p>Be careful what you do with this ZooKeeper instance, as it shared with other users: mind the single
     * event dispatching thread, don't use the global watchers. If these are a problem for you, you can as well
     * create your own ZooKeeper client.
     */
    public synchronized ZooKeeperItf getZooKeeper() throws ZkConnectException {
        if (zooKeeper == null) {
            switch (mode) {
                case EMBED:
                    JavaServiceManager serviceMgr = lilyServerTestUtility.getRuntime().getJavaServiceManager();
                    zooKeeper = (ZooKeeperItf)serviceMgr.getService(ZooKeeperItf.class);
                    break;
                case CONNECT:
                    zooKeeper = ZkUtil.connect("localhost:2181", 30000);
                    break;
                default:
                    throw new RuntimeException("Unexpected mode: " + mode);
            }
        }
        return zooKeeper;
    }

    public synchronized WriteableIndexerModel getIndexerModel() throws ZkConnectException, InterruptedException, KeeperException {
        if (indexerModel == null) {
            switch (mode) {
                case EMBED:
                    JavaServiceManager serviceMgr = lilyServerTestUtility.getRuntime().getJavaServiceManager();
                    indexerModel = (WriteableIndexerModel)serviceMgr.getService(WriteableIndexerModel.class);
                    break;
                case CONNECT:
                    indexerModel = new IndexerModelImpl(getZooKeeper());
                    break;
                default:
                    throw new RuntimeException("Unexpected mode: " + mode);
            }
        }
        return indexerModel;
    }
    
    private void extractTemplateConf(File confDir) throws URISyntaxException, IOException {
        URL confUrl = getClass().getClassLoader().getResource(ConfUtil.CONF_RESOURCE_PATH);
        ConfUtil.copyConfResources(confUrl, ConfUtil.CONF_RESOURCE_PATH, confDir);
    }

    /**
     * Adds an index from in index configuration contained in a resource.
     * 
     * <p>This method waits for the index subscription to be known by the MQ rowlog (or until a given timeout has passed).
     * Only when this is the case record creates or updates will result in messages to be created on the MQ rowlog.
     * <p>Not that when the messages from the MQ rowlog are processed, the data has been put in solr but this data might not
     * be visible until the solr index has been committed. See {@link SolrProxy#commit()}.
     * 
     * @param indexName name of the index
     * @param indexerConf path to the resource containing the index configuration
     * @param timeout time to wait for the index subscription to be known by the MQ rowlog.
     * @param waitForIndexerModel boolean indicating the call has to wait until the indexerModel knows the subscriptionId of the new index
     * @param waitForMQRowlog boolean indicating the call has to wait until the MQ rowlog knows the subscriptionId of the new index. 
     *        This can only be true if the waitForIndexerModel is true as well. 
     * @return false if the index subscription was not known by the MQ rowlog within the given timeout.
     */
    public boolean addIndexFromResource(String indexName, String indexerConf, long timeout, boolean waitForIndexerModel, boolean waitForMQRowlog) throws Exception {
        InputStream is = getClass().getClassLoader().getResourceAsStream(indexerConf);
        byte[] indexerConfiguration = IOUtils.toByteArray(is);
        is.close();
        return addIndex(indexName, indexerConfiguration, timeout, waitForIndexerModel, waitForMQRowlog);
    }
    
    /**
     * Shortcut method with waitForIndexerModel and waitForMQRowlog put to true
     */
    public boolean addIndexFromResource(String indexName, String indexerConf, long timeout) throws Exception {
        return addIndexFromResource(indexName, indexerConf, timeout, true, true);
    }

    /**
     * Adds an index from in index configuration contained in a file.
     * 
     * <p>This method waits for the index subscription to be known by the MQ rowlog (or until a given timeout has passed).
     * Only when this is the case record creates or updates will result in messages to be created on the MQ rowlog.
     * 
     * @param indexName name of the index
     * @param indexerConf path to the file containing the index configuration
     * @param timeout time to wait for the index subscription to be known by the MQ rowlog.
     * @param waitForIndexerModel boolean indicating the call has to wait until the indexerModel knows the subscriptionId of the new index
     * @param waitForMQRowlog boolean indicating the call has to wait until the MQ rowlog knows the subscriptionId of the new index. 
     *        This can only be true if the waitForIndexerModel is true as well. 
     * @return false if the index subscription was not known by the MQ rowlog within the given timeout.
     */
    public boolean addIndexFromFile(String indexName, String indexerConf, long timeout, boolean waitForIndexerModel, boolean waitForMQRowlog) throws Exception {
        byte[] indexerConfiguration = FileUtils.readFileToByteArray(new File(indexerConf));
        return addIndex(indexName, indexerConfiguration, timeout, waitForIndexerModel, waitForMQRowlog);
    }
    
    /**
     * Shortcut method with waitForIndexerModel and waitForMQRowlog put to true
     */
    public boolean addIndexFromFile(String indexName, String indexerConf, long timeout) throws Exception {
        return addIndexFromFile(indexName, indexerConf, timeout, true, true);
    }

    private boolean addIndex(String indexName, byte[] indexerConfiguration, long timeout, boolean waitForIndexerModel, boolean waitForMQRowlog) throws Exception {
        long tryUntil = System.currentTimeMillis() + timeout; 
        IndexerConfBuilder.build(new ByteArrayInputStream(indexerConfiguration), getClient().getRepository());
        WriteableIndexerModel indexerModel = getIndexerModel();
        IndexDefinition index = indexerModel.newIndex(indexName);
        Map<String, String> solrShards = new HashMap<String, String>();
        solrShards.put("shard1", "http://localhost:8983/solr");
        index.setSolrShards(solrShards);
        index.setConfiguration(indexerConfiguration);
        indexerModel.addIndex(index);
        
        String subscriptionId = null;
        if (waitForIndexerModel) {
            // Wait for subscriptionId to be known by indexerModel
            subscriptionId = waitIndexerModelKnowsSubscriptionId(indexName, tryUntil, timeout, indexerModel);
            if (subscriptionId == null)
                return false;
        } else {
            return true;
        }
        
        if (waitForMQRowlog) {
            // Wait for RowLog to know the mq subscriptionId
            return waitMQRowlogKnowsSubscriptionId(subscriptionId, tryUntil, timeout);
        } else {
            return true;
        }
    }

    
    public String waitIndexerModelKnowsSubscriptionId(String indexName, long timeout, WriteableIndexerModel indexerModel) throws Exception {
        return waitIndexerModelKnowsSubscriptionId(indexName, System.currentTimeMillis()+timeout, timeout, indexerModel);
    }
    
    private String waitIndexerModelKnowsSubscriptionId(String indexName, long tryUntil, long timeout, WriteableIndexerModel indexerModel) throws Exception {
        String subscriptionId = null;

        // Wait for index to be known by indexerModel 
        while (!indexerModel.hasIndex(indexName) && System.currentTimeMillis() < tryUntil) {
            Thread.sleep(50);
        }
        if (!indexerModel.hasIndex(indexName)) {
            log.info("Index '" + indexName + "' not known to indexerModel within " + timeout + "ms");
            return subscriptionId;
        }
        
        IndexDefinition indexDefinition = indexerModel.getIndex(indexName);
        subscriptionId = indexDefinition.getQueueSubscriptionId();
        while (subscriptionId == null && System.currentTimeMillis() < tryUntil) {
            Thread.sleep(50);
            subscriptionId = indexerModel.getIndex(indexName).getQueueSubscriptionId();
        }
        if (subscriptionId == null) {
            log.info("SubscriptionId for index '" + indexName + "' not known to indexerModel within " + timeout + "ms");
        }
        return subscriptionId;
    }

    public boolean waitMQRowlogKnowsSubscriptionId(String subscriptionId, String indexName, long timeout) throws Exception {
        return waitMQRowlogKnowsSubscriptionId(subscriptionId, System.currentTimeMillis()+timeout, timeout);
    }
    
    private boolean waitMQRowlogKnowsSubscriptionId(String subscriptionId, long tryUntil, long timeout) throws Exception {
        ObjectName objectName = new ObjectName("RowLog:name=mq");
        List<String> subscriptionIds = new ArrayList<String>();
        switch (mode) {
        case EMBED:
            MBeanServer platformMBeanServer = java.lang.management.ManagementFactory.getPlatformMBeanServer();
            
            subscriptionIds = (List<String>)platformMBeanServer.getAttribute(objectName, "SubscriptionIds");
            while (!subscriptionIds.contains(subscriptionId) && System.currentTimeMillis() < tryUntil) {
                Thread.sleep(50);
                subscriptionIds = (List<String>)platformMBeanServer.getAttribute(objectName, "SubscriptionIds");
            }
            if (!subscriptionIds.contains(subscriptionId)) {
                log.info("SubscriptionId '" + subscriptionId + "' not known to mq rowlog within " + timeout + "ms");
                return false;
            }
            break;
        case CONNECT:
            JMXConnector connector = null;
            try {
                String hostport = "localhost:10102";
                JMXServiceURL url = new JMXServiceURL("service:jmx:rmi://" + hostport + "/jndi/rmi://" + hostport + "/jmxrmi");
                connector = JMXConnectorFactory.connect(url);
                connector.connect();
                try {
                    subscriptionIds = (List<String>)connector.getMBeanServerConnection().getAttribute(objectName, "SubscriptionIds");
                } catch (InstanceNotFoundException e) {
                    // Ignore, we keep trying
                }
                while (!subscriptionIds.contains(subscriptionId) && System.currentTimeMillis() < tryUntil) {
                    Thread.sleep(50);
                    try {
                        subscriptionIds = (List<String>)connector.getMBeanServerConnection().getAttribute(objectName, "SubscriptionIds");
                    } catch (InstanceNotFoundException e) {
                        // Ignore, we keep trying
                    } 
                }
                if (!subscriptionIds.contains(subscriptionId)) {
                    log.info("SubscriptionId '" + subscriptionId + "' not known to mq rowlog within " + timeout + "ms");
                    return false;
                }
            } finally {
                if (connector != null)
                    connector.close();
            }
            break;

        default:
            throw new RuntimeException("Unexpected mode: " + mode);
        }
        return true;
    }
}
