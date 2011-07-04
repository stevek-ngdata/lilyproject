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

import org.apache.commons.io.FileUtils;
import org.lilyproject.solrtestfw.SolrProxy;
import org.lilyproject.hadooptestfw.HBaseProxy;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.test.TestHomeUtil;

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
    private boolean started = false;
    private boolean hasBeenStarted = false;

    public enum Mode { EMBED, CONNECT, HADOOP_CONNECT }
    public static String MODE_PROP_NAME = "lily.lilyproxy.mode";

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
            } else if (modeProp.equals("hadoop-connect")) {
                this.mode = Mode.HADOOP_CONNECT;
            } else {
                throw new RuntimeException("Unexpected value for " + MODE_PROP_NAME + ": " + modeProp);
            }
        } else {
            this.mode = mode;
        }

        HBaseProxy.Mode hbaseMode;
        SolrProxy.Mode solrMode;
        LilyServerProxy.Mode lilyServerMode;

        // LilyProxy imposes its mode on all of the specific Proxy's. This is because the special resetLilyState
        // operation in case of connect requires they are all in the same mode.
        // The special HADOOP_CONNECT mode is mainly intended for Lily's own tests: many tests suppose only
        // hadoop is running externally because they launch specific parts of the Lily implementation themselves,
        // combined with the fact that the mode should be the same for all tests.
        switch (this.mode) {
            case EMBED:
                hbaseMode = HBaseProxy.Mode.EMBED;
                solrMode = SolrProxy.Mode.EMBED;
                lilyServerMode = LilyServerProxy.Mode.EMBED;
                break;
            case CONNECT:
                hbaseMode = HBaseProxy.Mode.CONNECT;
                solrMode = SolrProxy.Mode.CONNECT;
                lilyServerMode = LilyServerProxy.Mode.CONNECT;
                break;
            case HADOOP_CONNECT:
                hbaseMode = HBaseProxy.Mode.CONNECT;
                solrMode = SolrProxy.Mode.EMBED;
                lilyServerMode = LilyServerProxy.Mode.EMBED;
                break;
            default:
                throw new RuntimeException("Unexpected mode: " + this.mode);
        }

        hbaseProxy = new HBaseProxy(hbaseMode);
        if (this.mode == Mode.CONNECT) {
            // we'll do the reset through the special JMX call
            hbaseProxy.setCleanStateOnConnect(false);
        }
        hbaseProxy.setEnableMapReduce(true);
        solrProxy = new SolrProxy(solrMode);
        lilyServerProxy = new LilyServerProxy(lilyServerMode);
    }

    public void start() throws Exception {
        start(null);
    }

    public void start(byte[] solrSchemaData) throws Exception {
        if (started) {
            throw new IllegalStateException("LilyProxy is already started.");
        } else {
            started = true;
        }

        if (hasBeenStarted && this.mode == Mode.EMBED) {
            // In embed mode, we can't support multiple start-stop sequences since
            // HBase/Hadoop does not shut down all processes synchronously.
            throw new IllegalStateException("LilyProxy can only be started once in a JVM when using embed mode.");
        } else {
            hasBeenStarted = true;
        }

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
                connector.close();
            } catch (Exception e) {
                throw new Exception("Resetting Lily state failed.", e);
            }
            System.out.println("State reset done.");
        }

        if (mode == Mode.EMBED || mode == Mode.HADOOP_CONNECT) {
            testHome = TestHomeUtil.createTestHome("lily-proxy-");
            if (mode == Mode.EMBED)
                hbaseProxy.setTestHome(new File(testHome, "hadoop"));
            solrProxy.setTestHome(new File(testHome, "solr"));
            lilyServerProxy.setTestHome(new File(testHome, "lilyserver"));
        }

        hbaseProxy.start();
        solrProxy.start(solrSchemaData);
        lilyServerProxy.start();
    }
    
    public void stop() throws Exception {
        Closer.close(lilyServerProxy);
        Closer.close(solrProxy);
        Closer.close(hbaseProxy);

        if (testHome != null) {
            FileUtils.deleteDirectory(testHome);
        }

        started = false;
    }

    public HBaseProxy getHBaseProxy() {
        return hbaseProxy;
    }

    public LilyServerProxy getLilyServerProxy() {
        return lilyServerProxy;
    }

    public SolrProxy getSolrProxy() {
        return solrProxy;
    }
    
    /**
     * Waits for all messages from the WAL and MQ to be processed and optionally commits the solr index.
     * 
     * @param timeout the maximum time to wait
     * @param
     * @return false if the timeout was reached before all messages were processed
     */
    public boolean waitWalAndMQMessagesProcessed(long timeout, boolean commitSolr) throws Exception {
        boolean result = hbaseProxy.waitWalAndMQMessagesProcessed(timeout);
        if (commitSolr)
            solrProxy.commit();
        return result;
    }
    
    /**
     * Waits for all messages from the WAL and MQ to be processed and commits the solr index by default.
     * 
     * @param timeout the maximum time to wait
     * @param
     * @return false if the timeout was reached before all messages were processed
     */
    public boolean waitWalAndMQMessagesProcessed(long timeout) throws Exception {
        return waitWalAndMQMessagesProcessed(timeout, true);
    }
}
