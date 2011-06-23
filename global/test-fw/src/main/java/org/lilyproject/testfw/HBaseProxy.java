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
package org.lilyproject.testfw;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.zookeeper.*;
import org.lilyproject.testfw.fork.HBaseTestingUtility;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import static org.apache.zookeeper.ZooKeeper.States.CONNECTED;

/**
 * Provides access to HBase, either by starting an embedded HBase or by connecting to a running HBase.
 *
 * <p>This is intended for usage in test cases.
 *
 * <p><b>VERY VERY IMPORTANT</b>: when connecting to an existing HBase, this class will DELETE ALL ROWS
 * FROM ALL TABLES!
 */
public class HBaseProxy {
    private static Mode MODE;
    private static Configuration CONF;
    private static HBaseTestingUtility TEST_UTIL;
    private static File TEST_HOME;
    private CleanupUtil cleanupUtil;

    private enum Mode { EMBED, CONNECT }
    private static String HBASE_MODE_PROP_NAME = "lily.test.hbase";

    public void start() throws Exception {
        start(Collections.<String, byte[]>emptyMap());
    }

    /**
     *
     * @param timestampReusingTables map containing table name as key and column family as value. Since HBase does
     *                               not support supporting writing data older than a deletion thombstone, these tables
     *                               will be compacted and waited for until inserting data works again.
     */
    public void start(Map<String, byte[]> timestampReusingTables) throws Exception {
        String hbaseModeProp = System.getProperty(HBASE_MODE_PROP_NAME);
        if (hbaseModeProp == null || hbaseModeProp.equals("") || hbaseModeProp.equals("embed")) {
            MODE = Mode.EMBED;
        } else if (hbaseModeProp.equals("connect")) {
            MODE = Mode.CONNECT;
        } else {
            throw new RuntimeException("Unexpected value for " + HBASE_MODE_PROP_NAME + ": " + hbaseModeProp);
        }

        System.out.println("HBase usage mode: " + MODE);

        CONF = HBaseConfiguration.create();

        switch (MODE) {
            case EMBED:
                addHBaseTestProps(CONF);
                addUserProps(CONF);

                if (TEST_HOME != null) {
                    TestHomeUtil.cleanupTestHome(TEST_HOME);
                }
                TEST_HOME = TestHomeUtil.createTestHome();

                TEST_UTIL = HBaseTestingUtilityFactory.create(CONF, TEST_HOME);
                TEST_UTIL.startMiniCluster(1);

                // In the past, it happened that HMaster would not become initialized, blocking later on
                // the proper shutdown of the mini cluster. Now added this as an early warning mechanism.
                long before = System.currentTimeMillis();
                while (!TEST_UTIL.getMiniHBaseCluster().getMaster().isInitialized()) {
                    if (System.currentTimeMillis() - before > 60000) {
                        throw new RuntimeException("HMaster.isInitialized() does not become true.");
                    }
                    System.out.println("Waiting for HMaster to be initialized");
                    Thread.sleep(500);
                }

                CONF = TEST_UTIL.getConfiguration();
                cleanupUtil = new CleanupUtil(CONF, getZkConnectString());
                break;
            case CONNECT:
                CONF.set("hbase.zookeeper.quorum", "localhost");
                CONF.set("hbase.zookeeper.property.clientPort", "2181"); // matches HBaseRunner
                addUserProps(CONF);

                cleanupUtil = new CleanupUtil(CONF, getZkConnectString());
                cleanupUtil.cleanZooKeeper();

                Map<String, byte[]> allTimestampReusingTables = new HashMap<String, byte[]>();
                allTimestampReusingTables.putAll(cleanupUtil.getDefaultTimestampReusingTables());
                allTimestampReusingTables.putAll(timestampReusingTables);
                cleanupUtil.cleanTables(allTimestampReusingTables);
                break;
            default:
                throw new RuntimeException("Unexpected mode: " + MODE);
        }
    }

    public String getZkConnectString() {
        return CONF.get("hbase.zookeeper.quorum") + ":" + CONF.get("hbase.zookeeper.property.clientPort");
    }
    
    /**
     * Adds all system property prefixed with "lily.test.hbase." to the HBase configuration.
     */
    private void addUserProps(Configuration conf) {
        Properties sysProps = System.getProperties();
        for (Map.Entry<Object, Object> entry : sysProps.entrySet()) {
            String name = entry.getKey().toString();
            if (name.startsWith("lily.test.hbase.")) {
                String hbasePropName = name.substring("lily.test.".length());
                conf.set(hbasePropName, entry.getValue().toString());
            }
        }
    }

    protected static void addHBaseTestProps(Configuration conf) {
        // The following properties are from HBase's src/test/resources/hbase-site.xml
        conf.set("hbase.regionserver.msginterval", "1000");
        conf.set("hbase.client.pause", "5000");
        conf.set("hbase.client.retries.number", "4");
        conf.set("hbase.master.meta.thread.rescanfrequency", "10000");
        conf.set("hbase.server.thread.wakefrequency", "1000");
        conf.set("hbase.regionserver.handler.count", "5");
        conf.set("hbase.master.info.port", "-1");
        conf.set("hbase.regionserver.info.port", "-1");
        conf.set("hbase.regionserver.info.port.auto", "true");
        conf.set("hbase.master.lease.thread.wakefrequency", "3000");
        conf.set("hbase.regionserver.optionalcacheflushinterval", "1000");
        conf.set("hbase.regionserver.safemode", "false");
    }

    public void stop() throws Exception {
        // Close connections with HBase and HBase's ZooKeeper handles
        //HConnectionManager.deleteConnectionInfo(CONF, true);
        HConnectionManager.deleteAllConnections(true);

        if (MODE == Mode.EMBED) {
            // Since HBase mini cluster shutdown has a tendency of sometimes failing (hanging waiting on master
            // to end), add a protection for this so that we do not run indefinitely. Especially important not to
            // annoy the other projects on our Hudson server.
            Thread stopHBaseThread = new Thread() {
                @Override
                public void run() {
                    try {
                        TEST_UTIL.shutdownMiniCluster();
                        TEST_UTIL = null;
                    } catch (IOException e) {
                        System.out.println("Error shutting down mini cluster.");
                        e.printStackTrace();
                    }
                }
            };
            stopHBaseThread.start();
            stopHBaseThread.join(60000);
            if (stopHBaseThread.isAlive()) {
                System.err.println("Unable to stop embedded mini cluster within predetermined timeout.");
                System.err.println("Dumping stack for future investigation.");
                ReflectionUtils.printThreadInfo(new PrintWriter(System.out), "Thread dump");
                System.out.println("Will now try to interrupt the mini-cluster-stop-thread and give it some more time to end.");
                stopHBaseThread.interrupt();
                stopHBaseThread.join(20000);
                throw new Exception("Failed to stop the mini cluster within the predetermined timeout.");
            }
        }
        CONF = null;

        if (TEST_HOME != null) {
            TestHomeUtil.cleanupTestHome(TEST_HOME);
        }
    }

    public Configuration getConf() {
        return CONF;
    }

    public FileSystem getBlobFS() throws IOException, URISyntaxException {
        if (MODE == Mode.EMBED) {
            return TEST_UTIL.getDFSCluster().getFileSystem();
        } else {
            String dfsUri = System.getProperty("lily.test.dfs");

            if (dfsUri == null) {
                dfsUri = "hdfs://localhost:8020";
            }

            return FileSystem.get(new URI(dfsUri), getConf());
        }
    }

    public void cleanTables() throws Exception {
        cleanupUtil.cleanTables();
    }
}
