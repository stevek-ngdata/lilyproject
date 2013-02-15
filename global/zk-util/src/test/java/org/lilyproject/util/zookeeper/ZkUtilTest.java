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
package org.lilyproject.util.zookeeper;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NotEmptyException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilyproject.hadooptestfw.TestHelper;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.net.NetUtils;

public class ZkUtilTest {

    private static MiniZooKeeperCluster ZK_CLUSTER;
    private static File ZK_DIR;
    private static int ZK_CLIENT_PORT;
    private static ZooKeeperItf ZK;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging("org.lilyproject.util.zookeeper");

        ZK_DIR = new File(System.getProperty("java.io.tmpdir") + File.separator + "lily.zkutiltest");
        ZK_CLIENT_PORT = NetUtils.getFreePort();

        ZK_CLUSTER = new MiniZooKeeperCluster();
        ZK_CLUSTER.setDefaultClientPort(ZK_CLIENT_PORT);
        ZK_CLUSTER.startup(ZK_DIR);

        ZK = ZkUtil.connect("localhost:" + ZK_CLIENT_PORT, 30000);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        Closer.close(ZK);
        if (ZK_CLUSTER != null) {
            ZK_CLUSTER.shutdown();
        }
        FileUtils.deleteDirectory(ZK_DIR);
    }

    @Test
    public void testDelete_BasicCase() throws InterruptedException, KeeperException {
        String path = "/testDelete/BasicCase";
        ZkUtil.createPath(ZK, path);

        // Sanity check
        assertNotNull(ZK.exists(path, false));

        ZkUtil.deleteNode(ZK, path);

        assertNull(ZK.exists(path, false));
    }

    @Test
    public void testDelete_NodeDoesntExist() throws KeeperException, InterruptedException {
        String path = "/testDelete/NodeDoesntExist";

        ZkUtil.deleteNode(ZK, path);

        // Obviously it shouldn't be created now either
        assertNull(ZK.exists(path, false));
    }

    @Test(expected = NotEmptyException.class)
    public void testDelete_NodeHasChildren() throws InterruptedException, KeeperException {
        String path = "/testDelete/NodeHasChildren";

        ZkUtil.createPath(ZK, path + "/child");

        ZkUtil.deleteNode(ZK, path);
    }

}
