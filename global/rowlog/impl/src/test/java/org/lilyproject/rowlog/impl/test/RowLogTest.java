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
package org.lilyproject.rowlog.impl.test;

import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isA;
import static org.easymock.classextension.EasyMock.createControl;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.easymock.classextension.IMocksControl;
import org.junit.*;
import org.lilyproject.rowlog.api.*;
import org.lilyproject.rowlog.api.RowLogSubscription.Type;
import org.lilyproject.rowlog.impl.RowLogConfigurationManagerImpl;
import org.lilyproject.rowlog.impl.RowLogImpl;
import org.lilyproject.rowlog.impl.RowLogMessageImpl;
import org.lilyproject.testfw.HBaseProxy;
import org.lilyproject.testfw.TestHelper;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.zookeeper.ZkUtil;
import org.lilyproject.util.zookeeper.ZooKeeperItf;


public class RowLogTest {
    private final static HBaseProxy HBASE_PROXY = new HBaseProxy();
    private static RowLogConfigurationManager configurationManager;
    private static IMocksControl control;
    private static RowLog rowLog;
    private static byte[] rowLogColumnFamily = RowLogTableUtil.ROWLOG_COLUMN_FAMILY;
    private static HTableInterface rowTable;
    private static String subscriptionId1 = "SubscriptionId";
    private static String rowLogId = "RowLogTest";
    private static ZooKeeperItf zooKeeper;
    private RowLogShard shard;
    private static List<String> subscriptionIds;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging();
        HBASE_PROXY.start();
        zooKeeper = ZkUtil.connect(HBASE_PROXY.getZkConnectString(), 10000);
        configurationManager = new RowLogConfigurationManagerImpl(zooKeeper);
        configurationManager.addRowLog(rowLogId, new RowLogConfig(60000L, true, true, 100L, 500L, 5000L));
        configurationManager.addSubscription(rowLogId, subscriptionId1, Type.VM, 1);
        subscriptionIds = Arrays.asList(new String[]{subscriptionId1});
        control = createControl();
        rowTable = RowLogTableUtil.getRowTable(HBASE_PROXY.getConf());
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        Closer.close(zooKeeper);
        Closer.close(configurationManager);
        HBASE_PROXY.stop();
    }

    @Before
    public void setUp() throws Exception {
        rowLog = new RowLogImpl(rowLogId, rowTable, rowLogColumnFamily, (byte)1, configurationManager, null);
        shard = control.createMock(RowLogShard.class);
        shard.getId();
        expectLastCall().andReturn("ShardId").anyTimes();
    }

    @After
    public void tearDown() throws Exception {
        control.reset();
    }
    
    @Test
    public void testPutMessage() throws Exception {
        shard.putMessage(isA(RowLogMessage.class), eq(subscriptionIds));
        control.replay();
        rowLog.registerShard(shard);
        byte[] rowKey = Bytes.toBytes("row1");
        RowLogMessage message = rowLog.putMessage(rowKey, null, null, null);
        List<RowLogMessage> messages = rowLog.getMessages(rowKey);
        assertEquals(1, messages.size());
        assertEquals(message, messages.get(0));
        control.verify();
    }
    
    @Test
    public void testPutMessageNoSubscriptions() throws Exception {
        configurationManager.removeSubscription(rowLogId, subscriptionId1);
        // Wait until rowlog notices the subscription has been removed
        long timeout = System.currentTimeMillis() + 10000;
        while(!rowLog.getSubscriptions().isEmpty()) {
            Thread.sleep(10);
            if (System.currentTimeMillis() > timeout)
                break;
        }
        control.replay();
        rowLog.registerShard(shard);
        byte[] rowKey = Bytes.toBytes("row1B");
        assertNull(rowLog.putMessage(rowKey, null, null, null));
        assertTrue(rowLog.getMessages(rowKey).isEmpty());
        control.verify();
        configurationManager.addSubscription(rowLogId, subscriptionId1, Type.VM, 1); // Put subscription back for the other tests
        AbstractRowLogEndToEndTest.waitForSubscription(rowLog, subscriptionId1);
    }
    
    @Test
    public void testMultipleMessages() throws Exception {
        shard.putMessage(isA(RowLogMessage.class), eq(subscriptionIds));
        expectLastCall().times(3);
        shard.removeMessage(isA(RowLogMessage.class), eq(subscriptionId1));
        
        control.replay();
        rowLog.registerShard(shard);
        byte[] rowKey = Bytes.toBytes("row2");
        RowLogMessage message1 = rowLog.putMessage(rowKey, null, null, null);
        RowLogMessage message2 = rowLog.putMessage(rowKey, null, null, null);
        RowLogMessage message3 = rowLog.putMessage(rowKey, null, null, null);
        rowLog.messageDone(message2, subscriptionId1);
        
        List<RowLogMessage> messages = rowLog.getMessages(rowKey, subscriptionId1);
        assertEquals(2, messages.size());
        assertEquals(message1, messages.get(0));
        assertEquals(message3, messages.get(1));
        control.verify();
    }
    
    @Test
    public void testNoShardsRegistered() throws Exception {

        control.replay();
        try {
            rowLog.putMessage(Bytes.toBytes("row1"), null, null, null);
            fail("Expected a RowLogException since no shards are registered");
        } catch (RowLogException expected) {
        }
        
        RowLogMessage message = new RowLogMessageImpl(System.currentTimeMillis(), Bytes.toBytes("row1"), 0L, null, rowLog);
        try {
            rowLog.messageDone(message , subscriptionId1);
            fail("Expected a RowLogException since no shards are registered");
        } catch (RowLogException expected) {
        }
        // Cleanup
        
        control.verify();
    }
    
    @Test
    public void testMessageConsumed() throws Exception {

        shard.putMessage(isA(RowLogMessage.class), eq(subscriptionIds));
        shard.removeMessage(isA(RowLogMessage.class), eq(subscriptionId1));
        expectLastCall().times(2);
        
        control.replay();
        rowLog.registerShard(shard);
        RowLogMessage message = rowLog.putMessage(Bytes.toBytes("row1"), null, null, null);

        rowLog.messageDone(message, subscriptionId1);
        assertTrue(rowLog.isMessageDone(message, subscriptionId1));
        control.verify();
    }
    
    @Test
    public void testgetMessages() throws Exception {
        String subscriptionId3 = "subscriptionId3";
        
        RowLogConfigurationManagerImpl configurationManager = new RowLogConfigurationManagerImpl(zooKeeper);
        configurationManager.addSubscription(rowLogId, subscriptionId3, Type.VM, 3);
        
        long waitUntil = System.currentTimeMillis() + 10000;
        while (waitUntil > System.currentTimeMillis()) {
            if (rowLog.getSubscriptions().size() > 1)
                break;
        }
        
        assertEquals(2, rowLog.getSubscriptions().size());
        List<String> ids = new ArrayList<String>(subscriptionIds);
        ids.add(subscriptionId3);
        shard.putMessage(isA(RowLogMessage.class), eq(ids));
        expectLastCall().times(2);
        shard.removeMessage(isA(RowLogMessage.class), eq(subscriptionId1));
        shard.removeMessage(isA(RowLogMessage.class), eq(subscriptionId3));

        control.replay();

        rowLog.registerShard(shard);
        byte[] rowKey = Bytes.toBytes("row3");
        RowLogMessage message1 = rowLog.putMessage(rowKey, null, null, null);
        RowLogMessage message2 = rowLog.putMessage(rowKey, null, null, null);

        rowLog.messageDone(message1, subscriptionId1);
        rowLog.messageDone(message2, subscriptionId3);
        
        List<RowLogMessage> messages;
        messages = rowLog.getMessages(rowKey);
        assertEquals(2, messages.size());
        
        messages = rowLog.getMessages(rowKey, subscriptionId1);
        assertEquals(1, messages.size());
        assertEquals(message2, messages.get(0));
        
        messages = rowLog.getMessages(rowKey, subscriptionId3);
        assertEquals(1, messages.size());
        assertEquals(message1, messages.get(0));
        
        messages = rowLog.getMessages(rowKey, subscriptionId1, subscriptionId3);
        assertEquals(2, messages.size());
        
        control.verify();
        configurationManager.removeSubscription(rowLogId, subscriptionId3);
    }
}
