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
package org.lilycms.rowlog.impl.test;


import static org.easymock.classextension.EasyMock.createControl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.util.Bytes;
import org.easymock.classextension.IMocksControl;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilycms.rowlog.api.RowLog;
import org.lilycms.rowlog.api.RowLogMessage;
import org.lilycms.rowlog.impl.RowLogMessageImpl;
import org.lilycms.rowlog.impl.RowLogShardImpl;
import org.lilycms.testfw.TestHelper;

public class RowLogShardTest {

	private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
	private static RowLogShardImpl shard;
	private static IMocksControl control;
    private static RowLog rowLog;
    
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		TestHelper.setupLogging();
		TEST_UTIL.startMiniCluster(1);
		control = createControl();
		rowLog = control.createMock(RowLog.class);
		shard = new RowLogShardImpl("TestShard", TEST_UTIL.getConfiguration(), rowLog);
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		TEST_UTIL.shutdownMiniCluster();
	}


	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
		control.reset();
	}
	
	@Test
	public void testSingleMessage() throws Exception {
		control.replay();
		int consumerId = 1;
		byte[] messageId1 = Bytes.toBytes("messageId1");
		RowLogMessageImpl message1 = new RowLogMessageImpl(messageId1, Bytes.toBytes("row1"), 0L, null, rowLog);
		shard.putMessage(message1, consumerId);
		
		RowLogMessage next = shard.next(consumerId);
		assertEquals(message1, next);
		
		shard.removeMessage(message1, consumerId);
		assertNull(shard.next(consumerId));
		control.verify();
	}
	
	@Test
	public void testMultipleMessages() throws Exception {
		IMocksControl control = createControl();
		
		control.replay();
		int consumerId = 1;
		byte[] messageId1 = Bytes.toBytes("messageId1");
		RowLogMessageImpl message1 = new RowLogMessageImpl(messageId1, Bytes.toBytes("row1"), 0L, null, rowLog);
		byte[] messageId2 = Bytes.toBytes("messageId2");
		RowLogMessageImpl message2 = new RowLogMessageImpl(messageId2, Bytes.toBytes("row2"), 0L, null, rowLog);

		shard.putMessage(message1, consumerId);
		shard.putMessage(message2, consumerId);

		RowLogMessage next = shard.next(consumerId);
		assertEquals(message1, next);

		shard.removeMessage(message1, consumerId);
		next = shard.next(consumerId);
		assertEquals(message2, next);
		
		shard.removeMessage(message2, consumerId);
		assertNull(shard.next(consumerId));
		control.verify();
	}
	
	
	@Test
	public void testMultipleConsumers() throws Exception {
		
		control.replay();
		byte[] messageId1 = Bytes.toBytes("messageId1");
		RowLogMessageImpl message1 = new RowLogMessageImpl(messageId1, Bytes.toBytes("row1"), 1L, null, rowLog);
		byte[] messageId2 = Bytes.toBytes("messageId2");
		RowLogMessageImpl message2 = new RowLogMessageImpl(messageId2, Bytes.toBytes("row2"), 1L, null, rowLog);
		
		int consumerId1 = 1;
		int consumerId2 = 2;
		shard.putMessage(message1, consumerId1);
		shard.putMessage(message2, consumerId2);
		RowLogMessage next = shard.next(consumerId1);
		assertEquals(message1, next);
		
		next = shard.next(consumerId2);
		assertEquals(message2, next);
		shard.removeMessage(message1, consumerId1);
		shard.removeMessage(message2, consumerId2);
		assertNull(shard.next(consumerId1));
		assertNull(shard.next(consumerId2));
		control.verify();
	}
	
	@Test
	public void testMessageDoesNotExistForConsumer() throws Exception {
		
		control.replay();
		byte[] messageId1 = Bytes.toBytes("messageId1");
		RowLogMessageImpl message1 = new RowLogMessageImpl(messageId1, Bytes.toBytes("row1"), 1L, null, rowLog);

		int consumerId1 = 1;
		int consumerId2 = 2;
		shard.putMessage(message1, consumerId1);
		assertNull(shard.next(consumerId2));
		shard.removeMessage(message1, consumerId2);
		// Cleanup
		RowLogMessage next = shard.next(consumerId1);
		assertEquals(message1, next);
		shard.removeMessage(message1, consumerId1);
		assertNull(shard.next(consumerId1));
		control.verify();
	}

}
