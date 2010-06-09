package org.lilycms.rowlog.impl.test;


import static org.easymock.classextension.EasyMock.createControl;
import static org.junit.Assert.*;

import java.util.Arrays;

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
import org.lilycms.util.Pair;

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
		shard = new RowLogShardImpl("TestShard", rowLog, TEST_UTIL.getConfiguration());
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
		RowLogMessageImpl message1 = new RowLogMessageImpl(Bytes.toBytes("row1"), 0L, null, rowLog);
		shard.putMessage(messageId1, consumerId, message1);
		
		Pair<byte[],RowLogMessage> next = shard.next(consumerId);
		assertTrue(Arrays.equals(messageId1, next.getV1()));
		assertEquals(message1, next.getV2());
		
		shard.removeMessage(messageId1, consumerId);
		assertNull(shard.next(consumerId));
		control.verify();
	}
	
	@Test
	public void testMultipleMessages() throws Exception {
		IMocksControl control = createControl();
		
		control.replay();
		int consumerId = 1;
		byte[] messageId1 = Bytes.toBytes("messageId1");
		RowLogMessageImpl message1 = new RowLogMessageImpl(Bytes.toBytes("row1"), 0L, null, rowLog);
		byte[] messageId2 = Bytes.toBytes("messageId2");
		RowLogMessageImpl message2 = new RowLogMessageImpl(Bytes.toBytes("row2"), 0L, null, rowLog);

		shard.putMessage(messageId1, consumerId, message1);
		shard.putMessage(messageId2, consumerId, message2);

		Pair<byte[], RowLogMessage> next = shard.next(consumerId);
		assertTrue(Arrays.equals(messageId1, next.getV1()));
		assertEquals(message1, next.getV2());

		shard.removeMessage(messageId1, consumerId);
		next = shard.next(consumerId);
		assertTrue(Arrays.equals(messageId2, next.getV1()));
		assertEquals(message2, next.getV2());
		
		shard.removeMessage(messageId2, consumerId);
		assertNull(shard.next(consumerId));
		control.verify();
	}
	
	
	@Test
	public void testMultipleConsumers() throws Exception {
		
		control.replay();
		byte[] messageId1 = Bytes.toBytes("messageId1");
		RowLogMessageImpl message1 = new RowLogMessageImpl(Bytes.toBytes("row1"), 1L, null, rowLog);
		byte[] messageId2 = Bytes.toBytes("messageId2");
		RowLogMessageImpl message2 = new RowLogMessageImpl(Bytes.toBytes("row2"), 1L, null, rowLog);
		
		int consumerId1 = 1;
		int consumerId2 = 2;
		shard.putMessage(messageId1, consumerId1, message1);
		shard.putMessage(messageId2, consumerId2, message2);
		Pair<byte[],RowLogMessage> next = shard.next(consumerId1);
		assertTrue(Arrays.equals(messageId1, next.getV1()));
		assertEquals(message1, next.getV2());
		
		next = shard.next(consumerId2);
		assertTrue(Arrays.equals(messageId2, next.getV1()));
		assertEquals(message2, next.getV2());
		shard.removeMessage(messageId1, consumerId1);
		shard.removeMessage(messageId2, consumerId2);
		assertNull(shard.next(consumerId1));
		assertNull(shard.next(consumerId2));
		control.verify();
	}
	
	@Test
	public void testMessageDoesNotExistForConsumer() throws Exception {
		
		control.replay();
		byte[] messageId1 = Bytes.toBytes("messageId1");
		RowLogMessageImpl message1 = new RowLogMessageImpl(Bytes.toBytes("row1"), 1L, null, rowLog);
		byte[] messageId2 = Bytes.toBytes("messageId2");

		int consumerId1 = 1;
		int consumerId2 = 2;
		shard.putMessage(messageId1, consumerId1, message1);
		assertNull(shard.next(consumerId2));
		shard.removeMessage(messageId1, consumerId2);
		shard.removeMessage(messageId2, consumerId1);
		// Cleanup
		Pair<byte[],RowLogMessage> next = shard.next(consumerId1);
		assertTrue(Arrays.equals(messageId1, next.getV1()));
		assertEquals(message1, next.getV2());
		shard.removeMessage(messageId1, consumerId1);
		assertNull(shard.next(consumerId1));
		control.verify();
	}

}
