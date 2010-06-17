package org.lilycms.rowlog.impl.test;

import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isA;
import static org.easymock.classextension.EasyMock.createControl;
import static org.junit.Assert.*;

import java.util.List;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.easymock.classextension.IMocksControl;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilycms.rowlog.api.RowLog;
import org.lilycms.rowlog.api.RowLogException;
import org.lilycms.rowlog.api.RowLogMessage;
import org.lilycms.rowlog.api.RowLogMessageConsumer;
import org.lilycms.rowlog.api.RowLogShard;
import org.lilycms.rowlog.impl.RowLogImpl;
import org.lilycms.rowlog.impl.RowLogMessageImpl;
import org.lilycms.testfw.TestHelper;

public class RowLogTest {
	private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
	private static IMocksControl control;
	private static RowLog rowLog;
	private static byte[] payloadColumnFamily = RowLogTableUtil.PAYLOAD_COLUMN_FAMILY;
	private static byte[] rowLogColumnFamily = RowLogTableUtil.ROWLOG_COLUMN_FAMILY;
	private static HTable rowTable;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		TestHelper.setupLogging();
		TEST_UTIL.startMiniCluster(1);
		control = createControl();
		rowTable = RowLogTableUtil.getRowTable(TEST_UTIL.getConfiguration());
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		TEST_UTIL.shutdownMiniCluster();
	}

	@Before
	public void setUp() throws Exception {
		rowLog = new RowLogImpl(rowTable, payloadColumnFamily, rowLogColumnFamily, 60000L);
	}

	@After
	public void tearDown() throws Exception {
		control.reset();
	}
	
	@Test
	public void testRegisterConsumer() throws Exception {
		RowLogMessageConsumer consumer = control.createMock(RowLogMessageConsumer.class);

		control.replay();
		rowLog.registerConsumer(consumer);
		List<RowLogMessageConsumer> consumers = rowLog.getConsumers();
		assertTrue(consumers.size() == 1);
		assertEquals(consumer, consumers.iterator().next());
		control.verify();
	}
	
	@Test
	public void testPutMessage() throws Exception {
		RowLogMessageConsumer consumer = control.createMock(RowLogMessageConsumer.class);
		consumer.getId();
		expectLastCall().andReturn(0).anyTimes();

		RowLogShard shard = control.createMock(RowLogShard.class);
		shard.putMessage(isA(RowLogMessage.class), eq(0));
		
		control.replay();
		rowLog.registerConsumer(consumer);
		rowLog.registerShard(shard);
		rowLog.putMessage(Bytes.toBytes("row1"), null, null, null);
		control.verify();
	}
	
	
	@Test
	public void testNoShardsRegistered() throws Exception {

		control.replay();
		try {
			rowLog.putMessage(Bytes.toBytes("row1"), null, null, null);
			fail("Expected a MessageQueueException since no shards are registered");
		} catch (RowLogException expected) {
		}
		
		RowLogMessage message = new RowLogMessageImpl(Bytes.toBytes("id"), Bytes.toBytes("row1"), 0L, null, rowLog);
		try {
			rowLog.messageDone(message , 1, null);
			fail("Expected a MessageQueueException since no shards are registered");
		} catch (RowLogException expected) {
		}
		// Cleanup
		
		control.verify();
	}
	
	@Test
	public void testMessageConsumed() throws Exception {
		RowLogMessageConsumer consumer = control.createMock(RowLogMessageConsumer.class);
		consumer.getId();
		expectLastCall().andReturn(Integer.valueOf(1)).anyTimes();
		

		int consumerId = 1;
		RowLogShard shard = control.createMock(RowLogShard.class);
		shard.putMessage(isA(RowLogMessage.class), eq(1));
		
		shard.removeMessage(isA(RowLogMessage.class), eq(consumerId));
		
		control.replay();
		rowLog.registerConsumer(consumer);
		rowLog.registerShard(shard);
		RowLogMessage message = rowLog.putMessage(Bytes.toBytes("row1"), null, null, null);

		byte[] lock = rowLog.lockMessage(message, consumerId);
		rowLog.messageDone(message, consumerId, lock);
		assertFalse(rowLog.isMessageLocked(message, consumerId));
		control.verify();
	}
	
	@Test
	public void testLockMessage() throws Exception {
		RowLogMessageConsumer consumer = control.createMock(RowLogMessageConsumer.class);
		consumer.getId();
		expectLastCall().andReturn(Integer.valueOf(1)).anyTimes();
		RowLogShard shard = control.createMock(RowLogShard.class);
		shard.putMessage(isA(RowLogMessage.class), eq(1));
		
		int consumerId = 1;
		
		control.replay();
		rowLog.registerConsumer(consumer);
		rowLog.registerShard(shard);
		RowLogMessage message = rowLog.putMessage(Bytes.toBytes("row1"), null, null, null);
		
		assertNotNull(rowLog.lockMessage(message, consumerId));
		assertTrue(rowLog.isMessageLocked(message, consumerId));
		assertNull(rowLog.lockMessage(message, consumerId));
		control.verify();
	}
	
	@Test
	public void testUnlockMessage() throws Exception {
		RowLogMessageConsumer consumer = control.createMock(RowLogMessageConsumer.class);
		consumer.getId();
		expectLastCall().andReturn(Integer.valueOf(1)).anyTimes();
		RowLogShard shard = control.createMock(RowLogShard.class);
		shard.putMessage(isA(RowLogMessage.class), eq(1));
		
		int consumerId = 1;
		
		control.replay();
		rowLog.registerConsumer(consumer);
		rowLog.registerShard(shard);
		RowLogMessage message = rowLog.putMessage(Bytes.toBytes("row2"), null, null, null);
		
		byte[] lock = rowLog.lockMessage(message, consumerId);
		assertNotNull(lock);
		assertTrue(rowLog.unlockMessage(message, consumerId, lock));
		assertFalse(rowLog.isMessageLocked(message, consumerId));
		byte[] lock2 = rowLog.lockMessage(message, consumerId);
		assertNotNull(lock2);
		control.verify();
		//Cleanup 
		rowLog.unlockMessage(message, consumerId, lock2);
	}
	
	@Test
	public void testLockTimeout() throws Exception {
		rowLog = new RowLogImpl(rowTable, payloadColumnFamily, rowLogColumnFamily, 1L);
		
		RowLogMessageConsumer consumer = control.createMock(RowLogMessageConsumer.class);
		consumer.getId();
		expectLastCall().andReturn(Integer.valueOf(1)).anyTimes();
		RowLogShard shard = control.createMock(RowLogShard.class);
		shard.putMessage(isA(RowLogMessage.class), eq(1));
		
		int consumerId = 1;
		
		control.replay();
		rowLog.registerConsumer(consumer);
		rowLog.registerShard(shard);
		RowLogMessage message = rowLog.putMessage(Bytes.toBytes("row2"), null, null, null);
		
		byte[] lock = rowLog.lockMessage(message, consumerId);
		assertNotNull(lock);
		Thread.sleep(10L);
		assertFalse(rowLog.isMessageLocked(message, consumerId));
		byte[] lock2 = rowLog.lockMessage(message, consumerId);
		assertNotNull(lock2);
		
		assertFalse(rowLog.unlockMessage(message, consumerId, lock));
		control.verify();
		//Cleanup
		rowLog.unlockMessage(message, consumerId, lock2);
	}
	
	@Test
	public void testLockingMultipleConsumers() throws Exception {
		RowLogMessageConsumer consumer1 = control.createMock(RowLogMessageConsumer.class);
		consumer1.getId();
		expectLastCall().andReturn(Integer.valueOf(1)).anyTimes();
		RowLogMessageConsumer consumer2 = control.createMock(RowLogMessageConsumer.class);
		consumer2.getId();
		expectLastCall().andReturn(Integer.valueOf(2)).anyTimes();

		RowLogShard shard = control.createMock(RowLogShard.class);
		shard.putMessage(isA(RowLogMessage.class), eq(1));
		shard.putMessage(isA(RowLogMessage.class), eq(2));
		shard.removeMessage(isA(RowLogMessage.class), eq(2));
		
		
		control.replay();
		rowLog.registerConsumer(consumer1);
		rowLog.registerConsumer(consumer2);
		rowLog.registerShard(shard);
		RowLogMessage message = rowLog.putMessage(Bytes.toBytes("row2"), null, null, null);
		
		byte[] lock = rowLog.lockMessage(message, 1);
		assertNotNull(lock);
		assertFalse(rowLog.isMessageLocked(message, 2));
		assertTrue(rowLog.unlockMessage(message, 1, lock));
		assertFalse(rowLog.isMessageLocked(message, 1));
		
		byte[] lock2 = rowLog.lockMessage(message, 2);
		assertNotNull(lock2);
		rowLog.messageDone(message, 2, lock2);
		assertFalse(rowLog.isMessageLocked(message, 2));
		
		control.verify();
		//Cleanup 
		rowLog.unlockMessage(message, 1, lock2);
	}
}
