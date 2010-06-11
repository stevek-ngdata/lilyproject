package org.lilycms.rowlog.impl.test;

import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isA;
import static org.easymock.classextension.EasyMock.createControl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
		rowLog = new RowLogImpl(rowTable, payloadColumnFamily, rowLogColumnFamily);
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

		RowLogMessageImpl message = new RowLogMessageImpl(Bytes.toBytes("row1"), 0L, null, rowLog);

		RowLogShard shard = control.createMock(RowLogShard.class);
		shard.putMessage(isA(byte[].class), eq(0), eq(message));
		
		control.replay();
		rowLog.registerConsumer(consumer);
		rowLog.registerShard(shard);
		rowLog.putMessage(message, null);
		control.verify();
	}
	
	
	@Test
	public void testNoShardsRegistered() throws Exception {

		RowLogMessageImpl message = new RowLogMessageImpl(Bytes.toBytes("row1"), 0L, null, rowLog);

		control.replay();
		try {
			rowLog.putMessage(message, null);
			fail("Expected a MessageQueueException since no shards are registered");
		} catch (RowLogException expected) {
		}
		
		try {
			rowLog.messageDone(Bytes.toBytes("aMessageId"), message, 1);
			fail("Expected a MessageQueueException since no shards are registered");
		} catch (RowLogException expected) {
		}
		
		control.verify();
	}
	
	@Test
	public void testMessageConsumed() throws Exception {
		RowLogMessageConsumer consumer = control.createMock(RowLogMessageConsumer.class);
		consumer.getId();
		expectLastCall().andReturn(Integer.valueOf(1)).anyTimes();
		
		byte[] messageId = Bytes.toBytes("aMessageId");
		RowLogMessageImpl message = new RowLogMessageImpl(Bytes.toBytes("row1"), 0L, null, rowLog);

		int consumerId = 1;
		RowLogShard shard = control.createMock(RowLogShard.class);
		
		shard.removeMessage(messageId, consumerId);
		
		control.replay();
		rowLog.registerConsumer(consumer);
		rowLog.registerShard(shard);
		rowLog.messageDone(messageId, message, consumerId);
		
		control.verify();
	}
}
