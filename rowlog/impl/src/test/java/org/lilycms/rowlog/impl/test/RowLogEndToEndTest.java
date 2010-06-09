package org.lilycms.rowlog.impl.test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilycms.rowlog.api.RowLog;
import org.lilycms.rowlog.api.RowLogMessage;
import org.lilycms.rowlog.api.RowLogMessageConsumer;
import org.lilycms.rowlog.api.RowLogProcessor;
import org.lilycms.rowlog.api.RowLogShard;
import org.lilycms.rowlog.impl.RowLogImpl;
import org.lilycms.rowlog.impl.RowLogMessageImpl;
import org.lilycms.rowlog.impl.RowLogProcessorImpl;
import org.lilycms.rowlog.impl.RowLogShardImpl;
import org.lilycms.testfw.TestHelper;


public class RowLogEndToEndTest {
	private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
	private static RowLog rowLog;
	private static TestingMessageConsumer consumer;
	private static RowLogShard shard;
	private static RowLogProcessor processor;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging();
        TEST_UTIL.startMiniCluster(1);
        Configuration configuration = TEST_UTIL.getConfiguration();
		HTable rowTable = RowLogTableUtil.getRowTable(configuration);
        rowLog = new RowLogImpl(rowTable, RowLogTableUtil.PAYLOAD_COLUMN_FAMILY, RowLogTableUtil.ROWLOG_COLUMN_FAMILY);
        shard = new RowLogShardImpl("EndToEndShard", rowLog, configuration);
        consumer = new TestingMessageConsumer(0);
        processor = new RowLogProcessorImpl(rowLog, shard);
        rowLog.registerConsumer(consumer);
        rowLog.registerShard(shard);
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
	}
	
	@Test
	public void testSingleMessage() throws Exception {
		RowLogMessage message = new RowLogMessageImpl(Bytes.toBytes("row1"), 1L, null, rowLog);
		consumer.expectMessage(message);
		rowLog.putMessage(message, null, null);
		processor.start();
		if (!consumer.waitUntilMessagesConsumed(10000)) {
			Assert.fail("Messages not consumed within timeout");
		}
		processor.stop();
	}

	@Test
	public void testMultipleMessagesSameRow() throws Exception {
		RowLogMessage message; 
		for (long seqnr = 0L; seqnr < 10; seqnr++) {
			message = new RowLogMessageImpl(Bytes.toBytes("row2"), seqnr, null, rowLog);
			consumer.expectMessage(message);
			rowLog.putMessage(message, null, null);
        }
		processor.start();
		if (!consumer.waitUntilMessagesConsumed(10000)) {
			Assert.fail("Messages not consumed within timeout");
		}
		processor.stop();
	}

	@Test
	public void testMultipleMessagesMultipleRows() throws Exception {
		processor.start();
		RowLogMessage message; 
		consumer.expectMessages(100);
		for (long seqnr = 0L; seqnr < 10; seqnr++) {
			for (int rownr = 0; rownr < 10; rownr++) {
				byte[] data = Bytes.toBytes(rownr);
				data = Bytes.add(data, Bytes.toBytes(seqnr));
				message = new RowLogMessageImpl(Bytes.toBytes("row"+rownr), seqnr, data, rowLog);
				consumer.expectMessage(message);
				rowLog.putMessage(message, null, null);
			}
        }
		if (!consumer.waitUntilMessagesConsumed(10000)) { // TODO avoid flipping tests
			Assert.fail("Messages not consumed within timeout");
		}
		processor.stop();
	}
	
	@Test
	public void testMultipleConsumers() throws Exception {
		processor.start();
		TestingMessageConsumer consumer2 = new TestingMessageConsumer(1);
		rowLog.registerConsumer(consumer2);
		consumer.expectMessages(100);
		consumer2.expectMessages(100);
		RowLogMessage message; 
		for (long seqnr = 0L; seqnr < 10; seqnr++) {
			for (int rownr = 0; rownr < 10; rownr++) {
				byte[] data = Bytes.toBytes(rownr);
				data = Bytes.add(data, Bytes.toBytes(seqnr));
				message = new RowLogMessageImpl(Bytes.toBytes("row"+rownr), seqnr, data, rowLog);
				consumer.expectMessage(message);
				consumer2.expectMessage(message);
				rowLog.putMessage(message, null, null);
			}
        }
		if (!consumer.waitUntilMessagesConsumed(10000)) { // TODO avoid flipping tests
			Assert.fail("Messages not consumed within timeout");
		}
		if (!consumer2.waitUntilMessagesConsumed(10000)) { // TODO avoid flipping tests
			Assert.fail("Messages not consumed within timeout");
		}
		processor.stop();
	}

	
	private static class TestingMessageConsumer implements RowLogMessageConsumer {
		
		private List<RowLogMessage> expectedMessages = Collections.synchronizedList(new ArrayList<RowLogMessage>());
		private int count = 0;
		private int numberOfMessagesToBeExpected = 0;
		private final int id;
		
		public TestingMessageConsumer(int id) {
			this.id = id;
        }
		
		public void expectMessage(RowLogMessage message) {
			expectedMessages.add(message);
		}
		
		public void expectMessages(int i) {
			count = 0;
			this.numberOfMessagesToBeExpected = i;
        }

		public int getId() {
			return id;
		}
	
		public boolean processMessage(RowLogMessage message, RowLock rowLock) {
//			System.out.println("ID="+id+" [Processing message " + count + "]");
			boolean removed;
			if (removed = expectedMessages.remove(message)) {
				count++;
			} else {
				System.out.println("ID="+id+" [Remove failed]");
			}
			return removed;
	    }
	    
	    public boolean waitUntilMessagesConsumed(long timeout) throws Exception {
	    	long waitUntil = System.currentTimeMillis() + timeout;
	    	while ((!expectedMessages.isEmpty() || (count < numberOfMessagesToBeExpected)) && System.currentTimeMillis() < waitUntil) {
//	    		System.out.println("ExpectedMessages size: " + expectedMessages.size() + " messages to be processed: " +  (numberOfMessagesToBeExpected - count) + " isRunning: "+ processor.isRunning() + "]");
	    		Thread.sleep(10);
	    	}
	    	return expectedMessages.isEmpty();
	    }

	}
}
