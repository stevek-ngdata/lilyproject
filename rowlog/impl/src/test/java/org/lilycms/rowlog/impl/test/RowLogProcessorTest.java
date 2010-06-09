package org.lilycms.rowlog.impl.test;

import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilycms.rowlog.api.RowLog;
import org.lilycms.rowlog.api.RowLogMessage;
import org.lilycms.rowlog.api.RowLogMessageConsumer;
import org.lilycms.rowlog.api.RowLogProcessor;
import org.lilycms.rowlog.api.RowLogShard;
import org.lilycms.rowlog.impl.RowLogProcessorImpl;
import org.lilycms.util.Pair;


public class RowLogProcessorTest {
	private IMocksControl control;
	private RowLog rowLog;
	private RowLogShard rowLogShard;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		control = EasyMock.createControl();

		int consumerId = 1;
		RowLogMessageConsumer consumer = control.createMock(RowLogMessageConsumer.class);
		consumer.getId();
		expectLastCall().andReturn(consumerId).anyTimes();

		rowLog = control.createMock(RowLog.class);
		List<RowLogMessageConsumer> consumers = new ArrayList<RowLogMessageConsumer>();
		consumers.add(consumer);
		rowLog.getConsumers();
		expectLastCall().andReturn(consumers).anyTimes();

		rowLogShard = control.createMock(RowLogShard.class);
		RowLogMessage message = control.createMock(RowLogMessage.class);
		byte[] messageId = Bytes.toBytes("aMessageId");
		rowLogShard.next(consumerId);
		expectLastCall().andReturn(
		        new Pair<byte[], RowLogMessage>(messageId, message)).anyTimes();
		
		consumer.processMessage(message, null);
		expectLastCall().andReturn(Boolean.TRUE).anyTimes();
		
		rowLog.messageDone(messageId, message, consumerId, null);
		expectLastCall().anyTimes();
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testProcessor() throws Exception {
		control.replay();
		RowLogProcessor processor = new RowLogProcessorImpl(rowLog, rowLogShard);
		assertFalse(processor.isRunning());
		processor.start();
		assertTrue(processor.isRunning());
		processor.stop();
		assertFalse(processor.isRunning());
		control.verify();
	}
	
	@Test
	public void testProcessorMultipleStartStop() throws Exception {
		control.replay();
		RowLogProcessor processor = new RowLogProcessorImpl(rowLog, rowLogShard);
		assertFalse(processor.isRunning());
		processor.start();
		assertTrue(processor.isRunning());
		processor.stop();
		assertFalse(processor.isRunning());
		processor.start();
		processor.start();
		assertTrue(processor.isRunning());
		processor.stop();
		processor.stop();
		assertFalse(processor.isRunning());
		control.verify();
	}
	
	@Test
	public void testProcessorStopWihtoutStart() throws Exception {
		control.replay();
		RowLogProcessor processor = new RowLogProcessorImpl(rowLog, rowLogShard);
		processor.stop();
		assertFalse(processor.isRunning());
		processor.start();
		assertTrue(processor.isRunning());
		processor.stop();
	}
}
