package org.lilycms.rowlog.impl;

import java.io.IOException;

import org.lilycms.rowlog.api.RowLog;
import org.lilycms.rowlog.api.RowLogException;
import org.lilycms.rowlog.api.RowLogMessage;
import org.lilycms.rowlog.api.RowLogMessageConsumer;
import org.lilycms.rowlog.api.RowLogProcessor;
import org.lilycms.rowlog.api.RowLogShard;
import org.lilycms.util.ArgumentValidator;
import org.lilycms.util.Pair;

public class RowLogProcessorImpl implements RowLogProcessor {
	private final RowLog rowLog;
	private final RowLogShard shard;
	private boolean stopRequested = true;
	private ProcessorThread processorThread;
	
	public RowLogProcessorImpl(RowLog rowLog, RowLogShard shard) {
		ArgumentValidator.notNull(rowLog, "rowLog");
		ArgumentValidator.notNull(shard, "shard");
		this.rowLog = rowLog;
		this.shard = shard;
    }
	
	public synchronized void start() {
			stopRequested = false;
			if (processorThread == null) {
				processorThread = new ProcessorThread();
				processorThread.start();
			}
	}

	public synchronized void stop() {
		stopRequested = true;
		try {
			if (processorThread != null) {
				processorThread.join();
				processorThread = null;
			}
        } catch (InterruptedException e) {
	        // TODO Auto-generated catch block
	        e.printStackTrace();
        }
	}

	public synchronized boolean isRunning() {
		if (processorThread == null) return false;
		return processorThread.isAlive();
	}

	@Override
	protected synchronized void finalize() throws Throwable {
		stop();
	    super.finalize();
	}
	

	private class ProcessorThread extends Thread {
		public void run() {
			while (!stopRequested) {
				for (RowLogMessageConsumer consumer : rowLog.getConsumers()) {
					int consumerId = consumer.getId();
					Pair<byte[], RowLogMessage> messagePair = null;
		                    try {
	                            messagePair = shard.next(consumerId);
	                            if (stopRequested) break; // Stop fast
	                            if (messagePair != null) {
	                            	if (consumer.processMessage(messagePair.getV2(), null)) {
	                            		rowLog.messageDone(messagePair.getV1(), messagePair.getV2(), consumerId, null);
	                            	}
	                            }
                            } catch (IOException e) {
	                            // TODO Auto-generated catch block
	                            e.printStackTrace();
                            } catch (RowLogException e) {
	                            // TODO Auto-generated catch block
	                            e.printStackTrace();
                            }
	            }
			}
		};
	}
}
