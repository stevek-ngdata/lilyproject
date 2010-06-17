package org.lilycms.rowlog.impl;

import org.lilycms.rowlog.api.RowLog;
import org.lilycms.rowlog.api.RowLogException;
import org.lilycms.rowlog.api.RowLogMessage;
import org.lilycms.rowlog.api.RowLogMessageConsumer;
import org.lilycms.rowlog.api.RowLogProcessor;
import org.lilycms.rowlog.api.RowLogShard;
import org.lilycms.util.ArgumentValidator;

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
                processorThread.interrupt();
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
					RowLogMessage message = null;
		                    try {
	                            message = shard.next(consumerId);
	                            if (stopRequested) break; // Stop fast
	                            if (message != null) {
	                            	byte[] lock = rowLog.lockMessage(message, consumerId);
	                            	if (lock != null) {
	                            		if (consumer.processMessage(message)) {
	                            			rowLog.messageDone(message, consumerId, lock);
	                            		} else {
	                            			rowLog.unlockMessage(message, consumerId, lock);
	                            		}
	                            	}
	                            }
                            } catch (RowLogException e) {
	                            // The message will be retried later
                            }
	            }
				// TODO this is a temporary (but mostly senseless) fix to slow down the
				//      amount of requests on HBase
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// if we are interrupted, we stop working
					return;
				}
			}
		};
	}
}
