package org.lilycms.rowlog.api;

/**
 * A RowLogProcessor is responsible for monitoring the {@link RowLog} for incoming messages 
 * and hand them over for processing to the {@link RowLogConsumers} that are registered with the {@link RowLog} 
 *
 * <p> More specifically, a RowLogProcessor is responsible for the messages of one {@link RowLogShard}.
 * So, one RowLogProcessor should be started for each registered {@link RowLogShard}.
 */
public interface RowLogProcessor {
    /**
     * Starts the RowLogProcessor. The execution should start in a separate thread, and the start call should return immediately. 
     */
	void start();
	
	/**
	 * Indicate that the RowLogProcessor should stop executing as soon as possible.
	 */
	void stop();
	
	/**
	 * Check is the RowLogProcessor is executing
	 * @return true if the RowLogProcessor is executing
	 */
	boolean isRunning();
}
