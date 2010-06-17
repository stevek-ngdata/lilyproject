package org.lilycms.rowlog.api;

/**
 * A RowLogShard manages the actual RowLogMessages on a HBase table. It needs to be registered to a RowLog.
 * 
 * <p> This API will be changed so that the putMessage can be called once for all related consumers.
 */
public interface RowLogShard {

    /**
     * Puts a RowLogMessage onto the table for a specific consumer.
     * 
     * @param message the {@link RowLogMessage} to be put on the table
     * @param consumerId the id of a {@link RowLogConsumer} for which the message is intended.
     * @throws RowLogException when an unexpected exception occurs
     */
	void putMessage(RowLogMessage message, int consumerId) throws RowLogException;
	
	/**
	 * Removes the RowLogMessage from the table for the indicated consumer.
	 * 
	 * @param message the {@link RowLogMessage} to be removed from the table
	 * @param consumerId the id of the {@link RowLogConsumer} for which the message needs to be removed
	 * @throws RowLogException when an unexpected exception occurs
	 */
	void removeMessage(RowLogMessage message, int consumerId) throws RowLogException;
	
	/**
	 * Retrieves the next message to be processed by the indicated consumer.
	 * 
	 * @param consumerId the id of the {@link RowLogConsumer} for which the next message should be retrieved
	 * @return the next {@link RowLogMessage} to be processed
	 * @throws RowLogException when an unexpected exception occurs
	 */
	RowLogMessage next(int consumerId) throws RowLogException;
}
