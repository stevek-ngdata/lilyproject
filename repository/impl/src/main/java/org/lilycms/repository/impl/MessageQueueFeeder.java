package org.lilycms.repository.impl;

import org.apache.hadoop.hbase.client.RowLock;
import org.lilycms.rowlog.api.RowLog;
import org.lilycms.rowlog.api.RowLogException;
import org.lilycms.rowlog.api.RowLogMessage;
import org.lilycms.rowlog.api.RowLogMessageConsumer;
import org.lilycms.rowlog.impl.RowLogMessageImpl;

public class MessageQueueFeeder implements RowLogMessageConsumer {

	public static final int ID = 1;
	private final RowLog messageQueue;
	public MessageQueueFeeder(RowLog messageQueue) {
		this.messageQueue = messageQueue;
    }
	
	public int getId() {
		return ID;
	}
	
	public boolean processMessage(RowLogMessage message, RowLock rowLock) {
	    try {
	        messageQueue.putMessage(new RowLogMessageImpl(message.getRowKey(), message.getSeqNr(), message.getData(), messageQueue), null, rowLock);
	        return true;
        } catch (RowLogException e) {
	        return false;
        }
    }
}
