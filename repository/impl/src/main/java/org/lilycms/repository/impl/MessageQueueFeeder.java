package org.lilycms.repository.impl;

import java.io.IOException;

import org.lilycms.rowlog.api.RowLog;
import org.lilycms.rowlog.api.RowLogException;
import org.lilycms.rowlog.api.RowLogMessage;
import org.lilycms.rowlog.api.RowLogMessageConsumer;

public class MessageQueueFeeder implements RowLogMessageConsumer {

	public static final int ID = 1;
	private final RowLog messageQueue;
	public MessageQueueFeeder(RowLog messageQueue) {
		this.messageQueue = messageQueue;
    }
	
	public int getId() {
		return ID;
	}
	
	public boolean processMessage(RowLogMessage message) {
	    try {
	        messageQueue.putMessage(message.getRowKey(), message.getData(), message.getPayload(), null);
	        return true;
	    } catch (IOException e) {
	    	return false; //TODO exception handling
        } catch (RowLogException e) {
	        return false;
        }
    }
}
