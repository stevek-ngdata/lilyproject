package org.lilycms.repository.impl;

import org.lilycms.repository.api.SecondaryTask;

public class MessageQueueTask implements SecondaryTask {

	public static final String ID = "MQ";
	
	public String getId() {
		return ID;
	}
	
	public String getInitialState() {
		return "NEW";
	}
	
	public boolean isDone(String state) {
		return "DONE".equals(state);
	}
	
	public String execute() {
		return "DONE";
	}
}
