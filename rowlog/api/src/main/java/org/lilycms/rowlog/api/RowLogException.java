package org.lilycms.rowlog.api;

public class RowLogException extends Exception {

	public RowLogException(String message) {
		super(message);
    }
	
	public RowLogException(String message, Throwable t) {
		super(message,t);
	}
}
