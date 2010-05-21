package org.lilycms.wal.api;

public class WalException extends Exception {

	public WalException(String message) {
		super(message);
    }
	
	public WalException(String message, Throwable t) {
		super(message,t);
	}
}
