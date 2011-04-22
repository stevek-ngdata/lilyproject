package org.lilyproject.rowlog.api;

/**
 * Exception thrown when an IOException occurs in the (Netty) communication channel with the remote listeners.
 */
public class RemoteListenerIOException extends RowLogException {

    public RemoteListenerIOException(String message) {
        super(message);
    }
    
    public RemoteListenerIOException(String message, Throwable t) {
        super(message,t);
    }
}
