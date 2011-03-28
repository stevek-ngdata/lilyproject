package org.lilyproject.rowlog.api;

public class RemoteListenerIOException extends RowLogException {

    public RemoteListenerIOException(String message) {
        super(message);
    }
    
    public RemoteListenerIOException(String message, Throwable t) {
        super(message,t);
    }
}
