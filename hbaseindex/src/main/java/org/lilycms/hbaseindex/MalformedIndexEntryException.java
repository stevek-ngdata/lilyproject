package org.lilycms.hbaseindex;

public class MalformedIndexEntryException extends RuntimeException {
    public MalformedIndexEntryException() {
        super();
    }

    public MalformedIndexEntryException(String message) {
        super(message);
    }

    public MalformedIndexEntryException(String message, Throwable cause) {
        super(message, cause);
    }

    public MalformedIndexEntryException(Throwable cause) {
        super(cause);
    }
}
