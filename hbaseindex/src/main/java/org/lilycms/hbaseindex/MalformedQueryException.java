package org.lilycms.hbaseindex;

public class MalformedQueryException extends RuntimeException {
    public MalformedQueryException() {
        super();
    }

    public MalformedQueryException(String message) {
        super(message);
    }

    public MalformedQueryException(String message, Throwable cause) {
        super(message, cause);
    }

    public MalformedQueryException(Throwable cause) {
        super(cause);
    }
}
