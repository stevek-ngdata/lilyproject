package org.lilyproject.linkindex;

public class LinkIndexException extends Exception {
    public LinkIndexException() {
        super();
    }

    public LinkIndexException(String message) {
        super(message);
    }

    public LinkIndexException(String message, Throwable cause) {
        super(message, cause);
    }

    public LinkIndexException(Throwable cause) {
        super(cause);
    }
}
