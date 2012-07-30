package org.lilyproject.indexer;

/**
 *
 */
public class IndexerException extends Exception {

    public IndexerException(String message) {
        super(message);
    }

    public IndexerException(Throwable cause) {
        super(cause);
    }

    public IndexerException(String message, Throwable cause) {
        super(message, cause);
    }

}
