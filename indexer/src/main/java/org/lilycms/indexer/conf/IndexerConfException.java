package org.lilycms.indexer.conf;

/**
 * Thrown when there is an error in the user-provided configuration.
 */
public class IndexerConfException extends Exception {
    public IndexerConfException() {
        super();
    }

    public IndexerConfException(String message) {
        super(message);
    }

    public IndexerConfException(String message, Throwable cause) {
        super(message, cause);
    }

    public IndexerConfException(Throwable cause) {
        super(cause);
    }
}
