package org.lilycms.repository.api.exception;

import org.lilycms.repository.api.Blob;

public class BlobNotFoundException extends Exception {
    private final Blob blob;
    private final Exception exception;

    public BlobNotFoundException(Blob blob) {
        this(blob, null);
    }
    
    public BlobNotFoundException(Blob blob, Exception exception) {
        this.blob = blob;
        this.exception = exception;
    }

    @Override
    public String getMessage() {
        return "Blob <" + blob + "> could not be found.";
    }

    public Exception getException() {
        return exception;
    }
}
