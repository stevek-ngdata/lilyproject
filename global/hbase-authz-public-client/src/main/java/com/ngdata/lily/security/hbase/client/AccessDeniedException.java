package com.ngdata.lily.security.hbase.client;

import org.apache.hadoop.hbase.DoNotRetryIOException;

// It is important for this class to be part of the client, so that it can get
// deserialized which is important so that the HBase client recognizes the exception
// as a DoNotRetryIOException (and is useful for the client in general as well).
public class AccessDeniedException extends DoNotRetryIOException {
    public AccessDeniedException() {
        super();
    }

    public AccessDeniedException(String message) {
        super(message);
    }

    public AccessDeniedException(String message, Throwable cause) {
        super(message, cause);
    }
}
