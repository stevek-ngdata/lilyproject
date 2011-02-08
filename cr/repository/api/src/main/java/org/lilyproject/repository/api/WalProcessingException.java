package org.lilyproject.repository.api;

import java.io.IOException;

public class WalProcessingException extends RepositoryException {
    private final Reason reason;
    private final RecordId recordId;

    public enum Reason {
        LOCKED, PROCESSING_FAILURE, OTHER
    }
    public WalProcessingException(Reason reason, RecordId recordId) {
        this.reason = reason;
        this.recordId = recordId;
    }

    public WalProcessingException(Reason reason, RecordId recordId, String message) {
        super(message);
        this.reason = reason;
        this.recordId = recordId;
    }

    public WalProcessingException(Reason reason, RecordId recordId, Throwable cause) {
        super(cause);
        this.reason = reason;
        this.recordId = recordId;
    }

    public WalProcessingException(Reason reason, RecordId recordId, String message, Throwable cause) {
        super(message, cause);
        this.reason = reason;
        this.recordId = recordId;
    }
    

    public Reason getReason() {
        return reason;
    }
    
    public RecordId getRecordId() {
        return recordId;
    }
    
    public String getMessage() {
        return "Wal failed to process messages for record '" + recordId + "', reason="+reason.name() + ", " + super.getMessage();
    }
}