package org.lilyproject.repository.api;


public class WalProcessingException extends RepositoryException {
    private final RecordId recordId;

    public WalProcessingException(RecordId recordId) {
        this.recordId = recordId;
    }

    public WalProcessingException(RecordId recordId, String message) {
        super(message);
        this.recordId = recordId;
    }

    public WalProcessingException(RecordId recordId, Throwable cause) {
        super(cause);
        this.recordId = recordId;
    }

    public WalProcessingException(RecordId recordId, String message, Throwable cause) {
        super(message, cause);
        this.recordId = recordId;
    }
    
    public RecordId getRecordId() {
        return recordId;
    }
    
    public String getMessage() {
        return "Wal failed to process messages for record '" + recordId + "', " + super.getMessage();
    }
}