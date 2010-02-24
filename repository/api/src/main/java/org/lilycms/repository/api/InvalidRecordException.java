package org.lilycms.repository.api;

public class InvalidRecordException extends Exception {

    private final String recordId;
    private final String message;

    public InvalidRecordException(String recordId, String message) {
        this.recordId = recordId;
        // TODO Auto-generated constructor stub
        this.message = message;
    }
    
    @Override
    public String getMessage() {
        return "Record <"+recordId+">: " + message;
    }

}
