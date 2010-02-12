package org.lilycms.repository.api;

public class RecordExistsException extends Exception {
    public RecordExistsException(String recordId) {
        super("Record <" + recordId + "> already exists");
    }
}
