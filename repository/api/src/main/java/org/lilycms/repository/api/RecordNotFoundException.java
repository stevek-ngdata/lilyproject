package org.lilycms.repository.api;

public class RecordNotFoundException extends Exception {

    public RecordNotFoundException(String recordId) {
        super("Record <" + recordId + "> does not exist");
    }
}
