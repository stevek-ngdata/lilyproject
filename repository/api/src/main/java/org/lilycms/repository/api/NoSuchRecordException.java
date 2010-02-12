package org.lilycms.repository.api;

public class NoSuchRecordException extends Exception {

    public NoSuchRecordException(String recordId) {
        super("Record <" + recordId + "> does not exist");
    }
}
