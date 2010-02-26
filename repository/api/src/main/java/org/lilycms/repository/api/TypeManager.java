package org.lilycms.repository.api;

import java.io.IOException;


public interface TypeManager {
    void createRecordType(RecordType recordType) throws IOException;
    RecordType getRecordType(String recordTypeId) throws IOException;
    RecordType getRecordType(String recordTypeId, long recordTypeVersion);
    void updateRecordType(RecordType recordType) throws IOException;
}
