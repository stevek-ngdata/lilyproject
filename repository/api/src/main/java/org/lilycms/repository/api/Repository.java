package org.lilycms.repository.api;

import java.io.IOException;
import java.util.Map;

public interface Repository {
    void create(Record record) throws RecordExistsException, RecordNotFoundException, InvalidRecordException, IOException;

    void update(Record record) throws RecordNotFoundException, InvalidRecordException, IOException;

    Record read(String recordId, String... fieldIds) throws RecordNotFoundException, IOException;

    Record read(String recordId, long version, String... fieldIds) throws RecordNotFoundException, IOException;
    
    Record read(String recordId, Map<String, String> variantProperties, String... fieldIds) throws RecordNotFoundException, IOException;

    Record read(String recordId, long version, Map<String, String> variantProperties, String... fieldIds) throws RecordNotFoundException, IOException;
    
    void delete(String recordId) throws IOException;
}
