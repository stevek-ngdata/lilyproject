package org.lilycms.repository.api;

import java.io.IOException;
import java.util.Map;

public interface Repository {
    void create(Record record) throws RecordExistsException, RecordNotFoundException, InvalidRecordException, IOException;

    void update(Record record) throws RecordNotFoundException, InvalidRecordException, IOException;

    Record read(String recordId) throws RecordNotFoundException, IOException;

    Record read(String recordId, long version) throws RecordNotFoundException, IOException;
    
    Record read(String recordId, Map<String, String> variantProperties) throws RecordNotFoundException, IOException;

    Record read(String recordId, long version, Map<String, String> variantProperties) throws RecordNotFoundException, IOException;
    
    Record read(String recordId, String recordTypeName, long recordTypeVersion, String... fieldNames)
                    throws RecordNotFoundException, IOException;

    void delete(String recordId) throws IOException;
}
