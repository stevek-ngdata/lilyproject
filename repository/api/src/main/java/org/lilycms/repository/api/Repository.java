package org.lilycms.repository.api;

import java.io.IOException;

public interface Repository {

    void create(Record record) throws RecordExistsException, InvalidRecordException, IOException;

    void update(Record record) throws RecordNotFoundException, InvalidRecordException, IOException;

    Record read(String recordId) throws RecordNotFoundException, IOException;

    Record read(String recordId, String recordTypeName, long recordTypeVersion, String... fieldNames)
                    throws IOException;

    Record read(String recordId, long version) throws IOException;

    void delete(String recordId) throws IOException;
}
