package org.lilycms.repository.api;

import java.io.IOException;
import java.util.Set;

public interface Repository {

    void create(Record record) throws IOException, RecordExistsException;

    void update(Record record) throws RecordNotFoundException, InvalidRecordException, IOException;

    Record read(String recordId) throws IOException;

    Record read(String recordId, Set<String> versionableFieldNames, Set<String> nonVersionableFieldNames)
                    throws IOException; // TODO deduct the versionable aspect
                                        // from the fieldType

    Record read(String recordId, long version) throws IOException;

    void delete(String recordId) throws IOException;
}
