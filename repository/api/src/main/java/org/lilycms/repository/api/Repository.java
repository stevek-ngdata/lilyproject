package org.lilycms.repository.api;

import java.io.IOException;
import java.util.Set;

public interface Repository {

	void create(Record record) throws IOException, RecordExistsException;
	void update(Record record) throws IOException, NoSuchRecordException;
	Record read(String recordId) throws IOException;
	Record read(String recordId, Set<String> fieldNames) throws IOException;
	void delete(String recordId) throws IOException;
}
