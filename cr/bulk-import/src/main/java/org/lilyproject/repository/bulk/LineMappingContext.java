/*
 * Copyright 2012 NGDATA nv
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lilyproject.repository.bulk;

import java.io.IOException;


import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordId;

/**
 * Context object that is provided to {@link LineMapper}s when doing line mapping. Contains base functionality needed to
 * create and write Lily records.
 */
public class LineMappingContext {

    private BulkIngester bulkIngester;
    private RecordWriter recordWriter;

    /**
     * Instantiate with the bulk ingester used to provide record and id creation functionality.
     * 
     * @param bulkIngester integester to create records and ids
     */
    public LineMappingContext(BulkIngester bulkIngester, RecordWriter recordWriter) {
        this.bulkIngester = bulkIngester;
        this.recordWriter = recordWriter;
    }

    /**
     * Create a new empty record.
     * 
     * @return empty record
     */
    public Record newRecord() {
        return bulkIngester.newRecord();
    }

    /**
     * Create a new UUID-based record id.
     * 
     * @return UUID record id
     */
    public RecordId newRecordId() {
        return bulkIngester.getIdGenerator().newRecordId();
    }

    /**
     * Create a new user-specified record id.
     * 
     * @param userProvidedId user-specified id
     * @return user-specified record id
     */
    public RecordId newRecordId(String userProvidedId) {
        return bulkIngester.getIdGenerator().newRecordId(userProvidedId);
    }

    /**
     * Create a qualified name from a fully-qualified name string.
     * 
     * @param qualifiedName name string
     * @return QName representation of the fully-qualified name
     */
    public QName qn(String qualifiedName) {
        return QName.fromString(qualifiedName);
    }
    
    /**
     * Get the underlying BulkIngester used for importing data.
     * 
     * @return the underlying bulk ingester
     */
    public BulkIngester getBulkIngester() {
        return bulkIngester;
    }

    public void writeRecord(Record record) throws IOException, InterruptedException {
        recordWriter.write(record);
    }

}
