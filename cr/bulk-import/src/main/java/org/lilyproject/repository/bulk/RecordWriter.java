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

import org.lilyproject.repository.api.Record;

/**
 * Writes single Lily {@link Record} objects to a sink.
 */
public interface RecordWriter {
    
    /**
     * Write a single record.
     * @param record record to be written
     */
    void write(Record record) throws IOException, InterruptedException;
    
    /**
     * Close the record writer.
     */
    void close();
    

}
