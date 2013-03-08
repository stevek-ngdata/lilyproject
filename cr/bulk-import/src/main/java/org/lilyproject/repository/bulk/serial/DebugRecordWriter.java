/*
 * Copyright 2013 NGDATA nv
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
package org.lilyproject.repository.bulk.serial;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;

import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.bulk.RecordWriter;

/**
 * A {@code RecordWriter} that simply prints out all incoming {@code Record}s.
 * <p>
 * This class is only meant to be used for debugging purposes.
 */
public class DebugRecordWriter implements RecordWriter {
    
    private long numRecords;
    private PrintStream output;
    
    public DebugRecordWriter(OutputStream outputStream) {
        output = new PrintStream(outputStream);
    }

    @Override
    public void write(Record record) throws IOException, InterruptedException {
        output.println(record.toString());
        numRecords++;
    }
    
    @Override
    public void close() {
        output.flush();
    }
    
    @Override
    public long getNumRecords() {
        return numRecords;
    }

}
