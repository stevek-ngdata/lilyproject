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
package org.lilyproject.repository.bulk.mapreduce;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.bulk.BulkIngester;
import org.lilyproject.repository.bulk.RecordWriter;

/**
 * RecordWriter for use within a MapReduce context where HFiles are being written directly.
 */
public class MapReduceRecordWriter implements RecordWriter {

    private long recordsWritten;
    private ImmutableBytesWritable rowKey = new ImmutableBytesWritable();
    private BulkIngester bulkIngester;
    private Mapper<?,?,ImmutableBytesWritable,Put>.Context context;
    
    public MapReduceRecordWriter(BulkIngester bulkIngester) {
        this.bulkIngester = bulkIngester;
    }

    public void setContext(Mapper<?,?,ImmutableBytesWritable,Put>.Context context) {
        this.context = context;
    }

    @Override
    public void write(Record record) throws IOException, InterruptedException {
        Put put;
        try {
            put = bulkIngester.buildPut(record);
            recordsWritten++;
        } catch (RepositoryException e) {
            throw new RuntimeException(e);
        }
        rowKey.set(record.getId().toBytes());
        context.write(rowKey, put);
    }
    
    @Override
    public void close() {
        // No-op
    }
    
    @Override
    public long getNumRecords() {
        return recordsWritten;
    }
    
}
