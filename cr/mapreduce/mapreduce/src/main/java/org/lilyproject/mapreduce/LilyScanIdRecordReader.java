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
package org.lilyproject.mapreduce;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.lilyproject.client.LilyClient;
import org.lilyproject.repository.api.IdRecord;
import org.lilyproject.repository.api.IdRecordScanner;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.util.io.Closer;

/**
 * A Hadoop RecordReader based on Lily RecordScanners. Used by {@link LilyScanInputFormat}.
 */
public class LilyScanIdRecordReader extends RecordReader<RecordIdWritable, IdRecordWritable> {
    private LilyClient lilyClient;
    private IdRecordScanner scanner;
    private IdRecord currentRecord;
    
    private RecordIdWritable recordId = new RecordIdWritable();
    private IdRecordWritable record = new IdRecordWritable();
    
    public LilyScanIdRecordReader(LilyClient lilyClient, IdRecordScanner scanner) {
        this.lilyClient = lilyClient;
        this.scanner = scanner;        
    }
    
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        try {
            currentRecord = scanner.next();
        } catch (RepositoryException e) {
            throw new IOException("Error scanning to next record.", e);
        }
        return currentRecord != null;
    }

    @Override
    public RecordIdWritable getCurrentKey() throws IOException, InterruptedException {
        recordId.setRecordId(currentRecord.getId());
        return recordId;
    }

    @Override
    public IdRecordWritable getCurrentValue() throws IOException, InterruptedException {
        record.setRecord(currentRecord);
        return record;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {
        Closer.close(scanner);
        Closer.close(lilyClient);
    }
}
