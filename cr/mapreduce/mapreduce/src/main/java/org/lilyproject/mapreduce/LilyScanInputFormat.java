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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.lilyproject.client.LilyClient;
import org.lilyproject.repository.api.RecordScan;
import org.lilyproject.repository.api.RecordScanner;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.util.io.Closer;

/**
 * A MapReduce InputFormat for Lily based on Lily scanners.
 */
public class LilyScanInputFormat extends AbstractLilyScanInputFormat<RecordIdWritable, RecordWritable> implements Configurable {

    @Override
    public RecordReader<RecordIdWritable, RecordWritable> createRecordReader(InputSplit inputSplit,
            TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        LilyClient lilyClient = null;
        try {
            lilyClient = new LilyClient(zkConnectString, 30000);
        } catch (Exception e) {
            throw new IOException("Error setting up LilyClient", e);
        }

        // Build RecordScan
        RecordScan scan = getScan(lilyClient.getRepository());

        // Change the start/stop record IDs on the scan to the current split
        TableSplit split = (TableSplit)inputSplit;
        scan.setRawStartRecordId(split.getStartRow());
        scan.setRawStopRecordId(split.getEndRow());

        RecordScanner scanner = null;
        try {
            scanner = lilyClient.getRepository().getScanner(scan);
        } catch (RepositoryException e) {
            Closer.close(lilyClient);
            throw new IOException("Error setting up RecordScanner", e);
        }

        return new LilyScanRecordReader(lilyClient, scanner);
    }
}
