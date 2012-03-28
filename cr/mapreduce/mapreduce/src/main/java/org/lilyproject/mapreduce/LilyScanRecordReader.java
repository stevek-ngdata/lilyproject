package org.lilyproject.mapreduce;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.lilyproject.client.LilyClient;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordScan;
import org.lilyproject.repository.api.RecordScanner;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.util.io.Closer;

import java.io.IOException;

/**
 * A Hadoop RecordReader based on Lily RecordScanners. Used by {@link LilyScanInputFormat}.
 */
public class LilyScanRecordReader extends RecordReader<RecordIdWritable, RecordWritable> {
    private LilyClient lilyClient;
    private RecordScanner scanner;
    private Record currentRecord;
    
    private RecordIdWritable recordId = new RecordIdWritable();
    private RecordWritable record = new RecordWritable();
    
    public LilyScanRecordReader(LilyClient lilyClient, RecordScanner scanner) {
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
    public RecordWritable getCurrentValue() throws IOException, InterruptedException {
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
