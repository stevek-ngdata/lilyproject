package org.lilyproject.repository.impl;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordScanner;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.util.hbase.LilyHBaseSchema;

import java.io.IOException;
import java.util.Iterator;

import static org.lilyproject.util.hbase.LilyHBaseSchema.*;

public class HBaseRecordScanner implements RecordScanner  {
    private RecordDecoder recdec;
    private ResultScanner hbaseScanner;

    public HBaseRecordScanner(ResultScanner hbaseScanner, RecordDecoder recdec) {
        this.hbaseScanner = hbaseScanner;
        this.recdec = recdec;
    }

    @Override
    public Record next() throws RepositoryException, InterruptedException {
        Record record = null;
        while (record == null) {
            Result result;
            try {
                result = hbaseScanner.next();
            } catch (IOException e) {
                throw new RepositoryException(e);
            }

            if (result == null) {
                // no more results
                return null;
            }

            // Check if the record was deleted
            byte[] deleted = recdec.getLatest(result, RecordCf.DATA.bytes, RecordColumn.DELETED.bytes);
            if ((deleted == null) || (Bytes.toBoolean(deleted))) {
                // skip
            } else {
                record = recdec.decodeRecord(result);
            }
        }

        return record;
    }

    @Override
    public void close() {
        hbaseScanner.close();
    }

    @Override
    public Iterator<Record> iterator() {
        return new Iterator<Record>() {
            private Record next;
            
            @Override
            public boolean hasNext() {
                if (next != null) {
                    return true;
                } else {
                    try {
                        next = HBaseRecordScanner.this.next();
                    } catch (RepositoryException e) {
                        throw new RuntimeException(e);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                    return next != null;
                }
            }

            @Override
            public Record next() {
                if (!hasNext()) {
                    return null;
                }
                
                Record result = next;
                next = null;
                return result;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
