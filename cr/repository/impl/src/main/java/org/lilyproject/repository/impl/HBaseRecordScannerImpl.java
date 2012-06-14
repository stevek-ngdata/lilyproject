package org.lilyproject.repository.impl;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordScanner;
import org.lilyproject.repository.api.RepositoryException;

/**
 * @author Jan Van Besien
 */
public class HBaseRecordScannerImpl extends AbstractHBaseRecordScanner<Record> implements RecordScanner {

    private final RecordDecoder recordDecoder;

    public HBaseRecordScannerImpl(ResultScanner hbaseScanner, RecordDecoder recordDecoder) {
        super(hbaseScanner);
        this.recordDecoder = recordDecoder;
    }

    @Override
    Record decode(Result result) throws RepositoryException, InterruptedException {
        return this.recordDecoder.decodeRecord(result);
    }

}
