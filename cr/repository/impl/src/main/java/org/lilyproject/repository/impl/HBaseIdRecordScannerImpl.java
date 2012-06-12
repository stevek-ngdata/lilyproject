package org.lilyproject.repository.impl;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.lilyproject.repository.api.IdRecord;
import org.lilyproject.repository.api.IdRecordScanner;
import org.lilyproject.repository.api.RepositoryException;

/**
 * @author Jan Van Besien
 */
public class HBaseIdRecordScannerImpl extends AbstractHBaseRecordScanner<IdRecord> implements IdRecordScanner {

    private final RecordDecoder recordDecoder;

    public HBaseIdRecordScannerImpl(ResultScanner hbaseScanner, RecordDecoder recordDecoder) {
        super(hbaseScanner);
        this.recordDecoder = recordDecoder;
    }

    @Override
    IdRecord decode(Result result) throws RepositoryException, InterruptedException {
        return this.recordDecoder.decodeRecordWithIds(result);
    }

}
