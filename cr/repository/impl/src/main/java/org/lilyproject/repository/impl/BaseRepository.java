package org.lilyproject.repository.impl;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.lilyproject.repository.api.*;
import org.lilyproject.util.ArgumentValidator;
import org.lilyproject.util.hbase.LilyHBaseSchema;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public abstract class BaseRepository implements Repository {
    protected final BlobManager blobManager;
    protected final TypeManager typeManager;
    protected final IdGenerator idGenerator;
    protected final RecordDecoder recdec;
    protected final HTableInterface recordTable;

    protected BaseRepository(TypeManager typeManager, BlobManager blobManager, IdGenerator idGenerator,
            HTableInterface recordTable) {
        this.typeManager = typeManager;
        this.blobManager = blobManager;
        this.idGenerator = idGenerator;
        this.recordTable = recordTable;
        this.recdec = new RecordDecoder(typeManager, idGenerator);
    }
    
    @Override
    public TypeManager getTypeManager() {
        return typeManager;
    }

    @Override
    public Record newRecord() {
        return new RecordImpl();
    }

    @Override
    public Record newRecord(RecordId recordId) {
        ArgumentValidator.notNull(recordId, "recordId");
        return new RecordImpl(recordId);
    }

    @Override
    public void registerBlobStoreAccess(BlobStoreAccess blobStoreAccess) {
        blobManager.register(blobStoreAccess);
    }

    @Override
    public OutputStream getOutputStream(Blob blob) throws BlobException {
        return blobManager.getOutputStream(blob);
    }

    @Override
    public InputStream getInputStream(RecordId recordId, Long version, QName fieldName, int... indexes)
            throws RepositoryException, InterruptedException {
        Record record = read(recordId, version, fieldName);
        return getInputStream(record, fieldName, indexes);
    }
    
    @Override
    public InputStream getInputStream(RecordId recordId, QName fieldName)
            throws RepositoryException, InterruptedException {
        return getInputStream(recordId, null, fieldName);
    }
    
    @Override
    public InputStream getInputStream(Record record, QName fieldName, int... indexes)
            throws RepositoryException, InterruptedException {
        FieldType fieldType = typeManager.getFieldTypeByName(fieldName);
        return blobManager.getBlobAccess(record, fieldName, fieldType, indexes).getInputStream();
    }
    
    @Override
    public BlobAccess getBlob(RecordId recordId, Long version, QName fieldName, int... indexes)
            throws RepositoryException, InterruptedException {
        Record record = read(recordId, version, fieldName);
        FieldType fieldType = typeManager.getFieldTypeByName(fieldName);
        return blobManager.getBlobAccess(record, fieldName, fieldType, indexes);
    }

    @Override
    public BlobAccess getBlob(RecordId recordId, Long version, QName fieldName, Integer mvIndex, Integer hIndex)
            throws RepositoryException, InterruptedException {
        return getBlob(recordId, version, fieldName, convertToIndexes(mvIndex, hIndex));
    }

    @Override
    public BlobAccess getBlob(RecordId recordId, QName fieldName) throws RepositoryException, InterruptedException {
        return getBlob(recordId, null, fieldName);
    }

    @Override
    public InputStream getInputStream(RecordId recordId, Long version, QName fieldName, Integer mvIndex, Integer hIndex)
            throws RepositoryException, InterruptedException {
        return getInputStream(recordId, version, fieldName, convertToIndexes(mvIndex, hIndex));
    }

    @Override
    public InputStream getInputStream(Record record, QName fieldName, Integer mvIndex, Integer hIndex)
            throws RepositoryException, InterruptedException {
        return getInputStream(record, fieldName, convertToIndexes(mvIndex, hIndex));
    }

    private int[] convertToIndexes(Integer mvIndex, Integer hIndex) {
        int[] indexes;
        if (mvIndex == null && hIndex == null) {
            indexes = new int[0];
        } else if (mvIndex == null) {
            indexes = new int[] { hIndex };
        } else if (hIndex == null) {
            indexes = new int[] { mvIndex };
        } else {
            indexes = new int[] { mvIndex, hIndex };
        }

        return indexes;
    }

    @Override
    public RecordScanner getScanner(RecordScan scan) throws RepositoryException, InterruptedException {
        Scan hbaseScan = new Scan();

        if (scan.getStartRecordId() != null) {
            hbaseScan.setStartRow(scan.getStartRecordId().toBytes());
        }

        if (scan.getStopRecordId() != null) {
            hbaseScan.setStopRow(scan.getStopRecordId().toBytes());
        }

        hbaseScan.setMaxVersions(1);

        // TODO allow to specify subset of fields
        hbaseScan.addFamily(LilyHBaseSchema.RecordCf.DATA.bytes);

        ResultScanner hbaseScanner;
        try {
            hbaseScanner = recordTable.getScanner(hbaseScan);
        } catch (IOException e) {
            throw new RecordException("Error creating scanner", e);
        }

        HBaseRecordScanner scanner = new HBaseRecordScanner(hbaseScanner, recdec);

        return scanner;
    }
}
