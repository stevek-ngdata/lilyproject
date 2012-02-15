package org.lilyproject.repository.impl;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.lilyproject.repository.api.*;
import org.lilyproject.repository.api.filter.RecordFilter;
import org.lilyproject.repository.spi.HBaseRecordFilterFactory;
import org.lilyproject.util.ArgumentValidator;
import org.lilyproject.util.hbase.LilyHBaseSchema;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ServiceLoader;

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

        hbaseScan.setMaxVersions(1);

        if (scan.getStartRecordId() != null) {
            hbaseScan.setStartRow(scan.getStartRecordId().toBytes());
        }

        if (scan.getStopRecordId() != null) {
            hbaseScan.setStopRow(scan.getStopRecordId().toBytes());
        }
        
        if (scan.getRecordFilter() != null) {
            Filter filter = filterFactory.createHBaseFilter(scan.getRecordFilter(), this, filterFactory);
            hbaseScan.setFilter(filter);
        }

        ReturnFields returnFields = scan.getReturnFields();
        if (returnFields != null && returnFields.getType() != ReturnFields.Type.ALL) {
            RecordDecoder.addSystemColumnsToScan(hbaseScan);            
            switch (returnFields.getType()) {
                case ENUM:
                    for (QName field : returnFields.getFields()) {
                        FieldTypeImpl fieldType = (FieldTypeImpl)typeManager.getFieldTypeByName(field);
                        hbaseScan.addColumn(LilyHBaseSchema.RecordCf.DATA.bytes, fieldType.getQualifier());
                    }
                    break;
                case NONE:
                    // nothing to add
                    break;
                default:
                    throw new RuntimeException("Unrecognized ReturnFields type: " + returnFields.getType());
            }
        } else {
            hbaseScan.addFamily(LilyHBaseSchema.RecordCf.DATA.bytes);
        }

        ResultScanner hbaseScanner;
        try {
            hbaseScanner = recordTable.getScanner(hbaseScan);
        } catch (IOException e) {
            throw new RecordException("Error creating scanner", e);
        }

        HBaseRecordScanner scanner = new HBaseRecordScanner(hbaseScanner, recdec);

        return scanner;
    }

    private HBaseRecordFilterFactory filterFactory = new HBaseRecordFilterFactory() {
        private ServiceLoader<HBaseRecordFilterFactory> filterLoader = ServiceLoader.load(HBaseRecordFilterFactory.class);

        @Override
        public Filter createHBaseFilter(RecordFilter filter, Repository repository, HBaseRecordFilterFactory factory)
                throws RepositoryException, InterruptedException {
            for (HBaseRecordFilterFactory filterFactory : filterLoader) {
                Filter hbaseFilter = filterFactory.createHBaseFilter(filter, repository, factory);
                if (hbaseFilter != null)
                    return hbaseFilter;
            }
            throw new RepositoryException("No implementation available for filter type " + filter.getClass().getName());
        }
    };

}
