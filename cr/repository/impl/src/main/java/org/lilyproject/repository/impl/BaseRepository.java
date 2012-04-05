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
package org.lilyproject.repository.impl;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilyproject.repository.api.*;
import org.lilyproject.repository.api.filter.RecordFilter;
import org.lilyproject.repository.spi.HBaseRecordFilterFactory;
import org.lilyproject.util.ArgumentValidator;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ServiceLoader;

import static org.lilyproject.util.hbase.LilyHBaseSchema.*;

public abstract class BaseRepository implements Repository {
    protected final BlobManager blobManager;
    protected final TypeManager typeManager;
    protected final IdGenerator idGenerator;
    protected final RecordDecoder recdec;
    protected final HTableInterface recordTable;
    /**
     * Not all rows in the HBase record table are real records, this filter excludes non-valid
     * record rows.
     */
    protected static final SingleColumnValueFilter REAL_RECORDS_FILTER;
    static {
        // A record is a real row iff the deleted flag exists and is not true.
        // It is possible for the delete flag not to exist on a row: this is
        // in case a lock was taken on a not-yet-existing row and the record
        // creation failed. Therefore, the filterIfMissing is important.
        REAL_RECORDS_FILTER = new SingleColumnValueFilter(RecordCf.DATA.bytes,
                RecordColumn.DELETED.bytes, CompareFilter.CompareOp.NOT_EQUAL, Bytes.toBytes(true));
        REAL_RECORDS_FILTER.setFilterIfMissing(true);
    }

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
        return recdec.newRecord();
    }

    @Override
    public Record newRecord(RecordId recordId) {
        ArgumentValidator.notNull(recordId, "recordId");
        return recdec.newRecord(recordId);
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

        if (scan.getRawStartRecordId() != null) {
            hbaseScan.setStartRow(scan.getRawStartRecordId());
        } else if (scan.getStartRecordId() != null) {
            hbaseScan.setStartRow(scan.getStartRecordId().toBytes());
        }

        if (scan.getRawStopRecordId() != null) {
            hbaseScan.setStopRow(scan.getRawStopRecordId());
        } else if (scan.getStopRecordId() != null) {
            hbaseScan.setStopRow(scan.getStopRecordId().toBytes());
        }

        // Filters
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);

        // filter out deleted records
        filterList.addFilter(REAL_RECORDS_FILTER);

        // add user's filter
        if (scan.getRecordFilter() != null) {
            Filter filter = filterFactory.createHBaseFilter(scan.getRecordFilter(), this, filterFactory);
            filterList.addFilter(filter);
        }

        hbaseScan.setFilter(filterList);

        hbaseScan.setCaching(scan.getCaching());

        hbaseScan.setCacheBlocks(scan.getCacheBlocks());

        ReturnFields returnFields = scan.getReturnFields();
        if (returnFields != null && returnFields.getType() != ReturnFields.Type.ALL) {
            RecordDecoder.addSystemColumnsToScan(hbaseScan);            
            switch (returnFields.getType()) {
                case ENUM:
                    for (QName field : returnFields.getFields()) {
                        FieldTypeImpl fieldType = (FieldTypeImpl)typeManager.getFieldTypeByName(field);
                        hbaseScan.addColumn(RecordCf.DATA.bytes, fieldType.getQualifier());
                    }
                    break;
                case NONE:
                    // nothing to add
                    break;
                default:
                    throw new RuntimeException("Unrecognized ReturnFields type: " + returnFields.getType());
            }
        } else {
            hbaseScan.addFamily(RecordCf.DATA.bytes);
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
