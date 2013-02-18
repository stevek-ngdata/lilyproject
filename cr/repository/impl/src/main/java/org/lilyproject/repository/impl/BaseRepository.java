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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ServiceLoader;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilyproject.repository.api.Blob;
import org.lilyproject.repository.api.BlobAccess;
import org.lilyproject.repository.api.BlobException;
import org.lilyproject.repository.api.BlobManager;
import org.lilyproject.repository.api.BlobStoreAccess;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.FieldTypes;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.IdRecord;
import org.lilyproject.repository.api.IdRecordScanner;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordException;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.RecordNotFoundException;
import org.lilyproject.repository.api.RecordScan;
import org.lilyproject.repository.api.RecordScanner;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.RepositoryManager;
import org.lilyproject.repository.api.ReturnFields;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.TypeException;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.api.VersionNotFoundException;
import org.lilyproject.repository.api.filter.RecordFilter;
import org.lilyproject.repository.impl.RepositoryMetrics.Action;
import org.lilyproject.repository.spi.HBaseRecordFilterFactory;
import org.lilyproject.util.ArgumentValidator;
import org.lilyproject.util.Pair;
import org.lilyproject.util.hbase.LilyHBaseSchema.RecordCf;
import org.lilyproject.util.hbase.LilyHBaseSchema.RecordColumn;

public abstract class BaseRepository implements Repository {
    protected final RepositoryManager repositoryManager;
    protected final TypeManager typeManager;
    protected final IdGenerator idGenerator;
    protected final BlobManager blobManager;
    protected final RecordDecoder recdec;
    protected final HTableInterface recordTable;
    protected RepositoryMetrics metrics;
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

    protected BaseRepository(RepositoryManager repositoryManager, BlobManager blobManager,
                             HTableInterface recordTable, RepositoryMetrics metrics) {
        
        this.repositoryManager = repositoryManager;
        this.typeManager = repositoryManager.getTypeManager();
        this.blobManager = blobManager;
        this.idGenerator = repositoryManager.getIdGenerator();
        this.recordTable = recordTable;
        this.recdec = new RecordDecoder(typeManager, idGenerator);
        this.metrics = metrics;
    }

    @Override
    public TypeManager getTypeManager() {
        return typeManager;
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
            indexes = new int[]{hIndex};
        } else if (hIndex == null) {
            indexes = new int[]{mvIndex};
        } else {
            indexes = new int[]{mvIndex, hIndex};
        }

        return indexes;
    }

    @Override
    public RecordScanner getScanner(RecordScan scan) throws RepositoryException, InterruptedException {
        return new HBaseRecordScannerImpl(createHBaseResultScanner(scan), recdec);
    }

    @Override
    public IdRecordScanner getScannerWithIds(RecordScan scan) throws RepositoryException, InterruptedException {
        return new HBaseIdRecordScannerImpl(createHBaseResultScanner(scan), recdec);
    }

    private ResultScanner createHBaseResultScanner(RecordScan scan) throws RepositoryException, InterruptedException {
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
            Filter filter = filterFactory.createHBaseFilter(scan.getRecordFilter(), repositoryManager, filterFactory);
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
                        FieldTypeImpl fieldType = (FieldTypeImpl) typeManager.getFieldTypeByName(field);
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
        return hbaseScanner;
    }

    private static List<HBaseRecordFilterFactory> FILTER_FACTORIES;
    static {
        FILTER_FACTORIES = new ArrayList<HBaseRecordFilterFactory>();
        // Make our own copy of list of filter factories, because it is not thread-safe to iterate over filterLoader
        ServiceLoader<HBaseRecordFilterFactory> filterLoader =
                ServiceLoader.load(HBaseRecordFilterFactory.class, BaseRepository.class.getClassLoader());
        for (HBaseRecordFilterFactory filterFactory : filterLoader) {
            FILTER_FACTORIES.add(filterFactory);
        }
        FILTER_FACTORIES = Collections.unmodifiableList(FILTER_FACTORIES);
    }

    private HBaseRecordFilterFactory filterFactory = new HBaseRecordFilterFactory() {
        @Override
        public Filter createHBaseFilter(RecordFilter filter, RepositoryManager repositoryManager, HBaseRecordFilterFactory factory)
                throws RepositoryException, InterruptedException {
            for (HBaseRecordFilterFactory filterFactory : FILTER_FACTORIES) {
                Filter hbaseFilter = filterFactory.createHBaseFilter(filter, repositoryManager, factory);
                if (hbaseFilter != null)
                    return hbaseFilter;
            }
            throw new RepositoryException("No implementation available for filter type " + filter.getClass().getName());
        }
    };

    /* READING */
    @Override
    public Record read(RecordId recordId, List<QName> fieldNames) throws RepositoryException, InterruptedException {
        return read(recordId, null, fieldNames == null ? null : fieldNames.toArray(new QName[fieldNames.size()]));
    }

    @Override
    public Record read(RecordId recordId, QName... fieldNames) throws RepositoryException, InterruptedException {
        return read(recordId, null, fieldNames);
    }

    @Override
    public List<Record> read(List<RecordId> recordIds, List<QName> fieldNames)
            throws RepositoryException, InterruptedException {
        return read(recordIds, fieldNames == null ? null : fieldNames.toArray(new QName[fieldNames.size()]));
    }

    @Override
    public List<Record> read(List<RecordId> recordIds, QName... fieldNames)
            throws RepositoryException, InterruptedException {
        FieldTypes fieldTypes = typeManager.getFieldTypesSnapshot();
        List<FieldType> fields = getFieldTypesFromNames(fieldTypes, fieldNames);

        return read(recordIds, fields, fieldTypes);
    }

    @Override
    public Record read(RecordId recordId, Long version, List<QName> fieldNames)
            throws RepositoryException, InterruptedException {
        return read(recordId, version, fieldNames == null ? null : fieldNames.toArray(new QName[fieldNames.size()]));
    }

    @Override
    public Record read(RecordId recordId, Long version, QName... fieldNames)
            throws RepositoryException, InterruptedException {
        FieldTypes fieldTypes = typeManager.getFieldTypesSnapshot();
        List<FieldType> fields = getFieldTypesFromNames(fieldTypes, fieldNames);

        return read(recordId, version, fields, fieldTypes);
    }

    @Override
    public IdRecord readWithIds(RecordId recordId, Long version, List<SchemaId> fieldIds)
            throws RepositoryException, InterruptedException {
        FieldTypes fieldTypes = typeManager.getFieldTypesSnapshot();
        List<FieldType> fields = getFieldTypesFromIds(fieldIds, fieldTypes);

        return readWithIds(recordId, version, fields, fieldTypes);
    }

    private IdRecord readWithIds(RecordId recordId, Long requestedVersion, List<FieldType> fields,
                                 FieldTypes fieldTypes) throws RepositoryException, InterruptedException {
        long before = System.currentTimeMillis();
        try {
            ArgumentValidator.notNull(recordId, "recordId");

            Result result = getRow(recordId, requestedVersion, 1, fields);

            Long latestVersion = recdec.getLatestVersion(result);
            if (requestedVersion == null) {
                // Latest version can still be null if there are only non-versioned fields in the record
                requestedVersion = latestVersion;
            } else {
                if (latestVersion == null || latestVersion < requestedVersion) {
                    // The requested version is higher than the highest existing version
                    throw new VersionNotFoundException(recordId, requestedVersion);
                }
            }
            return recdec.decodeRecordWithIds(recordId, requestedVersion, result, fieldTypes);
        } finally {
            if (metrics != null)
                metrics.report(Action.READ, System.currentTimeMillis() - before);
        }
    }

    private List<FieldType> getFieldTypesFromIds(List<SchemaId> fieldIds, FieldTypes fieldTypes)
            throws TypeException, InterruptedException {
        List<FieldType> fields = null;
        if (fieldIds != null) {
            fields = new ArrayList<FieldType>(fieldIds.size());
            for (SchemaId fieldId : fieldIds) {
                fields.add(fieldTypes.getFieldType(fieldId));
            }
        }
        return fields;
    }

    protected List<FieldType> getFieldTypesFromNames(FieldTypes fieldTypes, QName... fieldNames)
            throws TypeException, InterruptedException {
        List<FieldType> fields = null;
        if (fieldNames != null) {
            fields = new ArrayList<FieldType>();
            for (QName fieldName : fieldNames) {
                fields.add(fieldTypes.getFieldType(fieldName));
            }
        }
        return fields;
    }

    protected Record read(RecordId recordId, Long requestedVersion, List<FieldType> fields, FieldTypes fieldTypes)
            throws RepositoryException, InterruptedException {
        return readWithOcc(recordId, requestedVersion, fields, fieldTypes).getV1();
    }

    /**
     * Returns both the record and its occ (optimistic concurrency control) version.
     */
    protected Pair<Record, Long> readWithOcc(RecordId recordId, Long requestedVersion, List<FieldType> fields,
            FieldTypes fieldTypes) throws RepositoryException, InterruptedException {
        long before = System.currentTimeMillis();
        try {
            ArgumentValidator.notNull(recordId, "recordId");

            Result result = getRow(recordId, requestedVersion, 1, fields);

            Long latestVersion = recdec.getLatestVersion(result);
            if (requestedVersion == null) {
                // Latest version can still be null if there are only non-versioned fields in the record
                requestedVersion = latestVersion;
            } else {
                if (latestVersion == null || latestVersion < requestedVersion) {
                    // The requested version is higher than the highest existing version
                    throw new VersionNotFoundException(recordId, requestedVersion);
                }
            }

            Long occ = Bytes.toLong(result.getValue(RecordCf.DATA.bytes, RecordColumn.OCC.bytes));
            return new Pair<Record, Long>(recdec.decodeRecord(recordId, requestedVersion, null, result, fieldTypes), occ);
        } finally {
            if (metrics != null)
                metrics.report(Action.READ, System.currentTimeMillis() - before);
        }
    }

    private List<Record> read(List<RecordId> recordIds, List<FieldType> fields, FieldTypes fieldTypes)
            throws RepositoryException, InterruptedException {
        long before = System.currentTimeMillis();
        try {
            ArgumentValidator.notNull(recordIds, "recordIds");
            List<Record> records = new ArrayList<Record>();
            if (recordIds.isEmpty())
                return records;

            Map<RecordId, Result> results = getRows(recordIds, fields);

            for (Entry<RecordId, Result> entry : results.entrySet()) {
                Long version = recdec.getLatestVersion(entry.getValue());
                records.add(recdec.decodeRecord(entry.getKey(), version, null, entry.getValue(), fieldTypes));
            }
            return records;
        } finally {
            if (metrics != null)
                metrics.report(Action.READ, System.currentTimeMillis() - before);
        }
    }

    // Retrieves the row from the table and check if it exists and has not been flagged as deleted
    protected Result getRow(RecordId recordId, Long version, int numberOfVersions, List<FieldType> fields)
            throws RecordException {
        Result result;
        Get get = new Get(recordId.toBytes());
        get.setFilter(REAL_RECORDS_FILTER);

        try {
            // Add the columns for the fields to get
            addFieldsToGet(get, fields);

            if (version != null)
                get.setTimeRange(0, version + 1); // Only retrieve data within this timerange
            get.setMaxVersions(numberOfVersions);

            // Retrieve the data from the repository
            result = recordTable.get(get);

            if (result == null || result.isEmpty())
                throw new RecordNotFoundException(recordId);

        } catch (IOException e) {
            throw new RecordException("Exception occurred while retrieving record '" + recordId
                    + "' from HBase table", e);
        }
        return result;
    }

    private void addFieldsToGet(Get get, List<FieldType> fields) {
        if (fields != null && (!fields.isEmpty())) {
            for (FieldType field : fields) {
                get.addColumn(RecordCf.DATA.bytes, ((FieldTypeImpl) field).getQualifier());
            }
            RecordDecoder.addSystemColumnsToGet(get);
        } else {
            // Retrieve everything
            get.addFamily(RecordCf.DATA.bytes);
        }
    }

    // Retrieves the row from the table and check if it exists and has not been flagged as deleted
    protected Map<RecordId, Result> getRows(List<RecordId> recordIds, List<FieldType> fields)
            throws RecordException {
        Map<RecordId, Result> results = new HashMap<RecordId, Result>();

        try {
            List<Get> gets = new ArrayList<Get>();
            for (RecordId recordId : recordIds) {
                Get get = new Get(recordId.toBytes());
                // Add the columns for the fields to get
                addFieldsToGet(get, fields);
                get.setMaxVersions(1); // Only retrieve the most recent version of each field
                gets.add(get);
            }

            // Retrieve the data from the repository
            int i = 0;
            for (Result result : recordTable.get(gets)) {
                if (result == null || result.isEmpty()) {
                    i++; // Skip this recordId (instead of throwing a RecordNotFoundException)
                    continue;
                }
                // Check if the record was deleted
                byte[] deleted = recdec.getLatest(result, RecordCf.DATA.bytes, RecordColumn.DELETED.bytes);
                if ((deleted == null) || (Bytes.toBoolean(deleted))) {
                    i++; // Skip this recordId (instead of throwing a RecordNotFoundException)
                    continue;
                }
                results.put(recordIds.get(i++), result);
            }
        } catch (IOException e) {
            throw new RecordException("Exception occurred while retrieving records '" + recordIds
                    + "' from HBase table", e);
        }
        return results;
    }

    @Override
    public List<Record> readVersions(RecordId recordId, Long fromVersion, Long toVersion, List<QName> fieldNames)
            throws RepositoryException, InterruptedException {
        return readVersions(recordId, fromVersion, toVersion,
                fieldNames == null ? null : fieldNames.toArray(new QName[fieldNames.size()]));
    }

    @Override
    public List<Record> readVersions(RecordId recordId, Long fromVersion, Long toVersion, QName... fieldNames)
            throws RepositoryException, InterruptedException {
        ArgumentValidator.notNull(recordId, "recordId");
        ArgumentValidator.notNull(fromVersion, "fromVersion");
        ArgumentValidator.notNull(toVersion, "toVersion");
        if (fromVersion > toVersion) {
            throw new IllegalArgumentException("fromVersion '" + fromVersion +
                    "' must be smaller or equal to toVersion '" + toVersion + "'");
        }

        FieldTypes fieldTypes = typeManager.getFieldTypesSnapshot();
        List<FieldType> fields = getFieldTypesFromNames(fieldTypes, fieldNames);

        int numberOfVersionsToRetrieve = (int) (toVersion - fromVersion + 1);
        Result result = getRow(recordId, toVersion, numberOfVersionsToRetrieve, fields);
        if (fromVersion < 1L)
            fromVersion = 1L; // Put the fromVersion to a sensible value
        Long latestVersion = recdec.getLatestVersion(result);
        if (latestVersion < toVersion)
            toVersion = latestVersion; // Limit the toVersion to the highest possible version
        List<Long> versionsToRead = new ArrayList<Long>();
        for (long version = fromVersion; version <= toVersion; version++) {
            versionsToRead.add(version);
        }
        return recdec.decodeRecords(recordId, versionsToRead, result, fieldTypes);
    }

    @Override
    public List<Record> readVersions(RecordId recordId, List<Long> versions, List<QName> fieldNames)
            throws RepositoryException, InterruptedException {
        return readVersions(recordId, versions,
                fieldNames == null ? null : fieldNames.toArray(new QName[fieldNames.size()]));
    }

    @Override
    public List<Record> readVersions(RecordId recordId, List<Long> versions, QName... fieldNames)
            throws RepositoryException, InterruptedException {
        ArgumentValidator.notNull(recordId, "recordId");
        ArgumentValidator.notNull(versions, "versions");

        if (versions.isEmpty())
            return new ArrayList<Record>();

        Collections.sort(versions);

        FieldTypes fieldTypes = typeManager.getFieldTypesSnapshot();
        List<FieldType> fields = getFieldTypesFromNames(fieldTypes, fieldNames);

        Long lowestRequestedVersion = versions.get(0);
        Long highestRequestedVersion = versions.get(versions.size() - 1);
        int numberOfVersionsToRetrieve = (int) (highestRequestedVersion - lowestRequestedVersion + 1);
        Result result = getRow(recordId, highestRequestedVersion, numberOfVersionsToRetrieve, fields);
        Long latestVersion = recdec.getLatestVersion(result);

        // Drop the versions that are higher than the latestVersion
        List<Long> validVersions = new ArrayList<Long>();
        for (Long version : versions) {
            if (version > latestVersion)
                break;
            validVersions.add(version);
        }
        return recdec.decodeRecords(recordId, validVersions, result, fieldTypes);
    }
    
    @Override
    public Record newRecord() throws RecordException {
        return repositoryManager.getRecordFactory().newRecord();
    }
    
    @Override
    public Record newRecord(RecordId recordId) throws RecordException {
        return repositoryManager.getRecordFactory().newRecord(recordId);
    }

    @Override
    public RepositoryManager getRepositoryManager() {
        return repositoryManager;
    }
    
}
