/*
 * Copyright 2010 Outerthought bvba
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
package org.lilycms.repository.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilycms.repository.api.Blob;
import org.lilycms.repository.api.BlobStoreAccess;
import org.lilycms.repository.api.BlobStoreAccessFactory;
import org.lilycms.repository.api.FieldType;
import org.lilycms.repository.api.IdGenerator;
import org.lilycms.repository.api.QName;
import org.lilycms.repository.api.Record;
import org.lilycms.repository.api.RecordId;
import org.lilycms.repository.api.RecordType;
import org.lilycms.repository.api.Repository;
import org.lilycms.repository.api.Scope;
import org.lilycms.repository.api.TypeManager;
import org.lilycms.repository.api.ValueType;
import org.lilycms.repository.api.exception.BlobNotFoundException;
import org.lilycms.repository.api.exception.FieldTypeNotFoundException;
import org.lilycms.repository.api.exception.InvalidRecordException;
import org.lilycms.repository.api.exception.RecordExistsException;
import org.lilycms.repository.api.exception.RecordNotFoundException;
import org.lilycms.repository.api.exception.RecordTypeNotFoundException;
import org.lilycms.repository.api.exception.RepositoryException;
import org.lilycms.util.ArgumentValidator;
import org.lilycms.util.Pair;

public class HBaseRepository implements Repository {

    private static final byte[] NON_VERSIONED_SYSTEM_COLUMN_FAMILY = Bytes.toBytes("NVSCF");
    private static final byte[] VERSIONED_SYSTEM_COLUMN_FAMILY = Bytes.toBytes("VSCF");
    private static final byte[] NON_VERSIONED_COLUMN_FAMILY = Bytes.toBytes("NVCF");
    private static final byte[] VERSIONED_COLUMN_FAMILY = Bytes.toBytes("VCF");
    private static final byte[] VERSIONED_MUTABLE_COLUMN_FAMILY = Bytes.toBytes("VMCF");
    private static final byte[] CURRENT_VERSION_COLUMN_NAME = Bytes.toBytes("$CurrentVersion");
    private static final byte[] NON_VERSIONED_RECORDTYPEID_COLUMN_NAME = Bytes.toBytes("$NonVersionableRecordTypeId");
    private static final byte[] NON_VERSIONED_RECORDTYPEVERSION_COLUMN_NAME = Bytes
                    .toBytes("$NonVersionableRecordTypeVersion");
    private static final byte[] VERSIONED_RECORDTYPEID_COLUMN_NAME = Bytes.toBytes("$VersionableRecordTypeId");
    private static final byte[] VERSIONED_RECORDTYPEVERSION_COLUMN_NAME = Bytes
                    .toBytes("$VersionableRecordTypeVersion");
    private static final byte[] VERSIONED_MUTABLE_RECORDTYPEID_COLUMN_NAME = Bytes
                    .toBytes("$VersionableMutableRecordTypeId");
    private static final byte[] VERSIONED_MUTABLE_RECORDTYPEVERSION_COLUMN_NAME = Bytes
                    .toBytes("$VersionableMutableRecordTypeVersion");
    private static final String RECORD_TABLE = "recordTable";
    private HTable recordTable;
    private final TypeManager typeManager;
    private final IdGenerator idGenerator;
    private Map<Scope, byte[]> columnFamilies = new HashMap<Scope, byte[]>();
    private Map<Scope, byte[]> systemColumnFamilies = new HashMap<Scope, byte[]>();
    private Map<Scope, byte[]> recordTypeIdColumnNames = new HashMap<Scope, byte[]>();
    private Map<Scope, byte[]> recordTypeVersionColumnNames = new HashMap<Scope, byte[]>();
    private BlobStoreAccessRegistry blobStoreAccessRegistry;

    public HBaseRepository(TypeManager typeManager, IdGenerator idGenerator, BlobStoreAccessFactory blobStoreOutputStreamFactory, Configuration configuration)
                    throws IOException {
        this.typeManager = typeManager;
        this.idGenerator = idGenerator;
        blobStoreAccessRegistry = new BlobStoreAccessRegistry();
        blobStoreAccessRegistry.setBlobStoreOutputStreamFactory(blobStoreOutputStreamFactory); 
        try {
            recordTable = new HTable(configuration, RECORD_TABLE);
        } catch (IOException e) {
            HBaseAdmin admin = new HBaseAdmin(configuration);
            HTableDescriptor tableDescriptor = new HTableDescriptor(RECORD_TABLE);
            tableDescriptor.addFamily(new HColumnDescriptor(NON_VERSIONED_SYSTEM_COLUMN_FAMILY));
            tableDescriptor.addFamily(new HColumnDescriptor(VERSIONED_SYSTEM_COLUMN_FAMILY, HConstants.ALL_VERSIONS,
                            "none", false, true, HConstants.FOREVER, false));
            tableDescriptor.addFamily(new HColumnDescriptor(NON_VERSIONED_COLUMN_FAMILY));
            tableDescriptor.addFamily(new HColumnDescriptor(VERSIONED_COLUMN_FAMILY, HConstants.ALL_VERSIONS, "none",
                            false, true, HConstants.FOREVER, false));
            tableDescriptor.addFamily(new HColumnDescriptor(VERSIONED_MUTABLE_COLUMN_FAMILY, HConstants.ALL_VERSIONS,
                            "none", false, true, HConstants.FOREVER, false));
            admin.createTable(tableDescriptor);
            recordTable = new HTable(configuration, RECORD_TABLE);
        }
        columnFamilies.put(Scope.NON_VERSIONED, NON_VERSIONED_COLUMN_FAMILY);
        columnFamilies.put(Scope.VERSIONED, VERSIONED_COLUMN_FAMILY);
        columnFamilies.put(Scope.VERSIONED_MUTABLE, VERSIONED_MUTABLE_COLUMN_FAMILY);
        systemColumnFamilies.put(Scope.NON_VERSIONED, NON_VERSIONED_SYSTEM_COLUMN_FAMILY);
        systemColumnFamilies.put(Scope.VERSIONED, VERSIONED_SYSTEM_COLUMN_FAMILY);
        systemColumnFamilies.put(Scope.VERSIONED_MUTABLE, VERSIONED_SYSTEM_COLUMN_FAMILY);
        recordTypeIdColumnNames.put(Scope.NON_VERSIONED, NON_VERSIONED_RECORDTYPEID_COLUMN_NAME);
        recordTypeIdColumnNames.put(Scope.VERSIONED, VERSIONED_RECORDTYPEID_COLUMN_NAME);
        recordTypeIdColumnNames.put(Scope.VERSIONED_MUTABLE, VERSIONED_MUTABLE_RECORDTYPEID_COLUMN_NAME);
        recordTypeVersionColumnNames.put(Scope.NON_VERSIONED, NON_VERSIONED_RECORDTYPEVERSION_COLUMN_NAME);
        recordTypeVersionColumnNames.put(Scope.VERSIONED, VERSIONED_RECORDTYPEVERSION_COLUMN_NAME);
        recordTypeVersionColumnNames.put(Scope.VERSIONED_MUTABLE, VERSIONED_MUTABLE_RECORDTYPEVERSION_COLUMN_NAME);

    }

    public IdGenerator getIdGenerator() {
        return idGenerator;
    }

    public TypeManager getTypeManager() {
        return typeManager;
    }

    public Record newRecord() {
        return new RecordImpl();
    }

    public Record newRecord(RecordId recordId) {
        ArgumentValidator.notNull(recordId, "recordId");
        return new RecordImpl(recordId);
    }

    public Record create(Record record) throws RecordExistsException, RecordNotFoundException, InvalidRecordException,
                    RecordTypeNotFoundException, FieldTypeNotFoundException, RepositoryException {
        checkCreatePreconditions(record);

        Record newRecord = record.clone();

        RecordId recordId = newRecord.getId();
        if (recordId == null) {
            recordId = idGenerator.newRecordId();
            newRecord.setId(recordId);
        } else {
            // Check if the record is a variant, and if its master record exists
            RecordId masterRecordId = recordId.getMaster();
            if (!recordId.equals(masterRecordId)) {
                Get get = new Get(masterRecordId.toBytes());
                Result masterResult;
                try {
                    masterResult = recordTable.get(get);
                } catch (IOException e) {
                    throw new RepositoryException("Exception occured while checking existence of master record <"
                                    + recordId + ">", e);
                }
                if (masterResult.isEmpty()) {
                    throw new RecordNotFoundException(newRecord);
                }
            }
        }

        byte[] rowId = recordId.toBytes();
        try {
            // Creating an existing record is not allowed. Use update instead.
            if (recordTable.exists(new Get(rowId))) {
                throw new RecordExistsException(newRecord);
            }
            Record dummyOriginalRecord = newRecord();
            dummyOriginalRecord.setVersion(Long.valueOf(1));
            Put put = new Put(newRecord.getId().toBytes());
            putRecord(newRecord, dummyOriginalRecord, Long.valueOf(1), put);
            recordTable.put(put);
        } catch (IOException e) {
            throw new RepositoryException("Exception occured while creating record <" + recordId + "> in HBase table",
                            e);
        }
        newRecord.setVersion(Long.valueOf(1));
        return newRecord;
    }

    private void checkCreatePreconditions(Record record) throws InvalidRecordException {
        ArgumentValidator.notNull(record, "record");
        if (record.getRecordTypeId() == null) {
            throw new InvalidRecordException(record, "The recordType cannot be null for a record to be created.");
        }
        if (record.getFields().isEmpty()) {
            throw new InvalidRecordException(record, "Creating an empty record is not allowed");
        }
    }

    public Record update(Record record) throws RecordNotFoundException, InvalidRecordException,
                    RecordTypeNotFoundException, FieldTypeNotFoundException, RepositoryException {
        checkUpdatePreconditions(record);

        Record newRecord = record.clone();
        Record originalRecord = read(newRecord.getId());

        Put put = new Put(newRecord.getId().toBytes());
        if (putRecord(newRecord, originalRecord, originalRecord.getVersion() + 1, put)) {
            try {
                // Apply changes on the repository
                recordTable.put(put);
            } catch (IOException e) {
                throw new RepositoryException("Exception occured while putting updated record <" + record.getId()
                                + "> on HBase table", e);
            }
        }
        return newRecord;
    }

    private void checkUpdatePreconditions(Record record) throws InvalidRecordException, RecordNotFoundException,
                    RepositoryException {
        ArgumentValidator.notNull(record, "record");
        if (record.getRecordTypeId() == null) {
            throw new InvalidRecordException(record, "The recordType cannot be null for a record to be updated.");
        }
        Get get = new Get(record.getId().toBytes());
        try {
            if (!recordTable.exists(get)) {
                throw new RecordNotFoundException(record);
            }
        } catch (IOException e) {
            throw new RepositoryException("Exception occured while retrieving original record <" + record.getId()
                            + "> from HBase table", e);
        }
    }

    private boolean putRecord(Record record, Record originalRecord, Long version, Put put)
                    throws RecordTypeNotFoundException, FieldTypeNotFoundException, RepositoryException {
        String recordTypeId = record.getRecordTypeId();
        Long recordTypeVersion = record.getRecordTypeVersion();

        RecordType recordType = typeManager.getRecordType(recordTypeId, recordTypeVersion);

        Set<Scope> changedScopes = putFields(record, originalRecord, recordType, version, put);
        boolean changed = !changedScopes.isEmpty();
        boolean versionChanged = changedScopes.contains(Scope.VERSIONED)
                        || changedScopes.contains(Scope.VERSIONED_MUTABLE);
        if (!versionChanged) {
            version = originalRecord.getVersion();
        }
        if (changed) {
            recordTypeId = recordType.getId();
            recordTypeVersion = recordType.getVersion();
            put.add(NON_VERSIONED_SYSTEM_COLUMN_FAMILY, NON_VERSIONED_RECORDTYPEID_COLUMN_NAME, Bytes.toBytes(recordTypeId));
            put
                            .add(NON_VERSIONED_SYSTEM_COLUMN_FAMILY, NON_VERSIONED_RECORDTYPEVERSION_COLUMN_NAME, Bytes
                                            .toBytes(recordTypeVersion));
            record.setRecordType(recordTypeId, recordTypeVersion);
            put.add(NON_VERSIONED_SYSTEM_COLUMN_FAMILY, CURRENT_VERSION_COLUMN_NAME, Bytes.toBytes(version));
        }
        record.setVersion(version);
        return changed;
    }

    private Set<Scope> putFields(Record record, Record originalRecord, RecordType recordType, Long version, Put put)
                    throws FieldTypeNotFoundException, RecordTypeNotFoundException, RepositoryException {
        Set<Scope> changedScopes = putUpdateAndDeleteFields(record.getFields(), record.getFieldsToDelete(),
                        originalRecord.getFields(), version, put);
        for (Scope scope : changedScopes) {
            if (Scope.NON_VERSIONED.equals(scope)) {
                put.add(systemColumnFamilies.get(scope), recordTypeIdColumnNames.get(scope), Bytes.toBytes(recordType
                                .getId()));
                put.add(systemColumnFamilies.get(scope), recordTypeVersionColumnNames.get(scope), Bytes
                                .toBytes(recordType.getVersion()));
            } else {
                put.add(systemColumnFamilies.get(scope), recordTypeIdColumnNames.get(scope), version, Bytes
                                .toBytes(recordType.getId()));
                put.add(systemColumnFamilies.get(scope), recordTypeVersionColumnNames.get(scope), version, Bytes
                                .toBytes(recordType.getVersion()));

            }
            record.setRecordType(scope, recordType.getId(), recordType.getVersion());
        }
        return changedScopes;
    }

    private Set<Scope> putUpdateAndDeleteFields(Map<QName, Object> fields, List<QName> fieldsToDelete,
                    Map<QName, Object> originalFields, Long version, Put put) throws FieldTypeNotFoundException,
                    RecordTypeNotFoundException, RepositoryException {
        // Update fields
        Set<Scope> changedScopes = new HashSet<Scope>();
        changedScopes.addAll(putUpdateFields(fields, originalFields, version, put));
        // Delete fields
        changedScopes.addAll(putDeleteFields(fieldsToDelete, originalFields, version, put));
        return changedScopes;
    }

    private Set<Scope> putUpdateFields(Map<QName, Object> fields, Map<QName, Object> originalFields, Long version,
                    Put put) throws FieldTypeNotFoundException, RecordTypeNotFoundException, RepositoryException {
        Set<Scope> changedScopes = new HashSet<Scope>();
        for (Entry<QName, Object> field : fields.entrySet()) {
            QName fieldName = field.getKey();
            Object newValue = field.getValue();
            Object originalValue = originalFields.get(fieldName);
            if (((newValue == null) && (originalValue != null)) || !newValue.equals(originalValue)) {
                FieldType fieldType = typeManager.getFieldTypeByName(fieldName);
                Scope scope = fieldType.getScope();
                byte[] fieldIdAsBytes = Bytes.toBytes(fieldType.getId());
                byte[] encodedFieldValue = encodeFieldValue(fieldType, newValue);

                if (Scope.NON_VERSIONED.equals(scope)) {
                    put.add(columnFamilies.get(scope), fieldIdAsBytes, encodedFieldValue);
                } else {
                    put.add(columnFamilies.get(scope), fieldIdAsBytes, version, encodedFieldValue);
                }
                changedScopes.add(scope);
            }
        }
        return changedScopes;
    }

    private byte[] encodeFieldValue(FieldType fieldType, Object fieldValue) throws FieldTypeNotFoundException,
                    RecordTypeNotFoundException, RepositoryException {
        ValueType valueType = fieldType.getValueType();

        // TODO validate with Class#isAssignableFrom()
        byte[] encodedFieldValue = valueType.toBytes(fieldValue);
        encodedFieldValue = EncodingUtil.prefixValue(encodedFieldValue, EncodingUtil.EXISTS_FLAG);
        return encodedFieldValue;
    }

    private Set<Scope> putDeleteFields(List<QName> fieldsToDelete, Map<QName, Object> originalFields, Long version,
                    Put put) throws FieldTypeNotFoundException, RecordTypeNotFoundException, RepositoryException {
        Set<Scope> changedScopes = new HashSet<Scope>();
        for (QName fieldToDelete : fieldsToDelete) {
            if (originalFields.get(fieldToDelete) != null) {
                FieldType fieldType = typeManager.getFieldTypeByName(fieldToDelete);
                Scope scope = fieldType.getScope();
                byte[] fieldIdAsBytes = Bytes.toBytes(fieldType.getId());
                if (Scope.NON_VERSIONED.equals(scope)) {
                    put.add(columnFamilies.get(scope), fieldIdAsBytes, new byte[] { EncodingUtil.DELETE_FLAG });
                } else {
                    put
                                    .add(columnFamilies.get(scope), fieldIdAsBytes, version,
                                                    new byte[] { EncodingUtil.DELETE_FLAG });
                }
                changedScopes.add(scope);
            }
        }
        return changedScopes;
    }

    public Record updateMutableFields(Record record) throws InvalidRecordException, RecordNotFoundException,
                    RecordTypeNotFoundException, FieldTypeNotFoundException, RepositoryException {
        checkUpdatePreconditions(record);
        Long version = record.getVersion();
        if (version == null) {
            throw new InvalidRecordException(record,
                            "The version of the record cannot be null to update mutable fields");
        }

        Record newRecord = record.clone();
        Record originalRecord = read(record.getId(), version);

        // Update the mutable fields
        Put put = new Put(record.getId().toBytes());
        Map<QName, Object> fieldsToUpdate = filterMutableFields(record.getFields());
        Map<QName, Object> originalFields = filterMutableFields(originalRecord.getFields());
        Set<Scope> changedScopes = putUpdateFields(fieldsToUpdate, originalFields, version, put);

        // Delete mutable fields and copy values to the next version if needed
        Record originalNextRecord = null;
        try {
            originalNextRecord = read(record.getId(), version + 1);
        } catch (RecordNotFoundException exception) {
            // There is no next record
        }
        List<QName> fieldsToDelete = filterMutableFieldsToDelete(record.getFieldsToDelete());
        Map<QName, Object> originalNextFields = new HashMap<QName, Object>();
        if (originalNextRecord != null) {
            originalNextFields.putAll(filterMutableFields(originalNextRecord.getFields()));
        }
        boolean deleted = putDeleteMutableFields(fieldsToDelete, originalFields, originalNextFields, version, put);

        Scope scope = Scope.VERSIONED_MUTABLE;
        if (!changedScopes.isEmpty() || deleted) {
            RecordType recordType = typeManager.getRecordType(record.getRecordTypeId(), record.getRecordTypeVersion());
            // Update the mutable record type
            put.add(systemColumnFamilies.get(scope), recordTypeIdColumnNames.get(scope), version, Bytes
                            .toBytes(recordType.getId()));
            put.add(systemColumnFamilies.get(scope), recordTypeVersionColumnNames.get(scope), version, Bytes
                            .toBytes(recordType.getVersion()));
            try {
                // Apply changes on the repository
                recordTable.put(put);
            } catch (IOException e) {
                throw new RepositoryException("Exception occured while putting updated record <" + record.getId()
                                + "> on HBase table", e);
            }
            newRecord.setRecordType(scope, recordType.getId(), recordType.getVersion());
        }
        return newRecord;
    }

    private Map<QName, Object> filterMutableFields(Map<QName, Object> fields) throws FieldTypeNotFoundException,
                    RecordTypeNotFoundException, RepositoryException {
        Map<QName, Object> mutableFields = new HashMap<QName, Object>();
        for (Entry<QName, Object> field : fields.entrySet()) {
            FieldType fieldType = typeManager.getFieldTypeByName(field.getKey());
            if (Scope.VERSIONED_MUTABLE.equals(fieldType.getScope())) {
                mutableFields.put(field.getKey(), field.getValue());
            }
        }
        return mutableFields;
    }

    private List<QName> filterMutableFieldsToDelete(List<QName> fields) throws FieldTypeNotFoundException,
                    RecordTypeNotFoundException, RepositoryException {
        List<QName> mutableFields = new ArrayList<QName>();
        for (QName field : fields) {
            FieldType fieldType = typeManager.getFieldTypeByName(field);
            if (Scope.VERSIONED_MUTABLE.equals(fieldType.getScope())) {
                mutableFields.add(field);
            }
        }
        return mutableFields;
    }

    private boolean putDeleteMutableFields(List<QName> fieldsToDelete, Map<QName, Object> originalFields,
                    Map<QName, Object> originalNextFields, Long version, Put put) throws FieldTypeNotFoundException,
                    RecordTypeNotFoundException, RepositoryException {
        boolean changed = false;
        for (QName fieldToDelete : fieldsToDelete) {
            Object originalValue = originalFields.get(fieldToDelete);
            if (originalValue != null) {
                FieldType fieldType = typeManager.getFieldTypeByName(fieldToDelete);
                byte[] fieldIdBytes = Bytes.toBytes(fieldType.getId());
                put.add(columnFamilies.get(Scope.VERSIONED_MUTABLE), fieldIdBytes, version,
                                new byte[] { EncodingUtil.DELETE_FLAG });
                if (originalValue.equals(originalNextFields.get(fieldToDelete))) {
                    byte[] encodedValue = encodeFieldValue(fieldType, originalValue);
                    put.add(columnFamilies.get(Scope.VERSIONED_MUTABLE), fieldIdBytes, version + 1, encodedValue);
                }
                changed = true;
            }
        }
        return changed;
    }

    public Record read(RecordId recordId) throws RecordNotFoundException, RecordTypeNotFoundException,
                    FieldTypeNotFoundException, RepositoryException {
        return read(recordId, null, null);
    }

    public Record read(RecordId recordId, List<QName> fieldNames) throws RecordNotFoundException,
                    RecordTypeNotFoundException, FieldTypeNotFoundException, RepositoryException {
        return read(recordId, null, fieldNames);
    }

    public Record read(RecordId recordId, Long version) throws RecordNotFoundException, RecordTypeNotFoundException,
                    FieldTypeNotFoundException, RepositoryException {
        return read(recordId, version, null);
    }

    public Record read(RecordId recordId, Long version, List<QName> fieldNames) throws RecordNotFoundException,
                    RecordTypeNotFoundException, FieldTypeNotFoundException, RepositoryException {
        ArgumentValidator.notNull(recordId, "recordId");
        Record record = newRecord();
        record.setId(recordId);

        Get get = new Get(recordId.toBytes());
        if (version != null) {
            get.setMaxVersions();
        }
        // Add the columns for the fields to get
        addFieldsToGet(get, fieldNames);
        Result result;
        try {
            if (!recordTable.exists(get)) {
                throw new RecordNotFoundException(record);
            }
            // Retrieve the data from the repository
            result = recordTable.get(get);
        } catch (IOException e) {
            throw new RepositoryException("Exception occured while retrieving record <" + recordId
                            + "> from HBase table", e);
        }

        // Set retrieved version on the record
        long currentVersion = Bytes.toLong(result.getValue(NON_VERSIONED_SYSTEM_COLUMN_FAMILY,
                        CURRENT_VERSION_COLUMN_NAME));
        if (version != null) {
            if (currentVersion < version) {
                throw new RecordNotFoundException(record);
            }
            record.setVersion(version);
        } else {
            record.setVersion(currentVersion);
        }

        // Extract the actual fields from the retrieved data
        if (extractFields(result, version, record)) {
            // Set the recordType explicitly in case only versioned fields were extracted
            Pair<String, Long> recordTypePair = extractRecordType(Scope.NON_VERSIONED, result, null, record);
            record.setRecordType(recordTypePair.getV1(), recordTypePair.getV2());
        }
        return record;
    }

    private Pair<String, Long> extractRecordType(Scope scope, Result result, Long version, Record record) {
        if (version == null) {
            // Get latest version
            return new Pair<String, Long>(Bytes.toString(result.getValue(systemColumnFamilies.get(scope),
                            recordTypeIdColumnNames.get(scope))), Bytes.toLong(result.getValue(systemColumnFamilies
                            .get(scope), recordTypeVersionColumnNames.get(scope))));

        } else {
            // Get on version
            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> allVersionsMap = result.getMap();
            NavigableMap<byte[], NavigableMap<Long, byte[]>> versionableSystemCFversions = allVersionsMap
                            .get(systemColumnFamilies.get(scope));
            return extractVersionRecordType(version, versionableSystemCFversions, recordTypeIdColumnNames.get(scope),
                            recordTypeVersionColumnNames.get(scope));
        }
    }

    private Pair<String, Long> extractVersionRecordType(Long version,
                    NavigableMap<byte[], NavigableMap<Long, byte[]>> versionableSystemCFversions,
                    byte[] recordTypeIdColumnName, byte[] recordTypeVersionColumnName) {
        Entry<Long, byte[]> ceilingEntry = versionableSystemCFversions.get(recordTypeIdColumnName)
                        .ceilingEntry(version);
        String recordTypeId = null;
        if (ceilingEntry != null) {
            recordTypeId = Bytes.toString(ceilingEntry.getValue());
        }
        Long recordTypeVersion = null;
        ceilingEntry = versionableSystemCFversions.get(recordTypeVersionColumnName).ceilingEntry(version);
        if (ceilingEntry != null) {
            recordTypeVersion = Bytes.toLong(ceilingEntry.getValue());
        }
        Pair<String, Long> recordType = new Pair<String, Long>(recordTypeId, recordTypeVersion);
        return recordType;
    }

    private List<Pair<QName, Object>> extractFields(NavigableMap<byte[], byte[]> familyMap)
                    throws FieldTypeNotFoundException, RepositoryException {
        List<Pair<QName, Object>> fields = new ArrayList<Pair<QName, Object>>();
        if (familyMap != null) {
            for (Entry<byte[], byte[]> entry : familyMap.entrySet()) {
                Pair<QName, Object> field = extractField(entry.getKey(), entry.getValue());
                if (field != null) {
                    fields.add(field);
                }
            }
        }
        return fields;
    }

    private List<Pair<QName, Object>> extractVersionFields(Long version,
                    NavigableMap<byte[], NavigableMap<Long, byte[]>> mapWithVersions)
                    throws FieldTypeNotFoundException, RepositoryException {
        List<Pair<QName, Object>> fields = new ArrayList<Pair<QName, Object>>();
        if (mapWithVersions != null) {
            for (Entry<byte[], NavigableMap<Long, byte[]>> columnWithAllVersions : mapWithVersions.entrySet()) {
                NavigableMap<Long, byte[]> allValueVersions = columnWithAllVersions.getValue();
                Entry<Long, byte[]> ceilingEntry = allValueVersions.ceilingEntry(version);
                if (ceilingEntry != null) {
                    Pair<QName, Object> field = extractField(columnWithAllVersions.getKey(), ceilingEntry.getValue());
                    if (field != null) {
                        fields.add(field);
                    }
                }
            }
        }
        return fields;
    }

    private Pair<QName, Object> extractField(byte[] key, byte[] prefixedValue) throws FieldTypeNotFoundException,
                    RepositoryException {
        if (EncodingUtil.isDeletedField(prefixedValue)) {
            return null;
        }
        String fieldId = Bytes.toString(key);
        FieldType fieldType = typeManager.getFieldTypeById(fieldId);
        ValueType valueType = fieldType.getValueType();
        Object value = valueType.fromBytes(EncodingUtil.stripPrefix(prefixedValue));
        return new Pair<QName, Object>(fieldType.getName(), value);
    }

    private void addFieldsToGet(Get get, List<QName> fieldNames) throws RecordNotFoundException,
                    FieldTypeNotFoundException, RecordTypeNotFoundException, RepositoryException {
        boolean added = false;
        if (fieldNames != null) {
            for (QName fieldName : fieldNames) {
                FieldType fieldType = typeManager.getFieldTypeByName(fieldName);
                get.addColumn(columnFamilies.get(fieldType.getScope()), Bytes.toBytes(fieldType.getId()));
            }
            added = true;
        }
        if (added) {
            // Add system columns explicitly to get since we're not retrieving
            // all columns
            addSystemColumnsToGet(get);
        }
    }

    private void addSystemColumnsToGet(Get get) {
        get.addColumn(NON_VERSIONED_SYSTEM_COLUMN_FAMILY, CURRENT_VERSION_COLUMN_NAME);
        get.addColumn(NON_VERSIONED_SYSTEM_COLUMN_FAMILY, NON_VERSIONED_RECORDTYPEID_COLUMN_NAME);
        get.addColumn(NON_VERSIONED_SYSTEM_COLUMN_FAMILY, NON_VERSIONED_RECORDTYPEVERSION_COLUMN_NAME);
        get.addColumn(VERSIONED_SYSTEM_COLUMN_FAMILY, VERSIONED_RECORDTYPEID_COLUMN_NAME);
        get.addColumn(VERSIONED_SYSTEM_COLUMN_FAMILY, VERSIONED_RECORDTYPEVERSION_COLUMN_NAME);
        get.addColumn(VERSIONED_SYSTEM_COLUMN_FAMILY, VERSIONED_MUTABLE_RECORDTYPEID_COLUMN_NAME);
        get.addColumn(VERSIONED_SYSTEM_COLUMN_FAMILY, VERSIONED_MUTABLE_RECORDTYPEVERSION_COLUMN_NAME);
    }

    private boolean extractFields(Result result, Long version, Record record) throws RecordTypeNotFoundException,
                    FieldTypeNotFoundException, RepositoryException {
        boolean nvExtracted = extractFields(Scope.NON_VERSIONED, result, null, record);
        boolean vExtracted = extractFields(Scope.VERSIONED, result, version, record);
        boolean vmExtracted = extractFields(Scope.VERSIONED_MUTABLE, result, version, record);
        return nvExtracted || vExtracted || vmExtracted;
    }

    private boolean extractFields(Scope scope, Result result, Long version, Record record)
                    throws RecordTypeNotFoundException, RepositoryException, FieldTypeNotFoundException {
        boolean retrieved = false;
        Pair<String, Long> recordTypePair = extractRecordType(scope, result, version, record);
        String recordTypeId = recordTypePair.getV1();
        Long recordTypeVersion = recordTypePair.getV2();
        // If there is no recordType, there can't be any fields
        if (recordTypeId != null) {
            List<Pair<QName, Object>> fields;
            if (version == null) {
                fields = extractFields(result.getFamilyMap(columnFamilies.get(scope)));
            } else {
                fields = extractVersionFields(version, result.getMap().get(columnFamilies.get(scope)));
            }
            if (!fields.isEmpty()) {
                for (Pair<QName, Object> field : fields) {
                    record.setField(field.getV1(), field.getV2());
                }
                record.setRecordType(scope, recordTypeId, recordTypeVersion);
                retrieved = true;
            }
        }
        return retrieved;
    }

    public void delete(RecordId recordId) throws RepositoryException {
        ArgumentValidator.notNull(recordId, "recordId");
        Delete delete = new Delete(recordId.toBytes());
        try {
            recordTable.delete(delete);
        } catch (IOException e) {
            throw new RepositoryException(
                            "Exception occured while deleting record <" + recordId + "> from HBase table", e);
        }

    }

    public void registerBlobStoreAccess(BlobStoreAccess blobStoreAccess) {
        blobStoreAccessRegistry.register(blobStoreAccess);
    }
    
    public OutputStream getOutputStream(Blob blob) throws RepositoryException {
        return blobStoreAccessRegistry.getOutputStream(blob);
    }
    
    public InputStream getInputStream(Blob blob) throws BlobNotFoundException, RepositoryException {
        return blobStoreAccessRegistry.getInputStream(blob);
    }
    
    public void delete(Blob blob) throws BlobNotFoundException, RepositoryException {
        blobStoreAccessRegistry.delete(blob);
    }
}
