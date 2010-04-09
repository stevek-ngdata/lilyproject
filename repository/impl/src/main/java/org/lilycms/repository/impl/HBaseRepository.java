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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
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
import org.lilycms.repository.api.FieldDescriptor;
import org.lilycms.repository.api.FieldDescriptorNotFoundException;
import org.lilycms.repository.api.FieldGroupNotFoundException;
import org.lilycms.repository.api.IdGenerator;
import org.lilycms.repository.api.InvalidRecordException;
import org.lilycms.repository.api.Record;
import org.lilycms.repository.api.RecordExistsException;
import org.lilycms.repository.api.RecordId;
import org.lilycms.repository.api.RecordNotFoundException;
import org.lilycms.repository.api.RecordType;
import org.lilycms.repository.api.RecordTypeNotFoundException;
import org.lilycms.repository.api.Repository;
import org.lilycms.repository.api.RepositoryException;
import org.lilycms.repository.api.TypeManager;
import org.lilycms.repository.api.ValueType;
import org.lilycms.repository.api.Record.Scope;
import org.lilycms.util.ArgumentValidator;
import org.lilycms.util.Pair;

public class HBaseRepository implements Repository {

    private static final byte[] NON_VERSIONABLE_SYSTEM_COLUMN_FAMILY = Bytes.toBytes("NonVersionableSystemCF");
    private static final byte[] VERSIONABLE_SYSTEM_COLUMN_FAMILY = Bytes.toBytes("VersionableSystemCF");
    private static final byte[] NON_VERSIONABLE_COLUMN_FAMILY = Bytes.toBytes("NonVersionableCF");
    private static final byte[] VERSIONABLE_COLUMN_FAMILY = Bytes.toBytes("VersionableCF");
    private static final byte[] VERSIONABLE_MUTABLE_COLUMN_FAMILY = Bytes.toBytes("VersionableMutableCF");
    private static final byte[] CURRENT_VERSION_COLUMN_NAME = Bytes.toBytes("$CurrentVersion");
    private static final byte[] RECORDTYPEID_COLUMN_NAME = Bytes.toBytes("$RecordTypeId");
    private static final byte[] RECORDTYPEVERSION_COLUMN_NAME = Bytes.toBytes("$RecordTypeVersion");
    private static final byte[] NON_VERSIONABLE_RECORDTYPEID_COLUMN_NAME = Bytes.toBytes("$NonVersionableRecordTypeId");
    private static final byte[] NON_VERSIONABLE_RECORDTYPEVERSION_COLUMN_NAME = Bytes
                    .toBytes("$NonVersionableRecordTypeVersion");
    private static final byte[] VERSIONABLE_RECORDTYPEID_COLUMN_NAME = Bytes.toBytes("$VersionableRecordTypeId");
    private static final byte[] VERSIONABLE_RECORDTYPEVERSION_COLUMN_NAME = Bytes
                    .toBytes("$VersionableRecordTypeVersion");
    private static final byte[] VERSIONABLE_MUTABLE_RECORDTYPEID_COLUMN_NAME = Bytes
                    .toBytes("$VersionableMutableRecordTypeId");
    private static final byte[] VERSIONABLE_MUTABLE_RECORDTYPEVERSION_COLUMN_NAME = Bytes
                    .toBytes("$VersionableMutableRecordTypeVersion");
    private static final String RECORD_TABLE = "recordTable";
    private HTable recordTable;
    private final TypeManager typeManager;
    private final IdGenerator idGenerator;
    private Map<Scope, byte[]> columnFamilies = new HashMap<Scope, byte[]>();
    private Map<Scope, byte[]> systemColumnFamilies = new HashMap<Scope, byte[]>();
    private Map<Scope, byte[]> recordTypeIdColumnNames = new HashMap<Scope, byte[]>();
    private Map<Scope, byte[]> recordTypeVersionColumnNames = new HashMap<Scope, byte[]>();

    public HBaseRepository(TypeManager typeManager, IdGenerator idGenerator, Configuration configuration)
                    throws IOException {
        this.typeManager = typeManager;
        this.idGenerator = idGenerator;
        try {
            recordTable = new HTable(configuration, RECORD_TABLE);
        } catch (IOException e) {
            HBaseAdmin admin = new HBaseAdmin(configuration);
            HTableDescriptor tableDescriptor = new HTableDescriptor(RECORD_TABLE);
            tableDescriptor.addFamily(new HColumnDescriptor(NON_VERSIONABLE_SYSTEM_COLUMN_FAMILY));
            tableDescriptor.addFamily(new HColumnDescriptor(VERSIONABLE_SYSTEM_COLUMN_FAMILY, HConstants.ALL_VERSIONS,
                            "none", false, true, HConstants.FOREVER, false));
            tableDescriptor.addFamily(new HColumnDescriptor(NON_VERSIONABLE_COLUMN_FAMILY));
            tableDescriptor.addFamily(new HColumnDescriptor(VERSIONABLE_COLUMN_FAMILY, HConstants.ALL_VERSIONS, "none",
                            false, true, HConstants.FOREVER, false));
            tableDescriptor.addFamily(new HColumnDescriptor(VERSIONABLE_MUTABLE_COLUMN_FAMILY, HConstants.ALL_VERSIONS,
                            "none", false, true, HConstants.FOREVER, false));
            admin.createTable(tableDescriptor);
            recordTable = new HTable(configuration, RECORD_TABLE);
        }
        columnFamilies.put(Scope.NON_VERSIONABLE, NON_VERSIONABLE_COLUMN_FAMILY);
        columnFamilies.put(Scope.VERSIONABLE, VERSIONABLE_COLUMN_FAMILY);
        columnFamilies.put(Scope.VERSIONABLE_MUTABLE, VERSIONABLE_MUTABLE_COLUMN_FAMILY);
        systemColumnFamilies.put(Scope.NON_VERSIONABLE, NON_VERSIONABLE_SYSTEM_COLUMN_FAMILY);
        systemColumnFamilies.put(Scope.VERSIONABLE, VERSIONABLE_SYSTEM_COLUMN_FAMILY);
        systemColumnFamilies.put(Scope.VERSIONABLE_MUTABLE, VERSIONABLE_SYSTEM_COLUMN_FAMILY);
        recordTypeIdColumnNames.put(Scope.NON_VERSIONABLE, NON_VERSIONABLE_RECORDTYPEID_COLUMN_NAME);
        recordTypeIdColumnNames.put(Scope.VERSIONABLE, VERSIONABLE_RECORDTYPEID_COLUMN_NAME);
        recordTypeIdColumnNames.put(Scope.VERSIONABLE_MUTABLE, VERSIONABLE_MUTABLE_RECORDTYPEID_COLUMN_NAME);
        recordTypeVersionColumnNames.put(Scope.NON_VERSIONABLE, NON_VERSIONABLE_RECORDTYPEVERSION_COLUMN_NAME);
        recordTypeVersionColumnNames.put(Scope.VERSIONABLE, VERSIONABLE_RECORDTYPEVERSION_COLUMN_NAME);
        recordTypeVersionColumnNames.put(Scope.VERSIONABLE_MUTABLE, VERSIONABLE_MUTABLE_RECORDTYPEVERSION_COLUMN_NAME);
        
    }

    public IdGenerator getIdGenerator() {
        return idGenerator;
    }

    public Record newRecord() {
        return new RecordImpl();
    }

    public Record newRecord(RecordId recordId) {
        ArgumentValidator.notNull(recordId, "recordId");
        return new RecordImpl(recordId);
    }

    public Record create(Record record) throws RecordExistsException, RecordNotFoundException, InvalidRecordException,
                    RecordTypeNotFoundException, FieldGroupNotFoundException, FieldDescriptorNotFoundException,
                    RepositoryException {
        ArgumentValidator.notNull(record, "record");
        if (record.getRecordTypeId() == null) {
            throw new InvalidRecordException(record, "The recordType cannot be null for a record to be created.");
        }
        if (record.getFields(Scope.NON_VERSIONABLE).isEmpty() && record.getFields(Scope.VERSIONABLE).isEmpty()
                        && record.getFields(Scope.VERSIONABLE_MUTABLE).isEmpty()) {
            throw new InvalidRecordException(record, "Creating an empty record is not allowed");
        }

        Record newRecord = record.clone();

        RecordId recordId = newRecord.getId();
        if (recordId == null) {
            recordId = idGenerator.newRecordId();
            newRecord.setId(recordId);
        } else {
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
            if (recordTable.exists(new Get(rowId))) {
                throw new RecordExistsException(newRecord);
            }
            Record dummyOriginalRecord = newRecord();
            dummyOriginalRecord.setVersion(Long.valueOf(1));
            Put put = new Put(record.getId().toBytes());
            putRecord(newRecord, dummyOriginalRecord, Long.valueOf(1), put);
            recordTable.put(put);
        } catch (IOException e) {
            throw new RepositoryException("Exception occured while creating record <" + recordId + "> in HBase table",
                            e);
        }
        newRecord.setVersion(Long.valueOf(1));
        return newRecord;
    }

    public Record update(Record record) throws RecordNotFoundException, InvalidRecordException,
                    RecordTypeNotFoundException, FieldGroupNotFoundException, FieldDescriptorNotFoundException,
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
        Record newRecord = record.clone();
        Record originalRecord = read(newRecord.getId());

        Put put = new Put(newRecord.getId().toBytes());
        if (putRecord(newRecord, originalRecord, originalRecord.getVersion() + 1, put)) {
            try {
                recordTable.put(put);
            } catch (IOException e) {
                throw new RepositoryException("Exception occured while putting updated record <" + record.getId()
                                + "> on HBase table", e);
            }
        }
        return newRecord;
    }

    private boolean putRecord(Record record, Record originalRecord, Long version, Put put)
                    throws RecordTypeNotFoundException, FieldGroupNotFoundException, FieldDescriptorNotFoundException,
                    RepositoryException {
        String recordTypeId = record.getRecordTypeId();
        Long recordTypeVersion = record.getRecordTypeVersion();

        RecordType recordType = typeManager.getRecordType(recordTypeId, recordTypeVersion);

        boolean changed = false;
        boolean versionableChanged = false;
        if (putFields(Scope.NON_VERSIONABLE, record, originalRecord, recordType, null, put)) {
            changed = true;
        }
        if (putFields(Scope.VERSIONABLE, record, originalRecord, recordType, version, put)) {
            changed = true;
            versionableChanged = true;
        }
        if (putFields(Scope.VERSIONABLE_MUTABLE, record, originalRecord, recordType, version, put)) {
            changed = true;
            versionableChanged = true;
        }
        if (!versionableChanged) {
            version = originalRecord.getVersion();
        }
        if (changed) {
            recordTypeId = recordType.getId();
            recordTypeVersion = recordType.getVersion();
            put.add(NON_VERSIONABLE_SYSTEM_COLUMN_FAMILY, RECORDTYPEID_COLUMN_NAME, Bytes.toBytes(recordTypeId));
            put.add(NON_VERSIONABLE_SYSTEM_COLUMN_FAMILY, RECORDTYPEVERSION_COLUMN_NAME, Bytes
                            .toBytes(recordTypeVersion));
            record.setRecordType(recordTypeId, recordTypeVersion);
            put.add(NON_VERSIONABLE_SYSTEM_COLUMN_FAMILY, CURRENT_VERSION_COLUMN_NAME, Bytes.toBytes(version));
        }
        record.setVersion(version);
        return changed;
    }

    private boolean putFields(Scope scope, Record record, Record originalRecord, RecordType recordType, Long version, Put put)
                    throws FieldGroupNotFoundException, FieldDescriptorNotFoundException, RecordTypeNotFoundException, RepositoryException {
        if (putUpdateAndDeleteFieldGroupFields(record.getFields(scope), record.getFieldsToDelete(scope), originalRecord
                        .getFields(scope), scope, recordType, 
                        columnFamilies.get(scope), version, put)) {
            if (version == null) {
                put.add(systemColumnFamilies.get(scope), recordTypeIdColumnNames.get(scope), Bytes.toBytes(recordType.getId()));
                put.add(systemColumnFamilies.get(scope), recordTypeVersionColumnNames.get(scope), Bytes.toBytes(recordType
                                .getVersion()));
            } else {
                put.add(systemColumnFamilies.get(scope), recordTypeIdColumnNames.get(scope), version, Bytes.toBytes(recordType.getId()));
                put.add(systemColumnFamilies.get(scope), recordTypeVersionColumnNames.get(scope), version, Bytes.toBytes(recordType
                                .getVersion()));

            }
            record.setRecordType(scope, recordType.getId(), recordType.getVersion());
            return true;
        }
        return false;
    }
    
    private boolean putUpdateAndDeleteFieldGroupFields(Map<String, Object> fields, List<String> fieldsToDelete,
                    Map<String, Object> originalFields, Scope scope, RecordType recordType,
                    byte[] columnFamily, Long version, Put put) throws FieldGroupNotFoundException,
                    FieldDescriptorNotFoundException, RecordTypeNotFoundException, RepositoryException {
        // Update fields
        boolean updated = putUpdateFields(scope, fields, originalFields, version, recordType, put);
        // Delete fields
        boolean deleted = putDeleteFields(scope, fieldsToDelete, originalFields, version, recordType, put);
        return updated || deleted;
    }

    private boolean putUpdateFields(Scope scope, Map<String, Object> fields, Map<String, Object> originalFields, Long version, RecordType recordType, Put put)
                    throws FieldGroupNotFoundException, FieldDescriptorNotFoundException, RecordTypeNotFoundException, RepositoryException {
        boolean changed = false;
        for (Entry<String, Object> field : fields.entrySet()) {
            String fieldName = field.getKey();
            Object newValue = field.getValue();
            Object originalValue = originalFields.get(fieldName);
            if (((newValue == null) && (originalValue != null)) || !newValue.equals(originalValue)) {
                FieldDescriptor fieldDescriptor = typeManager.getFieldDescriptor(scope, fieldName, recordType);
                byte[] fieldIdAsBytes = Bytes.toBytes(fieldDescriptor.getId());
                byte[] encodedFieldValue = encodeFieldValue(fieldDescriptor, newValue);

                if (version != null) {
                    put.add(columnFamilies.get(scope), fieldIdAsBytes, version, encodedFieldValue);
                } else {
                    put.add(columnFamilies.get(scope), fieldIdAsBytes, encodedFieldValue);
                }
                changed = true;
            }
        }
        return changed;
    }

    private byte[] encodeFieldValue(FieldDescriptor fieldDescriptor, Object fieldValue)
                    throws FieldGroupNotFoundException, FieldDescriptorNotFoundException, RecordTypeNotFoundException, RepositoryException {
        ValueType valueType = fieldDescriptor.getValueType();

        // TODO validate with Class#isAssignableFrom()
        byte[] encodedFieldValue = valueType.toBytes(fieldValue);
        encodedFieldValue = EncodingUtil.prefixValue(encodedFieldValue, EncodingUtil.EXISTS_FLAG);
        return encodedFieldValue;
    }

    private boolean putDeleteFields(Scope scope, List<String> fieldsToDelete, Map<String, Object> originalFields, Long version, RecordType recordType, Put put) throws FieldGroupNotFoundException, FieldDescriptorNotFoundException, RecordTypeNotFoundException, RepositoryException {
        boolean changed = false;
        for (String fieldToDelete : fieldsToDelete) {
            if (originalFields.get(fieldToDelete) != null) {
                FieldDescriptor fieldDescriptor = typeManager.getFieldDescriptor(scope, fieldToDelete, recordType);
                byte[] fieldIdAsBytes = Bytes.toBytes(fieldDescriptor.getId());
                if (version != null) {
                    put.add(columnFamilies.get(scope), fieldIdAsBytes, version, new byte[] { EncodingUtil.DELETE_FLAG });
                } else {
                    put.add(columnFamilies.get(scope), fieldIdAsBytes, new byte[] { EncodingUtil.DELETE_FLAG });
                }
                changed = true;
            }
        }
        return changed;
    }

    public Record updateMutableFields(Record record) throws InvalidRecordException, RecordNotFoundException,
                    RecordTypeNotFoundException, FieldGroupNotFoundException, FieldDescriptorNotFoundException,
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
        Long version = record.getVersion();
        if (version == null) {
            throw new InvalidRecordException(record,
                            "The version of the record cannot be null to update mutable fields");
        }
        Record newRecord = record.clone();
        RecordType recordType = typeManager.getRecordType(record.getRecordTypeId(), record.getRecordTypeVersion());
        Record originalRecord = read(record.getId(), version);
        Record originalNextRecord = null;
        try {
            originalNextRecord = read(record.getId(), version + 1);
        } catch (RecordNotFoundException exception) {
            // There is no next record
        }

        Put put = new Put(record.getId().toBytes());
        Scope scope = Scope.VERSIONABLE_MUTABLE;
        Map<String, Object> fieldsToUpdate = record.getFields(scope);
        Map<String, Object> originalFields = originalRecord
                        .getFields(scope);
        boolean updated = putUpdateFields(scope, fieldsToUpdate, originalFields, version, 
                        recordType, put);
        List<String> fieldsToDelete = record.getFieldsToDelete(scope);
        Map<String, Object> originalNextFields = new HashMap<String, Object>();
        if (originalNextRecord != null) {
            originalNextFields.putAll(originalNextRecord.getFields(scope));
        }
        boolean deleted = putDeleteMutableFields(fieldsToDelete,
                        originalFields, originalNextFields, scope, recordType,
                        columnFamilies.get(scope), version, put);
        if (updated || deleted) {
            put.add(systemColumnFamilies.get(scope), recordTypeIdColumnNames.get(scope), version, Bytes
                            .toBytes(recordType.getId()));
            put.add(systemColumnFamilies.get(scope), recordTypeVersionColumnNames.get(scope), version, Bytes
                            .toBytes(recordType.getVersion()));
            try {
                recordTable.put(put);
            } catch (IOException e) {
                throw new RepositoryException("Exception occured while putting updated record <" + record.getId()
                                + "> on HBase table", e);
            }
            newRecord.setRecordType(scope, recordType.getId(), recordType.getVersion());
        }
        return newRecord;
    }

    private boolean putDeleteMutableFields(List<String> fieldsToDelete, Map<String, Object> originalFields,
                    Map<String, Object> originalNextFields, Scope scope, RecordType recordType,
                    byte[] columnFamily, Long version, Put put) throws FieldGroupNotFoundException,
                    FieldDescriptorNotFoundException, RecordTypeNotFoundException, RepositoryException {
        boolean changed = false;
        for (String fieldToDelete : fieldsToDelete) {
            Object originalValue = originalFields.get(fieldToDelete);
            if (originalValue != null) {
                FieldDescriptor fieldDescriptor = typeManager.getFieldDescriptor(scope, fieldToDelete, recordType);
                byte[] fieldIdBytes = Bytes.toBytes(fieldDescriptor.getId());
                put.add(columnFamily, fieldIdBytes, version, new byte[] { EncodingUtil.DELETE_FLAG });
                if (originalValue.equals(originalNextFields.get(fieldToDelete))) {
                    byte[] encodedValue = encodeFieldValue(fieldDescriptor, originalValue);
                    put.add(columnFamily, fieldIdBytes, version + 1, encodedValue);
                }
                changed = true;
            }
        }
        return changed;
    }

    public Record read(RecordId recordId) throws RecordNotFoundException, RecordTypeNotFoundException,
                    FieldGroupNotFoundException, FieldDescriptorNotFoundException, RepositoryException {
        return read(recordId, null);
    }

    public Record read(RecordId recordId, List<String> nonVersionableFieldIds, List<String> versionableFieldIds,
                    List<String> versionableMutableFieldIds) throws RecordNotFoundException,
                    RecordTypeNotFoundException, FieldGroupNotFoundException, FieldDescriptorNotFoundException,
                    RepositoryException {
        return read(recordId, null, nonVersionableFieldIds, versionableFieldIds, versionableMutableFieldIds);
    }

    public Record read(RecordId recordId, Long version) throws RecordNotFoundException, RecordTypeNotFoundException,
                    FieldGroupNotFoundException, FieldDescriptorNotFoundException, RepositoryException {
        return read(recordId, version, null, null, null);
    }

    public Record read(RecordId recordId, Long version, List<String> nonVersionableFieldIds,
                    List<String> versionableFieldIds, List<String> versionableMutableFieldIds)
                    throws RecordNotFoundException, RecordTypeNotFoundException, FieldGroupNotFoundException,
                    FieldDescriptorNotFoundException, RepositoryException {
        ArgumentValidator.notNull(recordId, "recordId");
        Record record = newRecord();
        record.setId(recordId);

        Get get = new Get(recordId.toBytes());
        if (version != null) {
            get.setMaxVersions();
        }
        addFieldsToGet(get, nonVersionableFieldIds, versionableFieldIds, versionableMutableFieldIds);
        Result result;
        try {
            if (!recordTable.exists(get)) {
                throw new RecordNotFoundException(record);
            }
            result = recordTable.get(get);
        } catch (IOException e) {
            throw new RepositoryException("Exception occured while retrieving record <" + recordId
                            + "> from HBase table", e);
        }
        long currentVersion = Bytes.toLong(result.getValue(NON_VERSIONABLE_SYSTEM_COLUMN_FAMILY,
                        CURRENT_VERSION_COLUMN_NAME));
        if (version != null) {
            if (currentVersion < version) {
                throw new RecordNotFoundException(record);
            }
            record.setVersion(version);
        } else {
            record.setVersion(currentVersion);
        }

        if (extractFields(result, version, record)) {
            Pair<String, Long> recordTypePair = extractRecordType(result, record);
            record.setRecordType(recordTypePair.getV1(), recordTypePair.getV2());
        }
        return record;
    }

    private Pair<String, Long> extractRecordType(Result result, Record record) {
        return new Pair<String, Long>(Bytes.toString(result.getValue(NON_VERSIONABLE_SYSTEM_COLUMN_FAMILY,
                        RECORDTYPEID_COLUMN_NAME)), Bytes.toLong(result.getValue(NON_VERSIONABLE_SYSTEM_COLUMN_FAMILY,
                        RECORDTYPEVERSION_COLUMN_NAME)));
    }

    private Pair<String, Long> extractRecordType(Scope scope, Result result, Long version, Record record) {
        if (version == null) {
            // Get latest version
            return new Pair<String, Long>(Bytes.toString(result.getValue(systemColumnFamilies.get(scope),
                            recordTypeIdColumnNames.get(scope))), Bytes.toLong(result.getValue(
                            systemColumnFamilies.get(scope), recordTypeVersionColumnNames.get(scope))));

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

    private List<Pair<String, Object>> extractFields(NavigableMap<byte[], byte[]> familyMap)
                    throws FieldDescriptorNotFoundException, RepositoryException {
        List<Pair<String, Object>> fields = new ArrayList<Pair<String, Object>>();
        if (familyMap != null) {
            for (Entry<byte[], byte[]> entry : familyMap.entrySet()) {
                Pair<String, Object> field = extractField(entry.getKey(), entry.getValue());
                if (field != null) {
                    fields.add(field);
                }
            }
        }
        return fields;
    }

    private List<Pair<String, Object>> extractVersionFields(Long version,
                    NavigableMap<byte[], NavigableMap<Long, byte[]>> mapWithVersions)
                    throws FieldDescriptorNotFoundException, RepositoryException {
        List<Pair<String, Object>> fields = new ArrayList<Pair<String, Object>>();
        if (mapWithVersions != null) {
            for (Entry<byte[], NavigableMap<Long, byte[]>> columnWithAllVersions : mapWithVersions.entrySet()) {
                NavigableMap<Long, byte[]> allValueVersions = columnWithAllVersions.getValue();
                Entry<Long, byte[]> ceilingEntry = allValueVersions.ceilingEntry(version);
                if (ceilingEntry != null) {
                    Pair<String, Object> field = extractField(columnWithAllVersions.getKey(), ceilingEntry.getValue());
                    if (field != null) {
                        fields.add(field);
                    }
                }
            }
        }
        return fields;
    }

    private Pair<String, Object> extractField(byte[] key, byte[] prefixedValue)
                    throws FieldDescriptorNotFoundException, RepositoryException {
        if (EncodingUtil.isDeletedField(prefixedValue)) {
            return null;
        }
        String fieldId = Bytes.toString(key);
        FieldDescriptor fieldDescriptor = typeManager.getFieldDescriptor(fieldId, null);
        ValueType valueType = fieldDescriptor.getValueType();
        Object value = valueType.fromBytes(EncodingUtil.stripPrefix(prefixedValue));
        return new Pair<String, Object>(fieldDescriptor.getName(), value);
    }

    private void addFieldsToGet(Get get, List<String> nonVersionableFieldIds, List<String> versionableFieldIds,
                    List<String> versionableMutableFieldIds) throws RecordNotFoundException, RepositoryException {
        boolean addedVersionable =  addFieldsToGet(get, nonVersionableFieldIds, NON_VERSIONABLE_COLUMN_FAMILY);
        boolean addedNonVersionable = addFieldsToGet(get, versionableFieldIds, VERSIONABLE_COLUMN_FAMILY);
        boolean addedVersionableMutable = addFieldsToGet(get, versionableMutableFieldIds, VERSIONABLE_MUTABLE_COLUMN_FAMILY);
        if (addedVersionable || addedNonVersionable || addedVersionableMutable) {
            addSystemColumnsToGet(get);
        }
    }

    private boolean addFieldsToGet(Get get, List<String> fieldIds, byte[] columnFamily) {
        boolean added = false;
        if (fieldIds != null) {
            for (String fieldId : fieldIds) {
                get.addColumn(columnFamily, Bytes.toBytes(fieldId));
            }
            added = true;
        }
        return added;
    }

    private void addSystemColumnsToGet(Get get) {
        get.addColumn(NON_VERSIONABLE_SYSTEM_COLUMN_FAMILY, CURRENT_VERSION_COLUMN_NAME);
        get.addColumn(NON_VERSIONABLE_SYSTEM_COLUMN_FAMILY, RECORDTYPEID_COLUMN_NAME);
        get.addColumn(NON_VERSIONABLE_SYSTEM_COLUMN_FAMILY, RECORDTYPEVERSION_COLUMN_NAME);
        get.addColumn(NON_VERSIONABLE_SYSTEM_COLUMN_FAMILY, NON_VERSIONABLE_RECORDTYPEID_COLUMN_NAME);
        get.addColumn(NON_VERSIONABLE_SYSTEM_COLUMN_FAMILY, NON_VERSIONABLE_RECORDTYPEVERSION_COLUMN_NAME);
        get.addColumn(VERSIONABLE_SYSTEM_COLUMN_FAMILY, VERSIONABLE_RECORDTYPEID_COLUMN_NAME);
        get.addColumn(VERSIONABLE_SYSTEM_COLUMN_FAMILY, VERSIONABLE_RECORDTYPEVERSION_COLUMN_NAME);
        get.addColumn(VERSIONABLE_SYSTEM_COLUMN_FAMILY, VERSIONABLE_MUTABLE_RECORDTYPEID_COLUMN_NAME);
        get.addColumn(VERSIONABLE_SYSTEM_COLUMN_FAMILY, VERSIONABLE_MUTABLE_RECORDTYPEVERSION_COLUMN_NAME);
    }

    private boolean extractFields(Result result, Long version, Record record) throws RecordTypeNotFoundException,
                    FieldGroupNotFoundException, FieldDescriptorNotFoundException, RepositoryException {
        boolean nvExtracted = extractFields(Scope.NON_VERSIONABLE, result, null, record);
        boolean vExtracted = extractFields(Scope.VERSIONABLE, result, version, record);
        boolean vmExtracted = extractFields(Scope.VERSIONABLE_MUTABLE, result, version, record);
        return nvExtracted || vExtracted || vmExtracted;
    }

    private boolean extractFields(Scope scope, Result result, Long version, Record record)
                    throws RecordTypeNotFoundException, RepositoryException, FieldGroupNotFoundException,
                    FieldDescriptorNotFoundException {
        boolean retrieved = false;
        Pair<String, Long> recordTypePair = extractRecordType(scope, result, version, record);
        String recordTypeId = recordTypePair.getV1();
        Long recordTypeVersion = recordTypePair.getV2();
        // If there is no recordType, there can't be any fields
        if (recordTypeId != null) {
            List<Pair<String, Object>> fields;
            if (version == null) {
                fields = extractFields(result.getFamilyMap(columnFamilies.get(scope)));
            } else {
                fields = extractVersionFields(version, result.getMap().get(columnFamilies.get(scope)));
            }
            if (!fields.isEmpty()) {
                for (Pair<String, Object> field : fields) {
                    record.setField(scope, field.getV1(), field.getV2());
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
}
