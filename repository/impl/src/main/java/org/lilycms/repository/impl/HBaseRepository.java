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
import org.lilycms.repository.api.FieldGroup;
import org.lilycms.repository.api.FieldGroupEntry;
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
    private static final byte[] NON_VERSIONABLE_RECORDTYPEVERSION_COLUMN_NAME = Bytes.toBytes("$NonVersionableRecordTypeVersion");
    private static final byte[] VERSIONABLE_RECORDTYPEID_COLUMN_NAME = Bytes.toBytes("$VersionableRecordTypeId");
    private static final byte[] VERSIONABLE_RECORDTYPEVERSION_COLUMN_NAME = Bytes.toBytes("$VersionableRecordTypeVersion");
    private static final byte[] VERSIONABLE_MUTABLE_RECORDTYPEID_COLUMN_NAME = Bytes.toBytes("$VersionableMutableRecordTypeId");
    private static final byte[] VERSIONABLE_MUTABLE_RECORDTYPEVERSION_COLUMN_NAME = Bytes.toBytes("$VersionableMutableRecordTypeVersion");
    private static final String RECORD_TABLE = "recordTable";
    private HTable recordTable;
    private final TypeManager typeManager;
    private final IdGenerator idGenerator;

    public HBaseRepository(TypeManager typeManager, IdGenerator idGenerator, 
                    Configuration configuration) throws IOException {
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
        if (record.getNonVersionableFields().isEmpty() && record.getVersionableFields().isEmpty()
                        && record.getVersionableMutableFields().isEmpty()) {
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

    public Record update(Record record) throws RecordNotFoundException, InvalidRecordException, RecordTypeNotFoundException, FieldGroupNotFoundException, FieldDescriptorNotFoundException, RepositoryException {
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
    
    private boolean putRecord(Record record, Record originalRecord, Long version, Put put) throws RecordTypeNotFoundException, FieldGroupNotFoundException,
                    FieldDescriptorNotFoundException, RepositoryException {
        String recordTypeId = record.getRecordTypeId();
        Long recordTypeVersion = record.getRecordTypeVersion();

        RecordType recordType = typeManager.getRecordType(recordTypeId, recordTypeVersion);

        boolean changed = false;
        boolean versionableChanged = false;
        if (putNonVersionableFields(record, originalRecord, recordType, put)) {
            changed = true;
        }
        if (putVersionableFields(record, originalRecord, recordType, version, put)) {
            changed = true;
            versionableChanged = true;
        }
        if (putVersionableMutableFields(record, originalRecord, recordType, version, put)) {
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
            put.add(NON_VERSIONABLE_SYSTEM_COLUMN_FAMILY, RECORDTYPEVERSION_COLUMN_NAME, Bytes.toBytes(recordTypeVersion));
            record.setRecordType(recordTypeId, recordTypeVersion);
            put.add(NON_VERSIONABLE_SYSTEM_COLUMN_FAMILY, CURRENT_VERSION_COLUMN_NAME, Bytes.toBytes(version));
        }
        record.setVersion(version);
        return changed;
    }
    private boolean putNonVersionableFields(Record record, Record originalRecord, RecordType recordType, Put put) throws FieldGroupNotFoundException, FieldDescriptorNotFoundException,
    RepositoryException {
        if (putFieldGroupFields(record.getNonVersionableFields(), record.getNonVersionableFieldsToDelete(), originalRecord.getNonVersionableFields(), recordType.getNonVersionableFieldGroupId(), recordType
                .getNonVersionableFieldGroupVersion(), NON_VERSIONABLE_COLUMN_FAMILY, null, put)) {
        put.add(NON_VERSIONABLE_SYSTEM_COLUMN_FAMILY, NON_VERSIONABLE_RECORDTYPEID_COLUMN_NAME, Bytes.toBytes(recordType.getId()));
        put.add(NON_VERSIONABLE_SYSTEM_COLUMN_FAMILY, NON_VERSIONABLE_RECORDTYPEVERSION_COLUMN_NAME, Bytes.toBytes(recordType.getVersion()));
        record.setNonVersionableRecordType(recordType.getId(), recordType.getVersion());
        return true;
        }
        return false;
    }
    
    private boolean putVersionableFields(Record record, Record originalRecord, RecordType recordType, Long version,
                    Put put) throws FieldGroupNotFoundException, FieldDescriptorNotFoundException,
                    RepositoryException {
        if (putFieldGroupFields(record.getVersionableFields(), record.getVersionableFieldsToDelete(), originalRecord.getVersionableFields(), recordType.getVersionableFieldGroupId(), recordType
                        .getVersionableFieldGroupVersion(), VERSIONABLE_COLUMN_FAMILY, version, put)) {
            put.add(VERSIONABLE_SYSTEM_COLUMN_FAMILY, VERSIONABLE_RECORDTYPEID_COLUMN_NAME, version, Bytes.toBytes(recordType.getId()));
            put.add(VERSIONABLE_SYSTEM_COLUMN_FAMILY, VERSIONABLE_RECORDTYPEVERSION_COLUMN_NAME, version, Bytes.toBytes(recordType.getVersion()));
            record.setVersionableRecordType(recordType.getId(), recordType.getVersion());
            return true;
        }
        return false;
    }
    
    private boolean putVersionableMutableFields(Record record, Record originalRecord, 
                    RecordType recordType, Long version, Put put) throws FieldGroupNotFoundException,
                    FieldDescriptorNotFoundException, RepositoryException {
        if (putFieldGroupFields(record.getVersionableMutableFields(), record.getVersionableMutableFieldsToDelete(), originalRecord.getVersionableMutableFields(), recordType.getVersionableMutableFieldGroupId(),
                        recordType.getVersionableMutableFieldGroupVersion(), VERSIONABLE_MUTABLE_COLUMN_FAMILY,
                        version, put)) {
            put.add(VERSIONABLE_SYSTEM_COLUMN_FAMILY, VERSIONABLE_MUTABLE_RECORDTYPEID_COLUMN_NAME, version, Bytes.toBytes(recordType.getId()));
            put.add(VERSIONABLE_SYSTEM_COLUMN_FAMILY, VERSIONABLE_MUTABLE_RECORDTYPEVERSION_COLUMN_NAME, version, Bytes.toBytes(recordType.getVersion()));
            record.setVersionableMutableRecordType(recordType.getId(), recordType.getVersion());
            return true;
        }
        return false;
    }

    private boolean putFieldGroupFields(Map<String, Object> fields, List<String> fieldsToDelete, Map<String, Object> originalFields, String fieldGroupId, Long fieldGroupVersion,
                    byte[] columnFamily, Long version, Put put) throws FieldGroupNotFoundException,
                    FieldDescriptorNotFoundException, RepositoryException {
        boolean changed = false;
        // Update fields
        for (Entry<String, Object> field : fields.entrySet()) {
            String fieldId = field.getKey();
            Object newValue = field.getValue();
            Object originalValue = originalFields.get(fieldId);
            if (((newValue == null) && (originalValue != null)) || !newValue.equals(originalValue)) {
                byte[] fieldIdAsBytes = Bytes.toBytes(fieldId);
                FieldGroup fieldGroup = typeManager.getFieldGroup(fieldGroupId, fieldGroupVersion);
                FieldGroupEntry fieldGroupEntry = fieldGroup.getFieldGroupEntry(fieldId);
                FieldDescriptor fieldDescriptor = typeManager.getFieldDescriptor(fieldGroupEntry.getFieldDescriptorId(),
                                fieldGroupEntry.getFieldDescriptorVersion());
                ValueType valueType = fieldDescriptor.getValueType();
    
                // TODO validate with Class#isAssignableFrom()
                byte[] fieldValue = valueType.toBytes(field.getValue());
                byte[] prefixedValue = EncodingUtil.prefixValue(fieldValue, EncodingUtil.EXISTS_FLAG);
    
                if (version != null) {
                    put.add(columnFamily, fieldIdAsBytes, version, prefixedValue);
                } else {
                    put.add(columnFamily, fieldIdAsBytes, prefixedValue);
                }
                changed = true;
            }
        }
        // Delete fields
        for (String fieldToDelete : fieldsToDelete) {
            if (originalFields.get(fieldToDelete) != null) {
                if (version != null) {
                    put.add(columnFamily, Bytes.toBytes(fieldToDelete), version, new byte[]{EncodingUtil.DELETE_FLAG});
                } else {
                    put.add(columnFamily, Bytes.toBytes(fieldToDelete), new byte[]{EncodingUtil.DELETE_FLAG});
                }
                changed = true;
            }
        }
        return changed;
    }

    public Record read(RecordId recordId) throws RecordNotFoundException, RecordTypeNotFoundException, FieldGroupNotFoundException, FieldDescriptorNotFoundException, RepositoryException {
        return read(recordId, null);
    }

    public Record read(RecordId recordId, List<String> nonVersionableFieldIds, List<String> versionableFieldIds, List<String> versionableMutableFieldIds) throws RecordNotFoundException, RecordTypeNotFoundException, FieldGroupNotFoundException, FieldDescriptorNotFoundException, RepositoryException {
        return read(recordId, null, nonVersionableFieldIds, versionableFieldIds, versionableMutableFieldIds);
    }

    public Record read(RecordId recordId, Long version) throws RecordNotFoundException, RecordTypeNotFoundException, FieldGroupNotFoundException, FieldDescriptorNotFoundException, RepositoryException {
        return read(recordId, version, null, null, null);
    }

    public Record read(RecordId recordId, Long version, List<String> nonVersionableFieldIds, List<String> versionableFieldIds, List<String> versionableMutableFieldIds) throws RecordNotFoundException, RecordTypeNotFoundException, FieldGroupNotFoundException, FieldDescriptorNotFoundException, RepositoryException {
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

    private Pair<String, Long> extractNonVersionableRecordType(Result result, Record record) {
        return new Pair<String, Long>(Bytes.toString(result.getValue(NON_VERSIONABLE_SYSTEM_COLUMN_FAMILY,
                        NON_VERSIONABLE_RECORDTYPEID_COLUMN_NAME)), Bytes.toLong(result.getValue(NON_VERSIONABLE_SYSTEM_COLUMN_FAMILY,
                                        NON_VERSIONABLE_RECORDTYPEVERSION_COLUMN_NAME)));
    }

    private Pair<String, Long> extractVersionableRecordType(Result result, Long version, Record record) {
        if (version == null) {
            // Get latest version
            return new Pair<String, Long>(Bytes.toString(result.getValue(VERSIONABLE_SYSTEM_COLUMN_FAMILY,
                            VERSIONABLE_RECORDTYPEID_COLUMN_NAME)), Bytes.toLong(result.getValue(VERSIONABLE_SYSTEM_COLUMN_FAMILY,
                                            VERSIONABLE_RECORDTYPEVERSION_COLUMN_NAME)));
            
        } else {
            // Get on version
            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> allVersionsMap = result.getMap();
            NavigableMap<byte[], NavigableMap<Long, byte[]>> versionableSystemCFversions = allVersionsMap.get(VERSIONABLE_SYSTEM_COLUMN_FAMILY);
            return extractVersionRecordType(version, versionableSystemCFversions,
                            VERSIONABLE_RECORDTYPEID_COLUMN_NAME, VERSIONABLE_RECORDTYPEVERSION_COLUMN_NAME);
        }
    }

    private Pair<String, Long> extractVersionableMutableRecordType(Result result, Long version, Record record) {
        if (version == null) {
            // Get latest version
            return new Pair<String, Long>(Bytes.toString(result.getValue(VERSIONABLE_SYSTEM_COLUMN_FAMILY,
                            VERSIONABLE_MUTABLE_RECORDTYPEID_COLUMN_NAME)), Bytes.toLong(result.getValue(VERSIONABLE_SYSTEM_COLUMN_FAMILY,
                            VERSIONABLE_MUTABLE_RECORDTYPEVERSION_COLUMN_NAME)));
        } else {
            // Get on version
            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> allVersionsMap = result.getMap();
            NavigableMap<byte[], NavigableMap<Long, byte[]>> versionableSystemCFversions = allVersionsMap.get(VERSIONABLE_SYSTEM_COLUMN_FAMILY);
            return extractVersionRecordType(version, versionableSystemCFversions,
                            VERSIONABLE_MUTABLE_RECORDTYPEID_COLUMN_NAME, VERSIONABLE_MUTABLE_RECORDTYPEVERSION_COLUMN_NAME);
        }
    }

    private Pair<String, Long> extractVersionRecordType(Long version,
                    NavigableMap<byte[], NavigableMap<Long, byte[]>> versionableSystemCFversions,
                    byte[] recordTypeIdColumnName, byte[] recordTypeVersionColumnName) {
        Entry<Long, byte[]> ceilingEntry = versionableSystemCFversions.get(recordTypeIdColumnName).ceilingEntry(version);
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
    
    private List<Pair<String, Object>> extractFields(NavigableMap<byte[], byte[]> familyMap) throws FieldDescriptorNotFoundException, RepositoryException {
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
    
    private List<Pair<String, Object>> extractVersionFields(Long version, NavigableMap<byte[], NavigableMap<Long, byte[]>> mapWithVersions) throws FieldDescriptorNotFoundException, RepositoryException {
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

    private Pair<String, Object> extractField(byte[] key, byte[] prefixedValue) throws FieldDescriptorNotFoundException, RepositoryException {
        if (EncodingUtil.isDeletedField(prefixedValue)) {
            return null;
        }
        String fieldId = Bytes.toString(key);
        ValueType valueType = typeManager.getFieldDescriptor(fieldId, null).getValueType();
        Object value = valueType.fromBytes(EncodingUtil.stripPrefix(prefixedValue));
        return new Pair<String, Object>(fieldId, value);
    }
    

    private void addFieldsToGet(Get get, List<String> nonVersionableFieldIds, List<String> versionableFieldIds, List<String> versionableMutableFieldIds)
                    throws RecordNotFoundException, RepositoryException {
        boolean added = false;
        added |= addFieldsToGet(get, nonVersionableFieldIds, NON_VERSIONABLE_COLUMN_FAMILY);
        added |= addFieldsToGet(get, versionableFieldIds, VERSIONABLE_COLUMN_FAMILY);
        added |= addFieldsToGet(get, versionableMutableFieldIds, VERSIONABLE_MUTABLE_COLUMN_FAMILY);
        if (added) {
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

    private boolean extractFields(Result result, Long version, Record record) throws RecordTypeNotFoundException, FieldGroupNotFoundException, FieldDescriptorNotFoundException, RepositoryException {
        boolean retrieved = false;
        retrieved |= extractNonVersionableFields(result, record);
        retrieved |= extractVersionableFields(result, version, record);
        retrieved |= extractVersionableMutableFields(result, version, record);
        return retrieved;
    }

    private boolean extractNonVersionableFields(Result result, Record record) throws RecordTypeNotFoundException,
    RepositoryException, FieldGroupNotFoundException, FieldDescriptorNotFoundException {
        boolean retrieved = false;
        Pair<String, Long> recordTypePair = extractNonVersionableRecordType(result, record);
        String recordTypeId = recordTypePair.getV1();
        Long recordTypeVersion = recordTypePair.getV2();
        // If there is no recordType, there can't be any fields
        if (recordTypeId != null) {
            List<Pair<String, Object>> fields = extractFields(result.getFamilyMap(NON_VERSIONABLE_COLUMN_FAMILY));
            if (!fields.isEmpty()) {
                for (Pair<String, Object> field : fields) {
                    record.setNonVersionableField(field.getV1(), field.getV2()); 
                }
                record.setNonVersionableRecordType(recordTypeId, recordTypeVersion);
                retrieved = true;
            }
        }
        return retrieved;
    }

    private boolean extractVersionableFields(Result result, Long version, Record record)
    throws RecordTypeNotFoundException, RepositoryException, FieldGroupNotFoundException,
    FieldDescriptorNotFoundException {
        boolean retrieved = false;
        Pair<String, Long> recordTypePair = extractVersionableRecordType(result, version, record);
        String recordTypeId = recordTypePair.getV1();
        Long recordTypeVersion = recordTypePair.getV2();
     // If there is no recordType, there can't be any fields
        if (recordTypeId != null) {
            List<Pair<String, Object>> fields;
            if (version == null) {
                fields = extractFields(result.getFamilyMap(VERSIONABLE_COLUMN_FAMILY));
            } else {
                fields = extractVersionFields(version, result.getMap().get(VERSIONABLE_COLUMN_FAMILY));
            }
            if (!fields.isEmpty()) {
                for (Pair<String, Object> field : fields) {
                    record.setVersionableField(field.getV1(), field.getV2()); 
                }
                record.setVersionableRecordType(recordTypeId, recordTypeVersion);
                retrieved = true;
            }
        }
        return retrieved;
    }

    private boolean extractVersionableMutableFields(Result result, Long version, Record record)
                    throws RecordTypeNotFoundException, RepositoryException, FieldGroupNotFoundException,
                    FieldDescriptorNotFoundException {
        boolean retrieved = false;
        Pair<String, Long> recordTypePair = extractVersionableMutableRecordType(result, version, record);
        String recordTypeId = recordTypePair.getV1();
        Long recordTypeVersion = recordTypePair.getV2();
     // If there is no recordType, there can't be any fields
        if (recordTypeId != null) {
            List<Pair<String, Object>> fields;
            if (version == null) {
                fields = extractFields(result.getFamilyMap(VERSIONABLE_MUTABLE_COLUMN_FAMILY));
            } else {
                fields = extractVersionFields(version, result.getMap().get(VERSIONABLE_MUTABLE_COLUMN_FAMILY));
            }
            if (!fields.isEmpty()) {
                for (Pair<String, Object> field : fields) {
                    record.setVersionableMutableField(field.getV1(), field.getV2()); 
                }
                record.setVersionableMutableRecordType(recordTypeId, recordTypeVersion);
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
