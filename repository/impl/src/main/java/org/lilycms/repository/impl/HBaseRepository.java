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
import java.lang.reflect.Constructor;
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
import org.lilycms.repository.api.Field;
import org.lilycms.repository.api.FieldDescriptor;
import org.lilycms.repository.api.InvalidRecordException;
import org.lilycms.repository.api.Record;
import org.lilycms.repository.api.RecordExistsException;
import org.lilycms.repository.api.RecordId;
import org.lilycms.repository.api.RecordNotFoundException;
import org.lilycms.repository.api.RecordType;
import org.lilycms.repository.api.Repository;
import org.lilycms.repository.api.RepositoryException;
import org.lilycms.repository.api.TypeManager;

public class HBaseRepository implements Repository {

    private static final byte[] SYSTEM_COLUMN_FAMILY = Bytes.toBytes("systemCF");
    private static final byte[] VERSIONABLE_SYSTEM_COLUMN_FAMILY = Bytes.toBytes("versionableSystemCF");
    private static final byte[] VERSIONABLE_COLUMN_FAMILY = Bytes.toBytes("versionableCF");
    private static final byte[] NON_VERSIONABLE_COLUMN_FAMILY = Bytes.toBytes("nonVersionableCF");
    private static final byte[] CURRENT_VERSION_COLUMN_NAME = Bytes.toBytes("currentVersion");
    private static final byte[] RECORDTYPEID_COLUMN_NAME = Bytes.toBytes("$RecordTypeId");
    private static final byte[] RECORDTYPEVERSION_COLUMN_NAME = Bytes.toBytes("$RecordTypeVersion");
    private static final String RECORD_TABLE = "recordTable";
    private HTable recordTable;
    private final TypeManager typeManager;
    private final IdGenerator idGenerator;
    private Class<Record> recordClass;
    private Class<Field> fieldClass;

    public HBaseRepository(TypeManager typeManager, IdGenerator idGenerator, Class recordClass, Class fieldClass,
                    Configuration configuration) throws IOException {
        this.typeManager = typeManager;
        this.idGenerator = idGenerator;
        this.recordClass = recordClass;
        this.fieldClass = fieldClass;
        try {
            recordTable = new HTable(configuration, RECORD_TABLE);
        } catch (IOException e) {
            HBaseAdmin admin = new HBaseAdmin(configuration);
            HTableDescriptor tableDescriptor = new HTableDescriptor(RECORD_TABLE);
            tableDescriptor.addFamily(new HColumnDescriptor(SYSTEM_COLUMN_FAMILY));
            tableDescriptor.addFamily(new HColumnDescriptor(VERSIONABLE_SYSTEM_COLUMN_FAMILY, HConstants.ALL_VERSIONS,
                            "none", false, true, HConstants.FOREVER, false));
            tableDescriptor.addFamily(new HColumnDescriptor(VERSIONABLE_COLUMN_FAMILY, HConstants.ALL_VERSIONS, "none",
                            false, true, HConstants.FOREVER, false));
            tableDescriptor.addFamily(new HColumnDescriptor(NON_VERSIONABLE_COLUMN_FAMILY));
            admin.createTable(tableDescriptor);
            recordTable = new HTable(configuration, RECORD_TABLE);
        }
    }

    public Record newRecord() throws RepositoryException {
        try {
            return (Record) recordClass.newInstance();
        } catch (Exception e) {
            throw new RepositoryException("Exception occured while creating new Record object", e);
        }
    }

    public Record newRecord(RecordId recordId) throws RepositoryException {
        try {
            Constructor<Record> constructor = recordClass.getConstructor(RecordId.class);
            return constructor.newInstance(recordId);
        } catch (Exception e) {
            throw new RepositoryException("Exception occured while creating new Record object <" + recordId + ">", e);
        }
    }

    public Field newField(String fieldId, byte[] value) throws RepositoryException {
        try {
            Constructor<Field> constructor = fieldClass.getConstructor(String.class, byte[].class);
            return constructor.newInstance(fieldId, value);
        } catch (Exception e) {
            throw new RepositoryException("Exception occured while creating new Field object <" + fieldId + ">", e);
        }
    }

    public void create(Record record) throws RecordExistsException, RecordNotFoundException, InvalidRecordException, RepositoryException {

        if (record.getFields().isEmpty()) {
            throw new InvalidRecordException(record, "Creating an empty record is not allowed");
        }

        RecordId recordId = record.getRecordId();
        Map<String, String> variantProperties = record.getVariantProperties();
        if (!variantProperties.isEmpty()) {
            if (recordId == null) {
                throw new InvalidRecordException(record,
                                "Creating a variant record without giving a master record id is not allowed");
            }
            Get get = new Get(EncodingUtil.generateRecordRowKey(recordId, null));
            Result masterResult;
            try {
                masterResult = recordTable.get(get);
            } catch (IOException e) {
                throw new RepositoryException("Exception occured while checking existence of master record <"+ recordId + ">", e);
            }
            if (masterResult.isEmpty()) {
                throw new RecordNotFoundException(record);
            }
        }

        if (recordId == null) {
            recordId = idGenerator.newRecordId();
            record.setRecordId(recordId);
        }

        Get get = new Get(EncodingUtil.generateRecordRowKey(recordId, variantProperties));
        Result result;
        try {
            result = recordTable.get(get);
        } catch (IOException e) {
            throw new RepositoryException("Exception occured while checking if record <" + recordId + "> already exists", e);
        }
        if (!result.isEmpty()) {
            throw new RecordExistsException(record);
        }
        Put put;
        try {
            put = createPut(record, 1);
            recordTable.put(put);
        } catch (IOException e) {
            throw new RepositoryException("Exception occured while creating record <" + recordId +"> in HBase table", e);
        }
        record.setRecordVersion(Long.valueOf(1));
    }

    public Record read(RecordId recordId, String... fieldIds) throws RecordNotFoundException, RepositoryException {
        return read(recordId, (Long) null, fieldIds);
    }

    public Record read(RecordId recordId, Long version, String... fieldIds) throws RecordNotFoundException, RepositoryException {
        return read(recordId, version, null, fieldIds);
    }

    public Record read(RecordId recordId, Map<String, String> variantProperties, String... fieldIds)
                    throws RecordNotFoundException, RepositoryException {
        return read(recordId, null, variantProperties, fieldIds);
    }

    public Record read(RecordId recordId, Long version, Map<String, String> variantProperties, String... fieldIds)
                    throws RecordNotFoundException, RepositoryException {
        Record record = newRecord();
        record.setRecordId(recordId);
        record.setRecordVersion(version);
        record.addVariantProperties(variantProperties);

        Get get = new Get(EncodingUtil.generateRecordRowKey(recordId, variantProperties));
        if (version != null) {
            get.setMaxVersions();
        }
        if (fieldIds.length != 0) {
            addFieldsToGet(get, record, fieldIds);
        }
        Result result;
        try {
            result = recordTable.get(get);
        } catch (IOException e) {
            throw new RepositoryException("Exception occured while retrieving record <" + recordId +"> from HBase table", e);
        }
        if (result.isEmpty()) {
            throw new RecordNotFoundException(record);
        }
        long currentVersion = Bytes.toLong(result.getValue(SYSTEM_COLUMN_FAMILY, CURRENT_VERSION_COLUMN_NAME));
        if (version != null) {
            if (currentVersion < version) {
                throw new RecordNotFoundException(record);
            }
        } else {
            record.setRecordVersion(currentVersion);
        }

        extractFields(result, version, record);
        return record;
    }

    private void addFieldsToGet(Get get, Record record, String... fieldIds) throws RecordNotFoundException, RepositoryException {
        String recordTypeId;
        long recordTypeVersion;
        Get recordTypeGet = new Get(EncodingUtil.generateRecordRowKey(record.getRecordId(), record
                        .getVariantProperties()));
        Long version = record.getRecordVersion();
        if (version != null) {
            get.getMaxVersions();
        }
        get.addColumn(SYSTEM_COLUMN_FAMILY, CURRENT_VERSION_COLUMN_NAME);
        get.addColumn(VERSIONABLE_SYSTEM_COLUMN_FAMILY, RECORDTYPEID_COLUMN_NAME);
        get.addColumn(VERSIONABLE_SYSTEM_COLUMN_FAMILY, RECORDTYPEVERSION_COLUMN_NAME);
        Result recordTypeResult;
        try {
            recordTypeResult = recordTable.get(recordTypeGet);
        } catch (IOException e) {
            throw new RepositoryException("Exception occured while retrieving record <"+ record.getRecordId() +"> from HBase table", e);
        }
        if (recordTypeResult.isEmpty()) {
            throw new RecordNotFoundException(record);
        }

        if (version == null) {
            recordTypeId = Bytes.toString(recordTypeResult.getValue(VERSIONABLE_SYSTEM_COLUMN_FAMILY,
                            RECORDTYPEID_COLUMN_NAME));
            recordTypeVersion = Bytes.toLong(recordTypeResult.getValue(VERSIONABLE_SYSTEM_COLUMN_FAMILY,
                            RECORDTYPEID_COLUMN_NAME));
        } else {
            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> allVersionsMap = recordTypeResult
                            .getMap();
            NavigableMap<byte[], NavigableMap<Long, byte[]>> allVersionsSystemColumnFamilyMap = allVersionsMap
                            .get(VERSIONABLE_SYSTEM_COLUMN_FAMILY);
            recordTypeId = Bytes.toString(allVersionsSystemColumnFamilyMap.get(RECORDTYPEID_COLUMN_NAME).ceilingEntry(
                            version).getValue());
            recordTypeVersion = Bytes.toLong(allVersionsSystemColumnFamilyMap.get(RECORDTYPEVERSION_COLUMN_NAME)
                            .ceilingEntry(version).getValue());
        }

        RecordType recordType = typeManager.getRecordType(recordTypeId, recordTypeVersion);

        get.addColumn(VERSIONABLE_SYSTEM_COLUMN_FAMILY, RECORDTYPEID_COLUMN_NAME);
        get.addColumn(VERSIONABLE_SYSTEM_COLUMN_FAMILY, RECORDTYPEVERSION_COLUMN_NAME);
        for (String fieldId : fieldIds) {
            byte[] columnFamily = recordType.getFieldDescriptor(fieldId).isVersionable() ? VERSIONABLE_COLUMN_FAMILY
                            : NON_VERSIONABLE_COLUMN_FAMILY;
            get.addColumn(columnFamily, Bytes.toBytes(fieldId));
        }
    }

    private void extractFields(Result result, Long version, Record record) throws RepositoryException {
        extractNonVersionableFields(result, record);
        if (version != null) {
            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> mapWithVersions = result.getMap();
            extractVersionableFieldsOnVersion(version, record, mapWithVersions.get(VERSIONABLE_COLUMN_FAMILY));
            extractRecordTypeInfoOnVersion(version, record, mapWithVersions.get(VERSIONABLE_SYSTEM_COLUMN_FAMILY));
        } else {
            extractLatestVersionableFields(result, record);
            extractLatestRecordTypeInfo(result, record);
        }
    }

    public void update(Record record) throws RecordNotFoundException, InvalidRecordException, RepositoryException {
        Get get = new Get(EncodingUtil.generateRecordRowKey(record.getRecordId(), record.getVariantProperties()));
        Result result;
        try {
            result = recordTable.get(get);
        } catch (IOException e) {
            throw new RepositoryException("Exception occured while retrieving original record <" + record.getRecordId() +"> from HBase table", e);
        }
        if (result.isEmpty()) {
            throw new RecordNotFoundException(record);
        }
        // TODO lock row
        NavigableMap<byte[], byte[]> systemFamilyMap = result.getFamilyMap(SYSTEM_COLUMN_FAMILY);
        long version = Bytes.toLong(systemFamilyMap.get(CURRENT_VERSION_COLUMN_NAME));
        if (record.getFields().isEmpty() && record.getDeleteFields().isEmpty()) {
            throw new InvalidRecordException(record, "No fields to update or delete");
        }
        long newVersion = version + 1;
        try {
            recordTable.put(createPut(record, newVersion));
        } catch (IOException e) {
            throw new RepositoryException("Exception occured while putting updated record <" + record.getRecordId() +"> on HBase table", e);
        }
        record.setRecordVersion(newVersion);
    }

    public void delete(RecordId recordId) throws RepositoryException {
        Delete delete = new Delete(EncodingUtil.generateRecordRowKey(recordId, null));
        try {
            recordTable.delete(delete);
        } catch (IOException e) {
            throw new RepositoryException("Exception occured while deleting record <"+recordId+"> from HBase table", e);
        }

    }

    private Put createPut(Record record, long version) throws RepositoryException {
        String recordTypeId = record.getRecordTypeId();
        long recordTypeVersion = record.getRecordTypeVersion();
        RecordType recordType = typeManager.getRecordType(recordTypeId, recordTypeVersion);

        Put put = new Put(EncodingUtil.generateRecordRowKey(record.getRecordId(), record.getVariantProperties()));
        put.add(SYSTEM_COLUMN_FAMILY, CURRENT_VERSION_COLUMN_NAME, Bytes.toBytes(version));
        put.add(VERSIONABLE_SYSTEM_COLUMN_FAMILY, RECORDTYPEID_COLUMN_NAME, version, Bytes.toBytes(recordTypeId));
        put.add(VERSIONABLE_SYSTEM_COLUMN_FAMILY, RECORDTYPEVERSION_COLUMN_NAME, version, Bytes
                        .toBytes(recordTypeVersion));
        for (Field field : record.getFields()) {
            String fieldId = field.getFieldId();
            byte[] fieldIdAsBytes = Bytes.toBytes(fieldId);
            byte[] fieldValue = field.getValue();
            byte[] prefixedValue = EncodingUtil.prefixValue(fieldValue, EncodingUtil.EXISTS_FLAG);

            FieldDescriptor fieldDescriptor = recordType.getFieldDescriptor(fieldId);
            if (fieldDescriptor.isVersionable()) {
                put.add(VERSIONABLE_COLUMN_FAMILY, fieldIdAsBytes, version, prefixedValue);
            } else {
                put.add(NON_VERSIONABLE_COLUMN_FAMILY, fieldIdAsBytes, prefixedValue);
            }
        }
        for (String deleteFieldId : record.getDeleteFields()) {
            FieldDescriptor fieldDescriptor = recordType.getFieldDescriptor(deleteFieldId);
            if (fieldDescriptor.isVersionable()) {
                put.add(VERSIONABLE_COLUMN_FAMILY, Bytes.toBytes(deleteFieldId), version,
                                new byte[] { EncodingUtil.DELETE_FLAG });
            } else {
                put.add(NON_VERSIONABLE_COLUMN_FAMILY, Bytes.toBytes(deleteFieldId),
                                new byte[] { EncodingUtil.DELETE_FLAG });
            }
        }
        return put;
    }

    private void extractVersionableFieldsOnVersion(Long version, Record record,
                    NavigableMap<byte[], NavigableMap<Long, byte[]>> mapWithVersions) throws RepositoryException {
        if (mapWithVersions != null) {
            Set<Entry<byte[], NavigableMap<Long, byte[]>>> columnSetWithAllVersions = mapWithVersions.entrySet();
            for (Entry<byte[], NavigableMap<Long, byte[]>> columnWithAllVersions : columnSetWithAllVersions) {
                NavigableMap<Long, byte[]> allValueVersions = columnWithAllVersions.getValue();
                Entry<Long, byte[]> ceilingEntry = allValueVersions.ceilingEntry(version);
                if (ceilingEntry != null) {
                    addField(record, columnWithAllVersions.getKey(), ceilingEntry.getValue());
                }
            }
        }
    }

    private void extractRecordTypeInfoOnVersion(Long version, Record record,
                    NavigableMap<byte[], NavigableMap<Long, byte[]>> mapWithVersions) {
        NavigableMap<Long, byte[]> recordTypeIdVersions = mapWithVersions.get(RECORDTYPEID_COLUMN_NAME);
        Entry<Long, byte[]> recordTypeIdEntry = recordTypeIdVersions.ceilingEntry(version);
        NavigableMap<Long, byte[]> recordTypeVersionVersions = mapWithVersions.get(RECORDTYPEVERSION_COLUMN_NAME);
        Entry<Long, byte[]> recordTypeVersionEntry = recordTypeVersionVersions.ceilingEntry(version);
        record.setRecordType(Bytes.toString(recordTypeIdEntry.getValue()), Bytes.toLong(recordTypeVersionEntry
                        .getValue()));
    }

    private void extractLatestRecordTypeInfo(Result result, Record record) {
        NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(VERSIONABLE_SYSTEM_COLUMN_FAMILY);
        record.setRecordType(Bytes.toString(familyMap.get(RECORDTYPEID_COLUMN_NAME)), Bytes.toLong(familyMap
                        .get(RECORDTYPEVERSION_COLUMN_NAME)));
    }

    private void extractLatestVersionableFields(Result result, Record record) throws RepositoryException {
        NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(VERSIONABLE_COLUMN_FAMILY);
        extractFields(record, familyMap);
    }

    private void extractNonVersionableFields(Result result, Record record) throws RepositoryException {
        NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(NON_VERSIONABLE_COLUMN_FAMILY);
        extractFields(record, familyMap);
    }

    private void extractFields(Record record, NavigableMap<byte[], byte[]> familyMap) throws RepositoryException {
        if (familyMap != null) {
            for (Entry<byte[], byte[]> entry : familyMap.entrySet()) {
                addField(record, entry.getKey(), entry.getValue());
            }
        }
    }

    private void addField(Record record, byte[] key, byte[] prefixedValue) throws RepositoryException {
        if (!EncodingUtil.isDeletedField(prefixedValue)) {
            record.addField(newField(Bytes.toString(key), EncodingUtil.stripPrefix(prefixedValue)));
        }
    }

}
