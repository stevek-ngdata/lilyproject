package org.lilycms.repository.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
import org.lilycms.repository.api.RecordNotFoundException;
import org.lilycms.repository.api.RecordType;
import org.lilycms.repository.api.Repository;
import org.lilycms.repository.api.TypeManager;

public class HBaseRepository implements Repository {

    private static final byte EXISTS_FLAG = (byte) 0;
    private static final byte DELETE_FLAG = (byte) 1;

    private static final byte[] SYSTEM_COLUMN_FAMILY = Bytes.toBytes("systemCF");
    private static final byte[] VERSIONABLE_SYSTEM_COLUMN_FAMILY = Bytes.toBytes("versionableSystemCF");
    private static final byte[] VERSIONABLE_COLUMN_FAMILY = Bytes.toBytes("versionableCF");
    private static final byte[] NON_VERSIONABLE_COLUMN_FAMILY = Bytes.toBytes("nonVersionableCF");
    private static final byte[] CURRENT_VERSION_COLUMN_NAME = Bytes.toBytes("currentVersion");
    private static final byte[] RECORDTYPENAME_COLUMN_NAME = Bytes.toBytes("$RecordTypeName");
    private static final byte[] RECORDTYPEVERSION_COLUMN_NAME = Bytes.toBytes("$RecordTypeVersion");
    private static final String RECORD_TABLE = "recordTable";
    private HTable recordTable;
    private final TypeManager typeManager;

    public HBaseRepository(TypeManager typeManager, Configuration configuration) throws IOException {
        this.typeManager = typeManager;
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

    public void create(Record record) throws RecordExistsException, RecordNotFoundException, InvalidRecordException, IOException {
        
        if (record.getFields().isEmpty()) {
            throw new InvalidRecordException(record.getRecordId(), "Creating an empty record is not allowed.");
        }

        String recordId = record.getRecordId();
        Map<String, String> variantProperties = record.getVariantProperties();
        if (!variantProperties.isEmpty()) {
            Get get = new Get(Bytes.toBytes(record.getRecordId()));
            Result masterResult = recordTable.get(get);
            if (masterResult.isEmpty()) {
                throw new RecordNotFoundException(recordId, null, null);
            }
        }
        String rowKey = generateRowKey(recordId, variantProperties);
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = recordTable.get(get);
        if (!result.isEmpty()) {
            throw new RecordExistsException(recordId, variantProperties);
        }
        Put put = createPut(record, 1);
        recordTable.put(put);
    }

    public Record read(String recordId) throws RecordNotFoundException, IOException {
        return read(recordId, null);
    }
    
    public Record read(String recordId, long version) throws RecordNotFoundException, IOException {
        return read(recordId, version, null);
    }

    public Record read(String recordId, Map<String, String> variantProperties) throws RecordNotFoundException,
                    IOException {
        return read(recordId, null, variantProperties);
    }
    
    public Record read(String recordId, long version, Map<String, String> variantProperties) throws RecordNotFoundException, IOException {
        return read(recordId, new Long(version), variantProperties);
    }
    
    private Record read(String recordId, Long version, Map<String, String> variantProperties) throws RecordNotFoundException, IOException {
        String rowKey = generateRowKey(recordId, variantProperties);
        Get get = new Get(Bytes.toBytes(rowKey));
        if (version != null) {
            get.setMaxVersions();
        }
        Result result = recordTable.get(get);
        if (result.isEmpty()) {
            throw new RecordNotFoundException(recordId, version, variantProperties);
        }
        if (version != null) {
            long currentVersion = Bytes.toLong(result.getValue(SYSTEM_COLUMN_FAMILY, CURRENT_VERSION_COLUMN_NAME));
            if (currentVersion < version) {
                throw new RecordNotFoundException(recordId, version, variantProperties);
            }
        }
        Record record = new RecordImpl(recordId);
        record.addVariantProperties(variantProperties);
        extractFields(result, version, record);
        return record;
    }

    public Record read(String recordId, String recordTypeName, long recordTypeVersion, String... fieldNames)
                    throws RecordNotFoundException, IOException {
        RecordType recordType = typeManager.getRecordType(recordTypeName, recordTypeVersion);
        Get get = new Get(Bytes.toBytes(recordId));
        get.addColumn(VERSIONABLE_SYSTEM_COLUMN_FAMILY, RECORDTYPENAME_COLUMN_NAME);
        get.addColumn(VERSIONABLE_SYSTEM_COLUMN_FAMILY, RECORDTYPEVERSION_COLUMN_NAME);
        for (String fieldName : fieldNames) {
            byte[] columnFamily = recordType.getFieldDescriptor(fieldName).isVersionable() ? VERSIONABLE_COLUMN_FAMILY
                            : NON_VERSIONABLE_COLUMN_FAMILY;
            get.addColumn(columnFamily, Bytes.toBytes(fieldName));
        }
        Result result = recordTable.get(get);
        if (result.isEmpty()) {
            throw new RecordNotFoundException(recordId, null, null);
        }
        Record record = new RecordImpl(recordId);
        extractFields(result, null, record);
        return record;
    }

    

    private void extractFields(Result result, Long version, Record record) {
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
    
    public void update(Record record) throws RecordNotFoundException, InvalidRecordException, IOException {
        Get get = new Get(Bytes.toBytes(generateRowKey(record.getRecordId(), record.getVariantProperties())));
        Result result = recordTable.get(get);
        if (result.isEmpty()) {
            throw new RecordNotFoundException(record.getRecordId(), null, record.getVariantProperties());
        }
        // TODO lock row
        NavigableMap<byte[], byte[]> systemFamilyMap = result.getFamilyMap(SYSTEM_COLUMN_FAMILY);
        long version = Bytes.toLong(systemFamilyMap.get(CURRENT_VERSION_COLUMN_NAME));
        if (record.getFields().isEmpty() && record.getDeleteFields().isEmpty()) {
            throw new InvalidRecordException(record.getRecordId(), "No fields to update or delete");
        }
        recordTable.put(createPut(record, version+1));
    }

    public void delete(String recordId) throws IOException {
        Delete delete = new Delete(Bytes.toBytes(recordId));
        recordTable.delete(delete);

    }

    private Put createPut(Record record, long version) throws IOException {
        String recordTypeName = record.getRecordTypeName();
        long recordTypeVersion = record.getRecordTypeVersion();
        RecordType recordType = typeManager.getRecordType(recordTypeName, recordTypeVersion);

        String rowId = generateRowKey(record.getRecordId(), record.getVariantProperties());
        Put put = new Put(Bytes.toBytes(rowId));
        put.add(SYSTEM_COLUMN_FAMILY, CURRENT_VERSION_COLUMN_NAME, Bytes.toBytes(version));
        put.add(VERSIONABLE_SYSTEM_COLUMN_FAMILY, RECORDTYPENAME_COLUMN_NAME, version, Bytes.toBytes(recordTypeName));
        put.add(VERSIONABLE_SYSTEM_COLUMN_FAMILY, RECORDTYPEVERSION_COLUMN_NAME, version, Bytes
                        .toBytes(recordTypeVersion));
        for (Field field : record.getFields()) {
            String fieldName = field.getName();
            byte[] fieldNameAsBytes = Bytes.toBytes(fieldName);
            byte[] fieldValue = field.getValue();
            byte[] prefixedValue = prefixValue(fieldValue, EXISTS_FLAG);

            FieldDescriptor fieldDescriptor = recordType.getFieldDescriptor(fieldName);
            if (fieldDescriptor.isVersionable()) {
                put.add(VERSIONABLE_COLUMN_FAMILY, fieldNameAsBytes, version, prefixedValue);
            } else {
                put.add(NON_VERSIONABLE_COLUMN_FAMILY, fieldNameAsBytes, prefixedValue);
            }
        }
        for (String deleteFieldName : record.getDeleteFields()) {
            FieldDescriptor fieldDescriptor = recordType.getFieldDescriptor(deleteFieldName);
            if (fieldDescriptor.isVersionable()) {
                put.add(VERSIONABLE_COLUMN_FAMILY, Bytes.toBytes(deleteFieldName), version, new byte[] { DELETE_FLAG });
            } else {
                put.add(NON_VERSIONABLE_COLUMN_FAMILY, Bytes.toBytes(deleteFieldName), new byte[] { DELETE_FLAG });
            }
        }
        return put;
    }

    private String generateRowKey(String recordId, Map<String, String> variantProperties) {
        StringBuffer rowKey = new StringBuffer();
        rowKey.append(recordId);
        if (variantProperties != null) {
            ArrayList<String> dimensions = new ArrayList<String>(variantProperties.keySet());
            Collections.sort(dimensions);
            for (String dimension : dimensions) {
                rowKey.append('|');
                rowKey.append(dimension);
                rowKey.append('|');
                rowKey.append(variantProperties.get(dimension));
            }
        }
        return rowKey.toString();
    }

    

    private void extractVersionableFieldsOnVersion(Long version, Record record,
                    NavigableMap<byte[], NavigableMap<Long, byte[]>> mapWithVersions) {
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
        NavigableMap<Long, byte[]> recordTypeNameVersions = mapWithVersions.get(RECORDTYPENAME_COLUMN_NAME);
        Entry<Long, byte[]> recordTypeNameEntry = recordTypeNameVersions.ceilingEntry(version);
        NavigableMap<Long, byte[]> recordTypeVersionVersions = mapWithVersions.get(RECORDTYPEVERSION_COLUMN_NAME);
        Entry<Long, byte[]> recordTypeVersionEntry = recordTypeVersionVersions.ceilingEntry(version);
        record.setRecordType(Bytes.toString(recordTypeNameEntry.getValue()), Bytes.toLong(recordTypeVersionEntry
                        .getValue()));
    }

    private void extractLatestRecordTypeInfo(Result result, Record record) {
        NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(VERSIONABLE_SYSTEM_COLUMN_FAMILY);
        record.setRecordType(Bytes.toString(familyMap.get(RECORDTYPENAME_COLUMN_NAME)), Bytes.toLong(familyMap
                        .get(RECORDTYPEVERSION_COLUMN_NAME)));
    }

    private void extractLatestVersionableFields(Result result, Record record) {
        NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(VERSIONABLE_COLUMN_FAMILY);
        extractFields(record, familyMap);
    }

    private void extractNonVersionableFields(Result result, Record record) {
        NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(NON_VERSIONABLE_COLUMN_FAMILY);
        extractFields(record, familyMap);
    }

    private void extractFields(Record record, NavigableMap<byte[], byte[]> familyMap) {
        if (familyMap != null) {
            for (Entry<byte[], byte[]> entry : familyMap.entrySet()) {
                addField(record, entry.getKey(), entry.getValue());
            }
        }
    }

    private void addField(Record record, byte[] key, byte[] prefixedValue) {
        if (!isDeletedField(prefixedValue)) {
            record.addField(new FieldImpl(new String(key), stripPrefix(prefixedValue)));
        }
    }

    private boolean isDeletedField(byte[] value) {
        return value[0] == DELETE_FLAG;
    }

    private byte[] prefixValue(byte[] fieldValue, byte prefix) {
        byte[] prefixedValue;
        prefixedValue = new byte[fieldValue.length + 1];
        prefixedValue[0] = prefix;
        System.arraycopy(fieldValue, 0, prefixedValue, 1, fieldValue.length);
        return prefixedValue;
    }

    private byte[] stripPrefix(byte[] prefixedValue) {
        return Arrays.copyOfRange(prefixedValue, 1, prefixedValue.length);
    }

}
