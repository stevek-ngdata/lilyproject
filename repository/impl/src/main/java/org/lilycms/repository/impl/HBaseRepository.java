package org.lilycms.repository.impl;

import java.io.IOException;
import java.util.Arrays;
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

    private static final byte EXISTS_FLAG = (byte)0;
    private static final byte DELETE_FLAG = (byte)1;
    
    private static final byte[] CURRENT_VERSION_COLUMN = Bytes.toBytes("currentVersion");
    private static final byte[] SYSTEM_COLUMN_FAMILY = Bytes.toBytes("systemCF");
    private static final byte[] VERSIONABLE_COLUMN_FAMILY = Bytes.toBytes("versionableCF");
    private static final byte[] NON_VERSIONABLE_COLUMN_FAMILY = Bytes.toBytes("nonVersionableCF");
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
            tableDescriptor.addFamily(new HColumnDescriptor(VERSIONABLE_COLUMN_FAMILY, HConstants.ALL_VERSIONS, "none",
                            false, true, HConstants.FOREVER, false));
            tableDescriptor.addFamily(new HColumnDescriptor(NON_VERSIONABLE_COLUMN_FAMILY));
            admin.createTable(tableDescriptor);
            recordTable = new HTable(configuration, RECORD_TABLE);
        }
    }

    public void create(Record record) throws IOException, RecordExistsException {
        String recordId = record.getRecordId();
        Get get = new Get(Bytes.toBytes(recordId));
        Result result = recordTable.get(get);
        if (!result.isEmpty()) {
            throw new RecordExistsException(recordId);
        }
        if (record.getFields().isEmpty()) {
            // TODO throw decent exception
            // TODO is it actually a problem in case of updates?
            throw new IOException("Record <" + record.getRecordId() + "> does not contain any fields");
        }
        Put put = createPut(record, 1);
        recordTable.put(put);
    }

    public Record read(String recordId) throws RecordNotFoundException, IOException {
        Get get = new Get(Bytes.toBytes(recordId));
        Result result = recordTable.get(get);
        Record record = createRecord(result, null);
        if (record == null) {
            throw new RecordNotFoundException(recordId);
        }
        return record;
    }

    public Record read(String recordId, String recordTypeName, long recordTypeVersion, String... fieldNames)
                    throws IOException {
        RecordType recordType = typeManager.getRecordType(recordTypeName, recordTypeVersion);
        Get get = new Get(Bytes.toBytes(recordId));
        for (String fieldName : fieldNames) {
            byte[] columnFamily = recordType.getFieldDescriptor(fieldName).isVersionable() ? VERSIONABLE_COLUMN_FAMILY : NON_VERSIONABLE_COLUMN_FAMILY;
            get.addColumn(columnFamily, Bytes.toBytes(fieldName));
        }
        Result result = recordTable.get(get);
        return createRecord(result, null);
    }

    public Record read(String recordId, long version) throws IOException {
        Get get = new Get(Bytes.toBytes(recordId));
        get.setMaxVersions();
        Result result = recordTable.get(get);
        return createRecord(result, version);
    }

    public void update(Record record) throws RecordNotFoundException, InvalidRecordException, IOException {
        Get get = new Get(Bytes.toBytes(record.getRecordId()));
        Result result = recordTable.get(get);
        if (result.isEmpty()) {
            throw new RecordNotFoundException(record.getRecordId());
        }
        NavigableMap<byte[], byte[]> systemFamilyMap = result.getFamilyMap(SYSTEM_COLUMN_FAMILY);
        long version = Bytes.toLong(systemFamilyMap.get(CURRENT_VERSION_COLUMN));
        if (record.getFields().isEmpty() && record.getDeleteFields().isEmpty()) {
            throw new InvalidRecordException(record.getRecordId(), "No fields to update or delete");
        }
        recordTable.put(createPut(record, ++version));
    }

    public void delete(String recordId) throws IOException {
        Delete delete = new Delete(Bytes.toBytes(recordId));
        recordTable.delete(delete);

    }

    private Put createPut(Record record, long version) throws IOException {
        Put put = new Put(Bytes.toBytes(record.getRecordId()));
        put.add(SYSTEM_COLUMN_FAMILY, CURRENT_VERSION_COLUMN, Bytes.toBytes(version));
        RecordType recordType = typeManager.getRecordType(record.getRecordTypeName(), record.getRecordTypeVersion());
        for (Field field : record.getFields()) {
            String fieldName = field.getName();
            byte[] fieldNameAsBytes = Bytes.toBytes(fieldName);
            byte[] fieldValue = field.getValue();
            byte[] prefixedValue = new byte[fieldValue.length+1];
            prefixedValue[0] = EXISTS_FLAG;
            System.arraycopy(fieldValue, 0, prefixedValue, 1, fieldValue.length);
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
                put.add(VERSIONABLE_COLUMN_FAMILY, Bytes.toBytes(deleteFieldName), new byte[]{DELETE_FLAG});
            } else {
                put.add(NON_VERSIONABLE_COLUMN_FAMILY, Bytes.toBytes(deleteFieldName), new byte[]{DELETE_FLAG});
            }
        }
        return put;
    }
    
    private Record createRecord(Result result, Long version) {
        Record record;
        byte[] rowKey = result.getRow();
        if (rowKey == null)
            return null;
        record = new RecordImpl(new String(rowKey));
        NavigableMap<byte[], byte[]> nonVersionableFamilyMap = result.getFamilyMap(NON_VERSIONABLE_COLUMN_FAMILY);
        Set<Entry<byte[], byte[]>> entrySet;
        if (nonVersionableFamilyMap != null) {
            entrySet = nonVersionableFamilyMap.entrySet();
            for (Entry<byte[], byte[]> entry : entrySet) {
                addField(record, entry.getKey(), entry.getValue());
            }
        }
        if (version != null) {
            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> mapWithVersions = result.getMap();
            NavigableMap<byte[], NavigableMap<Long, byte[]>> versionableCFMapWithVersions = mapWithVersions
                            .get(VERSIONABLE_COLUMN_FAMILY);
            if (versionableCFMapWithVersions != null) {
                Set<Entry<byte[], NavigableMap<Long, byte[]>>> columnSetWithAllVersions = versionableCFMapWithVersions
                                .entrySet();
                for (Entry<byte[], NavigableMap<Long, byte[]>> columnWithAllVersions : columnSetWithAllVersions) {
                    NavigableMap<Long, byte[]> allValueVersions = columnWithAllVersions.getValue();
                    Entry<Long, byte[]> ceilingEntry = allValueVersions.ceilingEntry(version);
                    if (ceilingEntry != null) {
                        addField(record, columnWithAllVersions.getKey(), ceilingEntry.getValue());
                    }
                }
            }
        } else {
            NavigableMap<byte[], byte[]> versionableFamilyMap = result.getFamilyMap(VERSIONABLE_COLUMN_FAMILY);
            if (versionableFamilyMap != null) {
                entrySet = versionableFamilyMap.entrySet();
                for (Entry<byte[], byte[]> entry : entrySet) {
                    addField(record, entry.getKey(), entry.getValue());
                }
            }
        }
        return record;
    }

    private void addField(Record record, byte[] key, byte[] prefixedValue) {
        if (!isDeletedField(prefixedValue)) {
            record.addField(new FieldImpl(new String(key), Arrays.copyOfRange(prefixedValue, 1, prefixedValue.length)));
        }
    }

    private boolean isDeletedField(byte[] value) {
        return value[0] == DELETE_FLAG;
    }

}
