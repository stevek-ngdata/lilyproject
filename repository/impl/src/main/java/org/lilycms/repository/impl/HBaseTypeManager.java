package org.lilycms.repository.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilycms.repository.api.FieldDescriptor;
import org.lilycms.repository.api.RecordType;
import org.lilycms.repository.api.TypeManager;

public class HBaseTypeManager implements TypeManager {

    private static final byte EXISTS_FLAG = (byte)0;
    private static final byte DELETE_FLAG = (byte)1;
    
    private static final String TYPE_TABLE = "typeTable";
    private static final byte[] SYSTEM_COLUMN_FAMILY = Bytes.toBytes("systemCF");
    private static final byte[] SYSTEM_VERSIONABLE_COLUMN_FAMILY = Bytes.toBytes("systemVersionableCF");
    private static final byte[] FIELDDESCRIPTOR_COLUMN_FAMILY = Bytes.toBytes("fieldDescriptorCF");
    private static final byte[] CURRENT_VERSION_COLUMN_NAME = Bytes.toBytes("currentVersion");
    private static final byte[] FIELD_TYPE_COLUMN_NAME = Bytes.toBytes("fieldType");
    private static final byte[] MANDATORY_COLUMN_NAME = Bytes.toBytes("mandatory");
    private static final byte[] VERSIONABLE_COLUMN_NAME = Bytes.toBytes("versionable");

    private HTable typeTable;

    public HBaseTypeManager(Configuration configuration) throws IOException {
        try {
            typeTable = new HTable(configuration, TYPE_TABLE);
        } catch (IOException e) {
            HBaseAdmin admin = new HBaseAdmin(configuration);
            HTableDescriptor tableDescriptor = new HTableDescriptor(TYPE_TABLE);
            tableDescriptor.addFamily(new HColumnDescriptor(SYSTEM_COLUMN_FAMILY));
            tableDescriptor.addFamily(new HColumnDescriptor(SYSTEM_VERSIONABLE_COLUMN_FAMILY, HConstants.ALL_VERSIONS, "none",
                            false, true, HConstants.FOREVER, false));
            tableDescriptor.addFamily(new HColumnDescriptor(FIELDDESCRIPTOR_COLUMN_FAMILY, HConstants.ALL_VERSIONS, "none",
                            false, true, HConstants.FOREVER, false));
            admin.createTable(tableDescriptor);
            typeTable = new HTable(configuration, TYPE_TABLE);
        }
    }

    public void createRecordType(RecordType recordType) throws IOException {
        List<Put> puts = new ArrayList<Put>();
        Put put = new Put(Bytes.toBytes(recordType.getRecordTypeId()));
        long recordTypeVersion = 1;
        put.add(SYSTEM_COLUMN_FAMILY, CURRENT_VERSION_COLUMN_NAME, Bytes.toBytes(recordTypeVersion));
        Collection<FieldDescriptor> fieldDescriptors = recordType.getFieldDescriptors();
        for (FieldDescriptor fieldDescriptor : fieldDescriptors) {
            puts.add(addFieldDescriptor(fieldDescriptor, recordTypeVersion, put));
        }
        puts.add(put);
        typeTable.put(puts);
    }

    private Put addFieldDescriptor(FieldDescriptor fieldDescriptor, long recordTypeVersion, Put recordTypePut) throws IOException {
        Put put = new Put(Bytes.toBytes(fieldDescriptor.getFieldDescriptorId()));

        long fieldDescriptorVersion = fieldDescriptor.getVersion() + 1;
        put.add(SYSTEM_COLUMN_FAMILY, CURRENT_VERSION_COLUMN_NAME, Bytes.toBytes(fieldDescriptorVersion));
        put.add(SYSTEM_VERSIONABLE_COLUMN_FAMILY, FIELD_TYPE_COLUMN_NAME, fieldDescriptorVersion, Bytes.toBytes(fieldDescriptor.getFieldType()));
        put.add(SYSTEM_VERSIONABLE_COLUMN_FAMILY, MANDATORY_COLUMN_NAME, fieldDescriptorVersion, Bytes.toBytes(fieldDescriptor.isMandatory()));
        put.add(SYSTEM_VERSIONABLE_COLUMN_FAMILY, VERSIONABLE_COLUMN_NAME, fieldDescriptorVersion, Bytes.toBytes(fieldDescriptor.isVersionable()));
        
        recordTypePut.add(FIELDDESCRIPTOR_COLUMN_FAMILY, Bytes.toBytes(fieldDescriptor.getFieldDescriptorId()), recordTypeVersion, prefixValue(Bytes.toBytes(fieldDescriptorVersion), EXISTS_FLAG));
        
        return put;
    }
    
    public void updateRecordType(RecordType recordType) throws IOException {
        List<Put> puts = new ArrayList<Put>();
        Put recordTypePut = new Put(Bytes.toBytes(recordType.getRecordTypeId()));
        RecordType originalRecordType = getRecordType(recordType.getRecordTypeId());
        long newRecordTypeVersion = originalRecordType.getVersion() + 1;
        boolean recordTypeChanged = false;
        Map<String, FieldDescriptor> fieldDescriptors = new HashMap<String, FieldDescriptor>();
        for (FieldDescriptor fieldDescriptor : recordType.getFieldDescriptors()) {
            fieldDescriptors.put(fieldDescriptor.getFieldDescriptorId(), fieldDescriptor);
        }
        Collection<FieldDescriptor> originalFieldDescriptors = originalRecordType.getFieldDescriptors();
        Set<FieldDescriptor> originalFieldDescriptorSet = new HashSet<FieldDescriptor>(originalFieldDescriptors);
        for (FieldDescriptor originalFieldDescriptor : originalFieldDescriptors) {
            String fieldDescriptorId = originalFieldDescriptor.getFieldDescriptorId();
            FieldDescriptor fieldDescriptor = fieldDescriptors.get(fieldDescriptorId);
            if (fieldDescriptor != null) {
                Put updateFieldDescriptorPut = updateFieldDescriptor(fieldDescriptor, originalFieldDescriptor, newRecordTypeVersion, recordTypePut); 
                if (updateFieldDescriptorPut != null) {
                    recordTypeChanged = true;
                    puts.add(updateFieldDescriptorPut);
                }
                fieldDescriptors.remove(fieldDescriptorId);
                originalFieldDescriptorSet.remove(originalFieldDescriptor);
            }
            
        }
        if (!fieldDescriptors.isEmpty()) {
            recordTypeChanged = true;
            for (FieldDescriptor fieldDescriptor : fieldDescriptors.values()) {
                puts.add(addFieldDescriptor(fieldDescriptor, newRecordTypeVersion, recordTypePut));
            }
        }
        if (!originalFieldDescriptorSet.isEmpty()) {
            recordTypeChanged = true;
            for (FieldDescriptor fieldDescriptor : originalFieldDescriptorSet) {
                removeFieldDescriptor(fieldDescriptor, newRecordTypeVersion, recordTypePut);
            }
        }

        if (recordTypeChanged) {
            recordTypePut.add(SYSTEM_COLUMN_FAMILY, CURRENT_VERSION_COLUMN_NAME, Bytes.toBytes(newRecordTypeVersion));
            puts.add(recordTypePut);
            typeTable.put(puts);
        }
    }

    private Put updateFieldDescriptor(FieldDescriptor fieldDescriptor, FieldDescriptor originalFieldDescriptor, long recordTypeVersion, Put recordTypePut) {
        boolean fieldDescriptorChanged = false;
        long fieldDescriptorVersion = originalFieldDescriptor.getVersion() + 1;
        Put put = new Put(Bytes.toBytes(fieldDescriptor.getFieldDescriptorId()));

        if (!originalFieldDescriptor.getFieldType().equals(fieldDescriptor.getFieldType())) {
            fieldDescriptorChanged = true;
            put.add(SYSTEM_VERSIONABLE_COLUMN_FAMILY, FIELD_TYPE_COLUMN_NAME, fieldDescriptorVersion, Bytes.toBytes(fieldDescriptor.getFieldType()));
        }
        if (fieldDescriptor.isMandatory() != originalFieldDescriptor.isMandatory()) {
            fieldDescriptorChanged = true;
            put.add(SYSTEM_VERSIONABLE_COLUMN_FAMILY, MANDATORY_COLUMN_NAME, fieldDescriptorVersion, Bytes.toBytes(fieldDescriptor.isMandatory()));
        }
        if (fieldDescriptor.isVersionable() != originalFieldDescriptor.isVersionable()) {
            fieldDescriptorChanged = true;
            put.add(SYSTEM_VERSIONABLE_COLUMN_FAMILY, VERSIONABLE_COLUMN_NAME, fieldDescriptorVersion, Bytes.toBytes(fieldDescriptor.isVersionable()));
        }
        if (fieldDescriptorChanged) {
            put.add(SYSTEM_COLUMN_FAMILY, CURRENT_VERSION_COLUMN_NAME, Bytes.toBytes(fieldDescriptorVersion));
            recordTypePut.add(FIELDDESCRIPTOR_COLUMN_FAMILY, Bytes.toBytes(fieldDescriptor.getFieldDescriptorId()), recordTypeVersion, prefixValue(Bytes.toBytes(fieldDescriptorVersion), EXISTS_FLAG));
            return put;
        }
        return null;
    }

    private void removeFieldDescriptor(FieldDescriptor fieldDescriptor, long recordTypeVersion, Put recordTypePut) {
        recordTypePut.add(FIELDDESCRIPTOR_COLUMN_FAMILY, Bytes.toBytes(fieldDescriptor.getFieldDescriptorId()), recordTypeVersion, new byte[]{DELETE_FLAG});
    }

    public RecordType getRecordType(String recordTypeId) throws IOException {
        Get get = new Get(Bytes.toBytes(recordTypeId));
        Result result = typeTable.get(get);
        NavigableMap<byte[], byte[]> systemFamilyMap = result.getFamilyMap(SYSTEM_COLUMN_FAMILY);
        long version = Bytes.toLong(systemFamilyMap.get(CURRENT_VERSION_COLUMN_NAME));
        
        RecordTypeImpl recordType = new RecordTypeImpl(recordTypeId);
        recordType.setVersion(version);
        
        NavigableMap<byte[], byte[]> fieldDescriptorsFamilyMap = result.getFamilyMap(FIELDDESCRIPTOR_COLUMN_FAMILY);
        Set<Entry<byte[], byte[]>> fieldDescriptorEntries = fieldDescriptorsFamilyMap.entrySet();
        for (Entry<byte[], byte[]> fieldDescriptorEntry : fieldDescriptorEntries) {
            if (!isDeletedField(fieldDescriptorEntry.getValue())) {
                recordType.addFieldDescriptor(getFieldDescriptor(Bytes.toString(fieldDescriptorEntry.getKey()), Bytes.toLong(stripPrefix(fieldDescriptorEntry.getValue()))));
            }
        }
        return recordType;
    }

    public RecordType getRecordType(String recordTypeId, long recordTypeVersion) throws IOException {
        Get get = new Get(Bytes.toBytes(recordTypeId));
        get.setMaxVersions();
        Result result = typeTable.get(get);
        RecordTypeImpl recordType = new RecordTypeImpl(recordTypeId);
        NavigableMap<byte[],NavigableMap<byte[],NavigableMap<Long,byte[]>>> allFamiliesMap = result.getMap();
        NavigableMap<byte[], NavigableMap<Long, byte[]>> fieldDescriptorsVersionedMap = allFamiliesMap.get(FIELDDESCRIPTOR_COLUMN_FAMILY);
        Set<Entry<byte[], NavigableMap<Long, byte[]>>> fieldDescriptorsEntrySet = fieldDescriptorsVersionedMap.entrySet();
        for (Entry<byte[], NavigableMap<Long, byte[]>> fieldDescriptorEntry : fieldDescriptorsEntrySet) {
            String fieldDescriptorId = Bytes.toString(fieldDescriptorEntry.getKey());
            Entry<Long, byte[]> ceilingEntry = fieldDescriptorEntry.getValue().ceilingEntry(recordTypeVersion);
            if (ceilingEntry != null && !isDeletedField(ceilingEntry.getValue())) {
                long fieldDescriptorVersion = Bytes.toLong(stripPrefix(ceilingEntry.getValue()));
                recordType.addFieldDescriptor(getFieldDescriptor(fieldDescriptorId, fieldDescriptorVersion));
            }
        }
        recordType.setVersion(recordTypeVersion);
        return recordType;
    }

    private FieldDescriptor getFieldDescriptor(String fieldDescriptorId, long fieldDescriptorVersion) throws IOException {
        Get get = new Get(Bytes.toBytes(fieldDescriptorId));
        get.setMaxVersions();
        Result result = typeTable.get(get);
        NavigableMap<byte[],NavigableMap<byte[],NavigableMap<Long,byte[]>>> allFamiliesMap = result.getMap();
        NavigableMap<byte[], NavigableMap<Long, byte[]>> systemVersionableVersionedMap = allFamiliesMap.get(SYSTEM_VERSIONABLE_COLUMN_FAMILY);
        String fieldType = Bytes.toString(systemVersionableVersionedMap.get(FIELD_TYPE_COLUMN_NAME).ceilingEntry(fieldDescriptorVersion).getValue());
        boolean mandatory = Bytes.toBoolean(systemVersionableVersionedMap.get(MANDATORY_COLUMN_NAME).ceilingEntry(fieldDescriptorVersion).getValue());
        boolean versionable = Bytes.toBoolean(systemVersionableVersionedMap.get(VERSIONABLE_COLUMN_NAME).ceilingEntry(fieldDescriptorVersion).getValue());
        
        return new FieldDescriptorImpl(fieldDescriptorId, fieldDescriptorVersion, fieldType, mandatory, versionable);
    }

    // TODO put in a util class, duplicate code in HBaseRepository
    private boolean isDeletedField(byte[] value) {
        return value[0] == DELETE_FLAG;
    }
    
    private byte[] prefixValue(byte[] fieldValue, byte prefix) {
        byte[] prefixedValue;
        prefixedValue = new byte[fieldValue.length+1];
        prefixedValue[0] = prefix;
        System.arraycopy(fieldValue, 0, prefixedValue, 1, fieldValue.length);
        return prefixedValue;
    }
    
    private byte[] stripPrefix(byte[] prefixedValue) {
        return Arrays.copyOfRange(prefixedValue, 1, prefixedValue.length);
    }

}
