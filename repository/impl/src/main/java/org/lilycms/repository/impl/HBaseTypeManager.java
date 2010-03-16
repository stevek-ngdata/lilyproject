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
import java.util.ArrayList;
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
import org.lilycms.repository.api.RepositoryException;
import org.lilycms.repository.api.TypeManager;

public class HBaseTypeManager implements TypeManager {

    private static final String TYPE_TABLE = "typeTable";
    private static final byte[] SYSTEM_COLUMN_FAMILY = Bytes.toBytes("systemCF");
    private static final byte[] SYSTEM_VERSIONABLE_COLUMN_FAMILY = Bytes.toBytes("systemVersionableCF");
    private static final byte[] FIELDDESCRIPTOR_COLUMN_FAMILY = Bytes.toBytes("fieldDescriptorCF");
    private static final byte[] CURRENT_VERSION_COLUMN_NAME = Bytes.toBytes("currentVersion");
    private static final byte[] FIELD_TYPE_COLUMN_NAME = Bytes.toBytes("fieldType");
    private static final byte[] MANDATORY_COLUMN_NAME = Bytes.toBytes("mandatory");
    private static final byte[] VERSIONABLE_COLUMN_NAME = Bytes.toBytes("versionable");

    private HTable typeTable;
    private Class<RecordType> recordTypeClass;
    private Class<FieldDescriptor> fieldDescriptorClass;

    public HBaseTypeManager(Class recordTypeClass, Class fieldDescriptorClass, Configuration configuration) throws IOException {
        this.recordTypeClass = recordTypeClass;
        this.fieldDescriptorClass = fieldDescriptorClass;
        try {
            typeTable = new HTable(configuration, TYPE_TABLE);
        } catch (IOException e) {
            HBaseAdmin admin = new HBaseAdmin(configuration);
            HTableDescriptor tableDescriptor = new HTableDescriptor(TYPE_TABLE);
            tableDescriptor.addFamily(new HColumnDescriptor(SYSTEM_COLUMN_FAMILY));
            tableDescriptor.addFamily(new HColumnDescriptor(SYSTEM_VERSIONABLE_COLUMN_FAMILY, HConstants.ALL_VERSIONS,
                            "none", false, true, HConstants.FOREVER, false));
            tableDescriptor.addFamily(new HColumnDescriptor(FIELDDESCRIPTOR_COLUMN_FAMILY, HConstants.ALL_VERSIONS,
                            "none", false, true, HConstants.FOREVER, false));
            admin.createTable(tableDescriptor);
            typeTable = new HTable(configuration, TYPE_TABLE);
        }
    }

    public RecordType newRecordType(String recordTypeId) throws RepositoryException {
        try {
            Constructor<RecordType> constructor = recordTypeClass.getConstructor(String.class);
            return constructor.newInstance(recordTypeId);
        } catch (Exception e) {
            throw new RepositoryException("Exception occured while creating new RecordType object", e);
        }
}
    
    public FieldDescriptor newFieldDescriptor(String fieldDescriptorId, String fieldType, boolean mandatory, boolean versionable) throws RepositoryException {
        try {
            Constructor<FieldDescriptor> constructor = fieldDescriptorClass.getConstructor(String.class, String.class, boolean.class, boolean.class);
            return constructor.newInstance(fieldDescriptorId, fieldType, mandatory, versionable);
        } catch (Exception e) {
            throw new RepositoryException("Exception occured while creating new FieldDescriptor object", e);
        }
}
    
    public FieldDescriptor newFieldDescriptor(String fieldDescriptorId, long version, String fieldType, boolean mandatory, boolean versionable) throws RepositoryException {
            try {
                Constructor<FieldDescriptor> constructor = fieldDescriptorClass.getConstructor(String.class, long.class, String.class, boolean.class, boolean.class);
                return constructor.newInstance(fieldDescriptorId, version, fieldType, mandatory, versionable);
            } catch (Exception e) {
                throw new RepositoryException("Exception occured while creating new FieldDescriptor object", e);
            }
    }
    
    public void createRecordType(RecordType recordType) throws RepositoryException {
        List<Put> puts = new ArrayList<Put>();
        String recordTypeId = recordType.getRecordTypeId();
        Put put = new Put(EncodingUtil.generateRecordTypeRowKey(recordTypeId));
        long recordTypeVersion = 1;
        put.add(SYSTEM_COLUMN_FAMILY, CURRENT_VERSION_COLUMN_NAME, Bytes.toBytes(recordTypeVersion));
        Collection<FieldDescriptor> fieldDescriptors = recordType.getFieldDescriptors();
        for (FieldDescriptor fieldDescriptor : fieldDescriptors) {
            puts.add(addFieldDescriptor(fieldDescriptor, recordTypeId, recordTypeVersion, put));
        }
        puts.add(put);
        try {
            typeTable.put(puts);
        } catch (IOException e) {
            throw new RepositoryException("Exception occured while creating recordType <"
                            + recordType.getRecordTypeId() + "> on HBase", e);
        }
    }

    private Put addFieldDescriptor(FieldDescriptor fieldDescriptor, String recordTypeId, long recordTypeVersion,
                    Put recordTypePut) throws RepositoryException {
        String fieldDescriptorId = fieldDescriptor.getFieldDescriptorId();
        Put put = new Put(EncodingUtil.generateFieldDescriptorRowKey(recordTypeId, fieldDescriptorId));

        long fieldDescriptorVersion = getLatestFieldDescriptorVersion(recordTypeId, fieldDescriptorId) + 1;
        put.add(SYSTEM_COLUMN_FAMILY, CURRENT_VERSION_COLUMN_NAME, Bytes.toBytes(fieldDescriptorVersion));
        put.add(SYSTEM_VERSIONABLE_COLUMN_FAMILY, FIELD_TYPE_COLUMN_NAME, fieldDescriptorVersion, Bytes
                        .toBytes(fieldDescriptor.getFieldType()));
        put.add(SYSTEM_VERSIONABLE_COLUMN_FAMILY, MANDATORY_COLUMN_NAME, fieldDescriptorVersion, Bytes
                        .toBytes(fieldDescriptor.isMandatory()));
        put.add(SYSTEM_VERSIONABLE_COLUMN_FAMILY, VERSIONABLE_COLUMN_NAME, fieldDescriptorVersion, Bytes
                        .toBytes(fieldDescriptor.isVersionable()));

        recordTypePut.add(FIELDDESCRIPTOR_COLUMN_FAMILY, Bytes.toBytes(fieldDescriptorId), recordTypeVersion,
                        EncodingUtil.prefixValue(Bytes.toBytes(fieldDescriptorVersion), EncodingUtil.EXISTS_FLAG));

        return put;
    }

    public void updateRecordType(RecordType recordType) throws RepositoryException {
        List<Put> puts = new ArrayList<Put>();
        String recordTypeId = recordType.getRecordTypeId();
        Put recordTypePut = new Put(EncodingUtil.generateRecordTypeRowKey(recordTypeId));
        RecordType originalRecordType = getRecordType(recordTypeId);
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
                Put updateFieldDescriptorPut = updateFieldDescriptor(fieldDescriptor, originalFieldDescriptor,
                                recordTypeId, newRecordTypeVersion, recordTypePut);
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
                puts.add(addFieldDescriptor(fieldDescriptor, recordTypeId, newRecordTypeVersion, recordTypePut));
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
            try {
                typeTable.put(puts);
            } catch (IOException e) {
                throw new RepositoryException("Exception occured while writing updated recordType <" + recordTypeId
                                + "> to HBase table", e);
            }
        }
    }

    private Put updateFieldDescriptor(FieldDescriptor fieldDescriptor, FieldDescriptor originalFieldDescriptor,
                    String recordTypeId, long recordTypeVersion, Put recordTypePut) throws RepositoryException {
        boolean fieldDescriptorChanged = false;
        long fieldDescriptorVersion = 0;
        String fieldDescriptorId = fieldDescriptor.getFieldDescriptorId();
        Put put = new Put(EncodingUtil.generateFieldDescriptorRowKey(recordTypeId, fieldDescriptorId));

        if (!originalFieldDescriptor.getFieldType().equals(fieldDescriptor.getFieldType())) {
            if (fieldDescriptorChanged == false) {
                fieldDescriptorChanged = true;
                fieldDescriptorVersion = getLatestFieldDescriptorVersion(recordTypeId, fieldDescriptorId) + 1;
            }
            put.add(SYSTEM_VERSIONABLE_COLUMN_FAMILY, FIELD_TYPE_COLUMN_NAME, fieldDescriptorVersion, Bytes
                            .toBytes(fieldDescriptor.getFieldType()));
        }
        if (fieldDescriptor.isMandatory() != originalFieldDescriptor.isMandatory()) {
            if (fieldDescriptorChanged == false) {
                fieldDescriptorChanged = true;
                fieldDescriptorVersion = getLatestFieldDescriptorVersion(recordTypeId, fieldDescriptorId) + 1;
            }
            put.add(SYSTEM_VERSIONABLE_COLUMN_FAMILY, MANDATORY_COLUMN_NAME, fieldDescriptorVersion, Bytes
                            .toBytes(fieldDescriptor.isMandatory()));
        }
        if (fieldDescriptor.isVersionable() != originalFieldDescriptor.isVersionable()) {
            if (fieldDescriptorChanged == false) {
                fieldDescriptorChanged = true;
                fieldDescriptorVersion = getLatestFieldDescriptorVersion(recordTypeId, fieldDescriptorId) + 1;
            }
            put.add(SYSTEM_VERSIONABLE_COLUMN_FAMILY, VERSIONABLE_COLUMN_NAME, fieldDescriptorVersion, Bytes
                            .toBytes(fieldDescriptor.isVersionable()));
        }
        if (fieldDescriptorChanged) {
            put.add(SYSTEM_COLUMN_FAMILY, CURRENT_VERSION_COLUMN_NAME, Bytes.toBytes(fieldDescriptorVersion));
            recordTypePut.add(FIELDDESCRIPTOR_COLUMN_FAMILY, Bytes.toBytes(fieldDescriptorId), recordTypeVersion,
                            EncodingUtil.prefixValue(Bytes.toBytes(fieldDescriptorVersion), EncodingUtil.EXISTS_FLAG));
            return put;
        }
        return null;
    }

    private void removeFieldDescriptor(FieldDescriptor fieldDescriptor, long recordTypeVersion, Put recordTypePut) {
        recordTypePut.add(FIELDDESCRIPTOR_COLUMN_FAMILY, Bytes.toBytes(fieldDescriptor.getFieldDescriptorId()),
                        recordTypeVersion, new byte[] { EncodingUtil.DELETE_FLAG });
    }

    public RecordType getRecordType(String recordTypeId) throws RepositoryException {
        Get get = new Get(EncodingUtil.generateRecordTypeRowKey(recordTypeId));
        Result result;
        try {
            result = typeTable.get(get);
        } catch (IOException e) {
            throw new RepositoryException("Exception occured while retrieving recordType <" + recordTypeId
                            + "> from HBase table", e);
        }
        NavigableMap<byte[], byte[]> systemFamilyMap = result.getFamilyMap(SYSTEM_COLUMN_FAMILY);
        long version = Bytes.toLong(systemFamilyMap.get(CURRENT_VERSION_COLUMN_NAME));

        RecordType recordType = newRecordType(recordTypeId);
        recordType.setVersion(version);

        NavigableMap<byte[], byte[]> fieldDescriptorsFamilyMap = result.getFamilyMap(FIELDDESCRIPTOR_COLUMN_FAMILY);
        Set<Entry<byte[], byte[]>> fieldDescriptorEntries = fieldDescriptorsFamilyMap.entrySet();
        for (Entry<byte[], byte[]> fieldDescriptorEntry : fieldDescriptorEntries) {
            if (!EncodingUtil.isDeletedField(fieldDescriptorEntry.getValue())) {
                recordType.addFieldDescriptor(getFieldDescriptor(recordTypeId, Bytes.toString(fieldDescriptorEntry
                                .getKey()), Bytes.toLong(EncodingUtil.stripPrefix(fieldDescriptorEntry.getValue()))));
            }
        }
        return recordType;
    }

    public RecordType getRecordType(String recordTypeId, long recordTypeVersion) throws RepositoryException {
        Get get = new Get(EncodingUtil.generateRecordTypeRowKey(recordTypeId));
        get.setMaxVersions();
        Result result;
        try {
            result = typeTable.get(get);
        } catch (IOException e) {
            throw new RepositoryException("Exception occured while retrieving recordType <" + recordTypeId
                            + "> from HBase table", e);
        }
        RecordType recordType = newRecordType(recordTypeId);
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> allFamiliesMap = result.getMap();
        NavigableMap<byte[], NavigableMap<Long, byte[]>> fieldDescriptorsVersionedMap = allFamiliesMap
                        .get(FIELDDESCRIPTOR_COLUMN_FAMILY);
        Set<Entry<byte[], NavigableMap<Long, byte[]>>> fieldDescriptorsEntrySet = fieldDescriptorsVersionedMap
                        .entrySet();
        for (Entry<byte[], NavigableMap<Long, byte[]>> fieldDescriptorEntry : fieldDescriptorsEntrySet) {
            String fieldDescriptorId = Bytes.toString(fieldDescriptorEntry.getKey());
            Entry<Long, byte[]> ceilingEntry = fieldDescriptorEntry.getValue().ceilingEntry(recordTypeVersion);
            if (ceilingEntry != null && !EncodingUtil.isDeletedField(ceilingEntry.getValue())) {
                long fieldDescriptorVersion = Bytes.toLong(EncodingUtil.stripPrefix(ceilingEntry.getValue()));
                recordType.addFieldDescriptor(getFieldDescriptor(recordTypeId, fieldDescriptorId,
                                fieldDescriptorVersion));
            }
        }
        recordType.setVersion(recordTypeVersion);
        return recordType;
    }

    private FieldDescriptor getFieldDescriptor(String recordTypeId, String fieldDescriptorId,
                    long fieldDescriptorVersion) throws RepositoryException {
        Get get = new Get(EncodingUtil.generateFieldDescriptorRowKey(recordTypeId, fieldDescriptorId));
        get.setMaxVersions();
        Result result;
        try {
            result = typeTable.get(get);
        } catch (IOException e) {
            throw new RepositoryException("Exception occured while retrieving fieldDescriptor <" + fieldDescriptorId
                            + "> for recordType <" + recordTypeId + "> from HBase table", e);
        }
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> allFamiliesMap = result.getMap();
        NavigableMap<byte[], NavigableMap<Long, byte[]>> systemVersionableVersionedMap = allFamiliesMap
                        .get(SYSTEM_VERSIONABLE_COLUMN_FAMILY);
        String fieldType = Bytes.toString(systemVersionableVersionedMap.get(FIELD_TYPE_COLUMN_NAME).ceilingEntry(
                        fieldDescriptorVersion).getValue());
        boolean mandatory = Bytes.toBoolean(systemVersionableVersionedMap.get(MANDATORY_COLUMN_NAME).ceilingEntry(
                        fieldDescriptorVersion).getValue());
        boolean versionable = Bytes.toBoolean(systemVersionableVersionedMap.get(VERSIONABLE_COLUMN_NAME).ceilingEntry(
                        fieldDescriptorVersion).getValue());

        return newFieldDescriptor(fieldDescriptorId, fieldDescriptorVersion, fieldType, mandatory, versionable);
    }

    private long getLatestFieldDescriptorVersion(String recordTypeId, String fieldDescriptorId)
                    throws RepositoryException {
        Get get = new Get(EncodingUtil.generateFieldDescriptorRowKey(recordTypeId, fieldDescriptorId));
        get.addColumn(SYSTEM_COLUMN_FAMILY, CURRENT_VERSION_COLUMN_NAME);
        Result result;
        try {
            result = typeTable.get(get);
        } catch (IOException e) {
            throw new RepositoryException("Exception occured while retrieving fielddescriptor <" + fieldDescriptorId
                            + "> of recordType <" + recordTypeId + ">", e);
        }
        if (result.isEmpty()) {
            return 0;
        }
        return Bytes.toLong(result.getValue(SYSTEM_COLUMN_FAMILY, CURRENT_VERSION_COLUMN_NAME));
    }
}
