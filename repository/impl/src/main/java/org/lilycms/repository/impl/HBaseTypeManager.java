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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
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
import org.lilycms.repository.api.FieldDescriptorExistsException;
import org.lilycms.repository.api.FieldDescriptorNotFoundException;
import org.lilycms.repository.api.FieldDescriptorUpdateException;
import org.lilycms.repository.api.FieldGroup;
import org.lilycms.repository.api.FieldGroupEntry;
import org.lilycms.repository.api.FieldGroupExistsException;
import org.lilycms.repository.api.FieldGroupNotFoundException;
import org.lilycms.repository.api.IdGenerator;
import org.lilycms.repository.api.PrimitiveValueType;
import org.lilycms.repository.api.RecordType;
import org.lilycms.repository.api.RecordTypeExistsException;
import org.lilycms.repository.api.RecordTypeNotFoundException;
import org.lilycms.repository.api.RepositoryException;
import org.lilycms.repository.api.TypeManager;
import org.lilycms.repository.api.ValueType;
import org.lilycms.util.ArgumentValidator;
import org.lilycms.util.Pair;

public class HBaseTypeManager implements TypeManager {

    private static final String TYPE_TABLE = "typeTable";
    private static final byte[] NON_VERSIONABLE_COLUMN_FAMILY = Bytes.toBytes("nonVersionableCF");
    private static final byte[] VERSIONABLE_COLUMN_FAMILY = Bytes.toBytes("versionableCF");

    private static final byte[] CURRENT_VERSION_COLUMN_NAME = Bytes.toBytes("$currentVersion");

    private static final byte[] RECORDTYPE_NONVERSIONABLEFIELDGROUP_COLUMN_NAME = Bytes.toBytes("$nonVersionableFG");
    private static final byte[] RECORDTYPE_VERSIONABLEFIELDGROUP_COLUMN_NAME = Bytes.toBytes("$versionableFG");
    private static final byte[] RECORDTYPE_VERSIONABLEMUTABLEFIELDGROUP_COLUMN_NAME = Bytes
                    .toBytes("$versionableMutableFG");
    private static final byte[] FIELDDESCRIPTOR_GLOBALNAME_COLUMN_NAME = Bytes.toBytes("$globalName");
    private static final byte[] FIELDDESCRIPTOR_VALUETYPE_COLUMN_NAME = Bytes.toBytes("$valueType");

    private HTable typeTable;
    private IdGenerator idGenerator;

    public HBaseTypeManager(IdGenerator idGenerator, Configuration configuration) throws IOException {
        this.idGenerator = idGenerator;
        try {
            typeTable = new HTable(configuration, TYPE_TABLE);
        } catch (IOException e) {
            HBaseAdmin admin = new HBaseAdmin(configuration);
            HTableDescriptor tableDescriptor = new HTableDescriptor(TYPE_TABLE);
            tableDescriptor.addFamily(new HColumnDescriptor(NON_VERSIONABLE_COLUMN_FAMILY));
            tableDescriptor.addFamily(new HColumnDescriptor(VERSIONABLE_COLUMN_FAMILY, HConstants.ALL_VERSIONS, "none",
                            false, true, HConstants.FOREVER, false));
            admin.createTable(tableDescriptor);
            typeTable = new HTable(configuration, TYPE_TABLE);
        }
        registerDefaultValueTypes();
    }

    public RecordType newRecordType(String recordTypeId) {
        ArgumentValidator.notNull(recordTypeId, "recordTypeId");
        return new RecordTypeImpl(recordTypeId);
    }

    public RecordType createRecordType(RecordType recordType) throws RecordTypeExistsException,
                    FieldGroupNotFoundException, RepositoryException {
        ArgumentValidator.notNull(recordType, "recordType");
        RecordType newRecordType = recordType.clone();
        Long recordTypeVersion = Long.valueOf(1);
        byte[] rowId = Bytes.toBytes(recordType.getId());
        try {
            if (typeTable.exists(new Get(rowId))) {
                throw new RecordTypeExistsException(recordType);
            }

            Put put = new Put(rowId);
            put.add(NON_VERSIONABLE_COLUMN_FAMILY, CURRENT_VERSION_COLUMN_NAME, Bytes.toBytes(recordTypeVersion));
            String fieldGroupId = recordType.getNonVersionableFieldGroupId();
            if (fieldGroupId != null) {
                newRecordType.setNonVersionableFieldGroupVersion(putFieldGroupOnRecordType(recordTypeVersion, put,
                                fieldGroupId, recordType.getNonVersionableFieldGroupVersion(),
                                RECORDTYPE_NONVERSIONABLEFIELDGROUP_COLUMN_NAME));
            }

            fieldGroupId = recordType.getVersionableFieldGroupId();
            if (fieldGroupId != null) {
                newRecordType.setVersionableFieldGroupVersion(putFieldGroupOnRecordType(recordTypeVersion, put,
                                fieldGroupId, recordType.getVersionableFieldGroupVersion(),
                                RECORDTYPE_VERSIONABLEFIELDGROUP_COLUMN_NAME));
            }

            fieldGroupId = recordType.getVersionableMutableFieldGroupId();
            if (fieldGroupId != null) {
                newRecordType.setVersionableMutableFieldGroupVersion(putFieldGroupOnRecordType(recordTypeVersion, put,
                                fieldGroupId, recordType.getVersionableMutableFieldGroupVersion(),
                                RECORDTYPE_VERSIONABLEMUTABLEFIELDGROUP_COLUMN_NAME));
            }

            typeTable.put(put);
        } catch (IOException e) {
            throw new RepositoryException("Exception occured while creating recordType <" + recordType.getId()
                            + "> on HBase", e);
        }
        newRecordType.setVersion(recordTypeVersion);
        return newRecordType;
    }

    public RecordType updateRecordType(RecordType recordType) throws RecordTypeNotFoundException,
                    FieldGroupNotFoundException, RepositoryException {
        ArgumentValidator.notNull(recordType, "recordType");
        RecordType newRecordType = recordType.clone();
        String id = recordType.getId();
        Put put = new Put(Bytes.toBytes(id));

        RecordType latestRecordType = getRecordType(id, null);
        Long latestRecordTypeVersion = latestRecordType.getVersion();
        Long newRecordTypeVersion = latestRecordTypeVersion + 1;

        boolean recordTypeChanged = false;
        // non-versionable field group
        Pair<Boolean, Long> updateResult = updateFieldGroupOnRecordType(put, newRecordTypeVersion, recordType
                        .getNonVersionableFieldGroupId(), recordType.getNonVersionableFieldGroupVersion(),
                        latestRecordType.getNonVersionableFieldGroupId(), latestRecordType
                                        .getNonVersionableFieldGroupVersion(),
                        RECORDTYPE_NONVERSIONABLEFIELDGROUP_COLUMN_NAME);
        if (updateResult.getV1()) {
            recordTypeChanged =true;
            newRecordType.setNonVersionableFieldGroupVersion(updateResult.getV2());
        }

        // versionable field group
        updateResult = updateFieldGroupOnRecordType(put, newRecordTypeVersion, recordType.getVersionableFieldGroupId(),
                        recordType.getVersionableFieldGroupVersion(), latestRecordType.getVersionableFieldGroupId(),
                        latestRecordType.getVersionableFieldGroupVersion(),
                        RECORDTYPE_VERSIONABLEFIELDGROUP_COLUMN_NAME);
        if (updateResult.getV1()) {
            recordTypeChanged = true;
            newRecordType.setVersionableFieldGroupVersion(updateResult.getV2());
        }

        // versionable mutable field group
        updateResult = updateFieldGroupOnRecordType(put, newRecordTypeVersion, recordType
                        .getVersionableMutableFieldGroupId(), recordType.getVersionableMutableFieldGroupVersion(),
                        latestRecordType.getVersionableMutableFieldGroupId(), latestRecordType
                                        .getVersionableMutableFieldGroupVersion(),
                        RECORDTYPE_VERSIONABLEMUTABLEFIELDGROUP_COLUMN_NAME);
        if (updateResult.getV1()) {
            recordTypeChanged = true;
            newRecordType.setVersionableMutableFieldGroupVersion(updateResult.getV2());
        }

        if (recordTypeChanged) {
            put.add(NON_VERSIONABLE_COLUMN_FAMILY, CURRENT_VERSION_COLUMN_NAME, Bytes.toBytes(newRecordTypeVersion));
            try {
                typeTable.put(put);
            } catch (IOException e) {
                throw new RepositoryException("Exception occured while updating recordType <" + recordType.getId()
                                + "> on HBase", e);
            }
            newRecordType.setVersion(newRecordTypeVersion);
        } else {
            newRecordType.setVersion(latestRecordTypeVersion);
        }
        return newRecordType;
    }

    private Pair<Boolean, Long> updateFieldGroupOnRecordType(Put put, Long newRecordTypeVersion, String fieldGroupId,
                    Long fieldGroupVersion, String latestFieldGroupId, Long latestFieldGroupVersion,
                    byte[] fieldGroupColumnName) throws FieldGroupNotFoundException, RepositoryException {

        if ((fieldGroupId == null)
                        || (fieldGroupId.equals(latestFieldGroupId) && (latestFieldGroupVersion
                                        .equals(fieldGroupVersion)))) {
            return new Pair<Boolean, Long>(false, null);
        } else {
            Long version = putFieldGroupOnRecordType(newRecordTypeVersion, put, fieldGroupId, fieldGroupVersion,
                            fieldGroupColumnName);
            return new Pair<Boolean, Long>(true, version);
        }
    }

    private Long putFieldGroupOnRecordType(Long recordTypeVersion, Put put, String fieldGroupId,
                    Long fieldGroupVersion, byte[] fieldGroupColumnName) throws FieldGroupNotFoundException,
                    RepositoryException {
        // Validate if the fieldGroup exists and use the latest version if none
        // was given.
        Long newFieldGroupVersion = getFieldGroup(fieldGroupId, fieldGroupVersion).getVersion();
        put.add(VERSIONABLE_COLUMN_FAMILY, fieldGroupColumnName, recordTypeVersion, encodeFieldGroup(fieldGroupId,
                        newFieldGroupVersion));
        return newFieldGroupVersion;
    }
    
    public RecordType removeFieldGroups(String recordTypeId, boolean nonVersionable, boolean versionable, boolean versionableMutable) throws RecordTypeNotFoundException, RepositoryException {
        RecordType recordType = getRecordType(recordTypeId, null);
        Long version = recordType.getVersion() + 1;
        Put put = new Put(Bytes.toBytes(recordTypeId));
        boolean changed = false;
        if (nonVersionable) {
            if (recordType.getNonVersionableFieldGroupId() != null) {
                put.add(VERSIONABLE_COLUMN_FAMILY, RECORDTYPE_NONVERSIONABLEFIELDGROUP_COLUMN_NAME, new byte[]{EncodingUtil.DELETE_FLAG});
                recordType.setNonVersionableFieldGroupId(null);
                recordType.setNonVersionableFieldGroupVersion(null);
                changed = true;
            }
        }
        if (versionable) {
            if (recordType.getVersionableFieldGroupId() != null) {
                put.add(VERSIONABLE_COLUMN_FAMILY, RECORDTYPE_VERSIONABLEFIELDGROUP_COLUMN_NAME, new byte[]{EncodingUtil.DELETE_FLAG});
                recordType.setVersionableFieldGroupId(null);
                recordType.setVersionableFieldGroupVersion(null);
                changed = true;
            }
        }
        if (versionableMutable) {
            if (recordType.getVersionableMutableFieldGroupId() != null) {
                put.add(VERSIONABLE_COLUMN_FAMILY, RECORDTYPE_VERSIONABLEMUTABLEFIELDGROUP_COLUMN_NAME, new byte[]{EncodingUtil.DELETE_FLAG});
                recordType.setVersionableMutableFieldGroupId(null);
                recordType.setVersionableMutableFieldGroupVersion(null);
                changed = true;
            }
        }
        if (changed) {
            put.add(NON_VERSIONABLE_COLUMN_FAMILY, CURRENT_VERSION_COLUMN_NAME, Bytes.toBytes(version));
            try {
                typeTable.put(put);
            } catch (IOException e) {
                throw new RepositoryException("Exception occured while removing fieldGroups from recordType <" + recordTypeId
                                + "> on HBase", e);
            }
            recordType.setVersion(version);
        }
        return recordType;
    }

    public RecordType getRecordType(String recordTypeId, Long version) throws RecordTypeNotFoundException,
                    RepositoryException {
        ArgumentValidator.notNull(recordTypeId, "recordTypeId");
        Get get = new Get(Bytes.toBytes(recordTypeId));
        if (version != null) {
            get.setMaxVersions();
        }
        Result result;
        try {
            if (!typeTable.exists(get)) {
                throw new RecordTypeNotFoundException(recordTypeId, null);
            }
            result = typeTable.get(get);
        } catch (IOException e) {
            throw new RepositoryException("Exception occured while retrieving recordType <" + recordTypeId
                            + "> from HBase table", e);
        }
        RecordType recordType = newRecordType(recordTypeId);
        Long currentVersion = Bytes.toLong(result.getValue(NON_VERSIONABLE_COLUMN_FAMILY, CURRENT_VERSION_COLUMN_NAME));
        if (version != null) {
            if (currentVersion < version) {
                throw new RecordTypeNotFoundException(recordTypeId, version);
            }
            recordType.setVersion(version);
        } else {
            recordType.setVersion(currentVersion);
        }
        Pair<String, Long> fieldGroup = extractFieldGroup(result, version,
                        RECORDTYPE_NONVERSIONABLEFIELDGROUP_COLUMN_NAME);
        if (fieldGroup != null) {
            recordType.setNonVersionableFieldGroupId(fieldGroup.getV1());
            recordType.setNonVersionableFieldGroupVersion(fieldGroup.getV2());
        }
        fieldGroup = extractFieldGroup(result, version, RECORDTYPE_VERSIONABLEFIELDGROUP_COLUMN_NAME);
        if (fieldGroup != null) {
            recordType.setVersionableFieldGroupId(fieldGroup.getV1());
            recordType.setVersionableFieldGroupVersion(fieldGroup.getV2());
        }
        fieldGroup = extractFieldGroup(result, version, RECORDTYPE_VERSIONABLEMUTABLEFIELDGROUP_COLUMN_NAME);
        if (fieldGroup != null) {
            recordType.setVersionableMutableFieldGroupId(fieldGroup.getV1());
            recordType.setVersionableMutableFieldGroupVersion(fieldGroup.getV2());
        }
        return recordType;
    }

    private Pair<String, Long> extractFieldGroup(Result result, Long version, byte[] fieldGroupColumnName) {
        byte[] fieldGroupBytes = null;
        Pair<String, Long> fieldGroup = null;
        if (version != null) {
            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> allVersionsMap = result.getMap();
            NavigableMap<byte[], NavigableMap<Long, byte[]>> versionableVersionsMap = allVersionsMap
                            .get(VERSIONABLE_COLUMN_FAMILY);
            NavigableMap<Long, byte[]> fieldGroupVersions = versionableVersionsMap.get(fieldGroupColumnName);
            if (fieldGroupVersions != null) {
                Entry<Long, byte[]> ceilingEntry = fieldGroupVersions.ceilingEntry(version);
                if (ceilingEntry != null) {
                    fieldGroupBytes = ceilingEntry.getValue();
                }
            }
        } else {
            fieldGroupBytes = result.getValue(VERSIONABLE_COLUMN_FAMILY, fieldGroupColumnName);
        }
        if (fieldGroupBytes != null) {
            fieldGroup = decodeFieldGroup(fieldGroupBytes);
        }
        return fieldGroup;
    }

    private byte[] encodeFieldGroup(String fieldGroupId, Long fieldGroupVersion) {
        byte[] bytes = Bytes.toBytes(fieldGroupVersion);
        bytes = EncodingUtil.prefixValue(Bytes.add(bytes, Bytes.toBytes(fieldGroupId)), EncodingUtil.EXISTS_FLAG);
        return bytes;
    }

    private Pair<String, Long> decodeFieldGroup(byte[] fieldGroupBytes) {
        if (EncodingUtil.isDeletedField(fieldGroupBytes)) {
            return null;
        }
        byte[] encodedFieldGroupBytes = EncodingUtil.stripPrefix(fieldGroupBytes);
        Long version = Bytes.toLong(encodedFieldGroupBytes, 0, Bytes.SIZEOF_LONG);
        String fieldGroupId = Bytes.toString(encodedFieldGroupBytes, Bytes.SIZEOF_LONG, encodedFieldGroupBytes.length
                        - Bytes.SIZEOF_LONG);
        return new Pair<String, Long>(fieldGroupId, version);
    }

    // Field Groups

    public FieldGroup newFieldGroup(String id) {
        ArgumentValidator.notNull(id, "id");
        return new FieldGroupImpl(id);
    }

    public FieldGroupEntry newFieldGroupEntry(String fieldDescriptorId, Long fieldDescriptorVersion, boolean mandatory,
                    String alias) {
        ArgumentValidator.notNull(fieldDescriptorId, "fieldDescriptorId");
        ArgumentValidator.notNull(mandatory, "mandatory");
        ArgumentValidator.notNull(alias, "alias");
        return new FieldGroupEntryImpl(fieldDescriptorId, fieldDescriptorVersion, mandatory, alias);
    }

    public FieldGroup createFieldGroup(FieldGroup fieldGroup) throws FieldGroupExistsException,
                    FieldDescriptorNotFoundException, RepositoryException {
        ArgumentValidator.notNull(fieldGroup, "fieldGroup");
        FieldGroup result;
        String id = fieldGroup.getId();
        byte[] rowId = Bytes.toBytes(id);
        Long version = Long.valueOf(1);
        result = fieldGroup.clone();
        try {
            if (typeTable.exists(new Get(rowId))) {
                throw new FieldGroupExistsException(fieldGroup);
            }
            Put put = new Put(rowId);
            put.add(NON_VERSIONABLE_COLUMN_FAMILY, CURRENT_VERSION_COLUMN_NAME, Bytes.toBytes(version));
            Collection<FieldGroupEntry> fieldGroupEntries = fieldGroup.getFieldGroupEntries();
            for (FieldGroupEntry fieldGroupEntry : fieldGroupEntries) {
                result.setFieldGroupEntry(putFieldGroupEntry(version, put, fieldGroupEntry));
            }
            typeTable.put(put);
        } catch (IOException e) {
            throw new RepositoryException("Exception occured while creating fieldGroup <" + fieldGroup.getId()
                            + "> version: <" + version + "> on HBase", e);
        }
        result.setVersion(version);
        return result;
    }

    public FieldGroup updateFieldGroup(FieldGroup fieldGroup) throws FieldGroupNotFoundException,
                    FieldDescriptorNotFoundException, RepositoryException {
        ArgumentValidator.notNull(fieldGroup, "fieldGroup");
        String id = fieldGroup.getId();
        FieldGroup latestFieldGroup = getFieldGroup(id, null);
        Long version = latestFieldGroup.getVersion() + 1;
        byte[] rowId = Bytes.toBytes(id);
        Put put = new Put(rowId);
        boolean updateNeeded = false;
        FieldGroup updatedFieldGroup = fieldGroup.clone();

        for (FieldGroupEntry fieldGroupEntry : fieldGroup.getFieldGroupEntries()) {
            FieldGroupEntry latestFieldGroupEntry = latestFieldGroup.getFieldGroupEntry(fieldGroupEntry
                            .getFieldDescriptorId());
            if (!fieldGroupEntry.equals(latestFieldGroupEntry)) {
                updatedFieldGroup.setFieldGroupEntry(putFieldGroupEntry(version, put, fieldGroupEntry));
                updateNeeded = true;
            }
        }

        if (updateNeeded) {
            updatedFieldGroup.setVersion(version);
            put.add(NON_VERSIONABLE_COLUMN_FAMILY, CURRENT_VERSION_COLUMN_NAME, Bytes.toBytes(version));
            try {
                typeTable.put(put);
            } catch (IOException e) {
                throw new RepositoryException("Exception occured while updating fieldGroup <" + fieldGroup.getId()
                                + "> version: <" + version + "> on HBase", e);
            }
        } else {
            updatedFieldGroup.setVersion(latestFieldGroup.getVersion());
        }
        return updatedFieldGroup;
    }

    private FieldGroupEntry putFieldGroupEntry(Long version, Put put, FieldGroupEntry fieldGroupEntry)
                    throws FieldDescriptorNotFoundException, RepositoryException {
        FieldGroupEntry newFieldGroupEntry = fieldGroupEntry.clone();
        // Retrieve fieldDescriptor to check it exists and get the latest
        // version number if none was given.
        Long fieldDescriptorVersion = getFieldDescriptor(fieldGroupEntry.getFieldDescriptorId(),
                        fieldGroupEntry.getFieldDescriptorVersion()).getVersion();
        newFieldGroupEntry.setFieldDescriptorVersion(fieldDescriptorVersion);
        put.add(VERSIONABLE_COLUMN_FAMILY, Bytes.toBytes(fieldGroupEntry.getFieldDescriptorId()), version,
                        encodeFieldGroupEntry(newFieldGroupEntry));
        return newFieldGroupEntry;
    }
    
    public FieldGroup removeFieldDescriptors(String fieldGroupId, List<String> fieldDescriptorIds) throws FieldGroupNotFoundException, RepositoryException{
        FieldGroup fieldGroup = getFieldGroup(fieldGroupId, null);
        Put put = new Put(Bytes.toBytes(fieldGroupId));
        Long version = fieldGroup.getVersion() + 1;
        boolean changed = false;
        for (String fieldDescriptorId : fieldDescriptorIds) {
            if (fieldGroup.getFieldGroupEntry(fieldDescriptorId) == null) {
                // Ignore
            } else {
                put.add(VERSIONABLE_COLUMN_FAMILY, Bytes.toBytes(fieldDescriptorId), version, new byte[]{EncodingUtil.DELETE_FLAG});
                fieldGroup.removeFieldGroupEntry(fieldDescriptorId);
                changed = true;
            }
        }
        if (changed) {
            put.add(NON_VERSIONABLE_COLUMN_FAMILY, CURRENT_VERSION_COLUMN_NAME, Bytes.toBytes(version));
            try {
            typeTable.put(put);
            } catch (IOException e) {
                throw new RepositoryException("Exception occured while removing fieldDescriptor <"+fieldGroupId+"> from fieldGroup <" + fieldGroup.getId()
                                + "> on HBase", e);
            }
            fieldGroup.setVersion(version);
        }
        return fieldGroup;
    }

    public FieldGroup getFieldGroup(String id, Long version) throws FieldGroupNotFoundException, RepositoryException {
        FieldGroup fieldGroup = new FieldGroupImpl(id);
        byte[] rowId = Bytes.toBytes(id);
        Get get = new Get(rowId);
        if (version != null) {
            get.setMaxVersions();
        }
        Result result;
        try {
            if (!typeTable.exists(get)) {
                throw new FieldGroupNotFoundException(id, null);
            }
            result = typeTable.get(get);
        } catch (IOException e) {
            throw new RepositoryException("Exception occured while retrieving fieldGroup <" + id + "> version: <"
                            + version + "> from HBase", e);
        }
        long latestVersion = Bytes.toLong(result.getValue(NON_VERSIONABLE_COLUMN_FAMILY, CURRENT_VERSION_COLUMN_NAME));
        if (version != null) {
            if (version > latestVersion) {
                throw new FieldGroupNotFoundException(id, version);
            }
            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> allVersionsMap = result.getMap();
            NavigableMap<byte[], NavigableMap<Long, byte[]>> versionableVersionsMap = allVersionsMap
                            .get(VERSIONABLE_COLUMN_FAMILY);
            for (Entry<byte[], NavigableMap<Long, byte[]>> entry : versionableVersionsMap.entrySet()) {
                String fieldDescriptorId = Bytes.toString(entry.getKey());
                Entry<Long, byte[]> ceilingEntry = entry.getValue().ceilingEntry(version);
                if (ceilingEntry != null) {
                    FieldGroupEntry decodedFieldGroupEntry = decodeFieldGroupEntry(ceilingEntry.getValue(), fieldDescriptorId);
                    if (decodedFieldGroupEntry != null) {
                        fieldGroup.setFieldGroupEntry(decodedFieldGroupEntry);
                    }
                }
            }
            fieldGroup.setVersion(version);
        } else {
            NavigableMap<byte[], byte[]> versionableFamilyMap = result.getFamilyMap(VERSIONABLE_COLUMN_FAMILY);
            for (Entry<byte[], byte[]> entry : versionableFamilyMap.entrySet()) {
                String fieldDescriptorId = Bytes.toString(entry.getKey());
                FieldGroupEntry decodeFieldGroupEntry = decodeFieldGroupEntry(entry.getValue(), fieldDescriptorId);
                if (decodeFieldGroupEntry != null) {
                    fieldGroup.setFieldGroupEntry(decodeFieldGroupEntry);
                }
            }
            fieldGroup.setVersion(latestVersion);
        }

        return fieldGroup;
    }

    // TODO move to some encoder/decoder
    /**
     * Encoding the fields: FD-version, mandatory, alias
     */
    private byte[] encodeFieldGroupEntry(FieldGroupEntry fieldGroupEntry) {
        // TODO check if we can use nio instead
        byte[] bytes = new byte[0];
        bytes = Bytes.add(bytes, Bytes.toBytes(fieldGroupEntry.getFieldDescriptorVersion()));
        bytes = Bytes.add(bytes, Bytes.toBytes(fieldGroupEntry.isMandatory()));
        bytes = Bytes.add(bytes, Bytes.toBytes(fieldGroupEntry.getAlias()));
        return EncodingUtil.prefixValue(bytes, EncodingUtil.EXISTS_FLAG);
    }

    private FieldGroupEntry decodeFieldGroupEntry(byte[] bytes, String fieldDescriptorId) {
        if (EncodingUtil.isDeletedField(bytes)) {
            return null;
        }
        byte[] encodedBytes = EncodingUtil.stripPrefix(bytes);
        int offset = 0;
        long fieldDescriptorVersion = Bytes.toLong(encodedBytes, 0);
        offset = offset + Bytes.SIZEOF_LONG;
        byte[] booleanBytes = new byte[Bytes.SIZEOF_BOOLEAN];
        Bytes.putBytes(booleanBytes, 0, encodedBytes, offset, Bytes.SIZEOF_BOOLEAN);
        boolean mandatory = Bytes.toBoolean(booleanBytes);
        offset = offset + Bytes.SIZEOF_BOOLEAN;
        String alias = Bytes.toString(encodedBytes, offset, encodedBytes.length - offset);
        return new FieldGroupEntryImpl(fieldDescriptorId, fieldDescriptorVersion, mandatory, alias);
    }

    public FieldDescriptor newFieldDescriptor(String id, ValueType valueType, String globalName) {
        ArgumentValidator.notNull(id, "id");
        ArgumentValidator.notNull(valueType, "valueType");
        ArgumentValidator.notNull(globalName, "globalName");
        return new FieldDescriptorImpl(id, valueType, globalName);
    }

    public FieldDescriptor createFieldDescriptor(FieldDescriptor fieldDescriptor)
                    throws FieldDescriptorExistsException, RepositoryException {
        ArgumentValidator.notNull(fieldDescriptor, "fieldDescriptor");
        FieldDescriptor result;
        byte[] rowId = Bytes.toBytes(fieldDescriptor.getId());
        Long version = Long.valueOf(1);
        try {
            if (typeTable.exists(new Get(rowId))) {
                throw new FieldDescriptorExistsException(fieldDescriptor);
            }
            Put put = new Put(rowId);
            put.add(NON_VERSIONABLE_COLUMN_FAMILY, CURRENT_VERSION_COLUMN_NAME, Bytes.toBytes(version));
            put.add(NON_VERSIONABLE_COLUMN_FAMILY, FIELDDESCRIPTOR_VALUETYPE_COLUMN_NAME, fieldDescriptor
                            .getValueType().toBytes());
            put.add(VERSIONABLE_COLUMN_FAMILY, FIELDDESCRIPTOR_GLOBALNAME_COLUMN_NAME, version, Bytes
                            .toBytes(fieldDescriptor.getGlobalName()));
            typeTable.put(put);
        } catch (IOException e) {
            throw new RepositoryException("Exception occured while creating fieldDescriptor <"
                            + fieldDescriptor.getId() + "> version: <" + version + "> on HBase", e);
        }
        result = fieldDescriptor.clone();
        result.setVersion(version);
        return result;
    }

    public FieldDescriptor updateFieldDescriptor(FieldDescriptor fieldDescriptor)
                    throws FieldDescriptorNotFoundException, FieldDescriptorUpdateException, RepositoryException {
        FieldDescriptor latestFieldDescriptor = getFieldDescriptor(fieldDescriptor.getId(), null);
        if (!fieldDescriptor.getValueType().equals(latestFieldDescriptor.getValueType())) {
            throw new FieldDescriptorUpdateException("Changing the valueType of a fieldDescriptor <"
                            + fieldDescriptor.getId() + "> is not allowed; old<" + latestFieldDescriptor.getValueType()
                            + "> new<" + fieldDescriptor.getValueType() + ">");
        }
        Long version = latestFieldDescriptor.getVersion();
        if (!fieldDescriptor.getGlobalName().equals(latestFieldDescriptor.getGlobalName())) {
            version = version + 1;
            Put put = new Put(Bytes.toBytes(fieldDescriptor.getId()));
            put.add(VERSIONABLE_COLUMN_FAMILY, FIELDDESCRIPTOR_GLOBALNAME_COLUMN_NAME, version, Bytes
                            .toBytes(fieldDescriptor.getGlobalName()));
            put.add(NON_VERSIONABLE_COLUMN_FAMILY, CURRENT_VERSION_COLUMN_NAME, Bytes.toBytes(version));
            try {
                typeTable.put(put);
            } catch (IOException e) {
                throw new RepositoryException("Exception occured while updating fieldDescriptor <"
                                + fieldDescriptor.getId() + "> on HBase", e);
            }
        }
        FieldDescriptor newFieldDescriptor = fieldDescriptor.clone();
        newFieldDescriptor.setVersion(version);
        return newFieldDescriptor;
    }

    public FieldDescriptor getFieldDescriptor(String id, Long version) throws FieldDescriptorNotFoundException,
                    RepositoryException {
        ArgumentValidator.notNull(id, "id");
        Result result;
        Get get = new Get(Bytes.toBytes(id));
        if (version != null) {
            get.setMaxVersions();
        }
        try {
            if (!typeTable.exists(get)) {
                throw new FieldDescriptorNotFoundException(id, null);
            }
            result = typeTable.get(get);
        } catch (IOException e) {
            throw new RepositoryException("Exception occured while retrieving fieldDescriptor <" + id + "> version: <"
                            + version + "> from HBase", e);
        }
        NavigableMap<byte[], byte[]> nonVersionableColumnFamily = result.getFamilyMap(NON_VERSIONABLE_COLUMN_FAMILY);
        Long currentVersion = Bytes.toLong(nonVersionableColumnFamily.get(CURRENT_VERSION_COLUMN_NAME));
        Long retrievedVersion;
        String globalName;
        if (version != null) {
            if (version > currentVersion) {
                throw new FieldDescriptorNotFoundException(id, version);
            }
            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> allVersionsMap = result.getMap();
            NavigableMap<byte[], NavigableMap<Long, byte[]>> versionableVersionsMap = allVersionsMap
                            .get(VERSIONABLE_COLUMN_FAMILY);
            NavigableMap<Long, byte[]> globalNameVersionsMap = versionableVersionsMap
                            .get(FIELDDESCRIPTOR_GLOBALNAME_COLUMN_NAME);
            globalName = Bytes.toString(globalNameVersionsMap.floorEntry(version).getValue());
            retrievedVersion = version;
        } else {
            NavigableMap<byte[], byte[]> versionableColumnFamily = result.getFamilyMap(VERSIONABLE_COLUMN_FAMILY);
            globalName = Bytes.toString(versionableColumnFamily.get(FIELDDESCRIPTOR_GLOBALNAME_COLUMN_NAME));
            retrievedVersion = currentVersion;
        }
        ValueType valueType = ValueTypeImpl.fromBytes(nonVersionableColumnFamily
                        .get(FIELDDESCRIPTOR_VALUETYPE_COLUMN_NAME), this);
        FieldDescriptor fieldDescriptor = new FieldDescriptorImpl(id, valueType, globalName);
        fieldDescriptor.setVersion(retrievedVersion);
        return fieldDescriptor;
    }

    // Value Types

    // TODO move to a primitiveValueType registry
    private Map<String, PrimitiveValueType> primitiveValueTypes = new HashMap<String, PrimitiveValueType>();

    // TODO get this from some configuration file
    private void registerDefaultValueTypes() {
        registerPrimitiveValueType(new StringValueType());
        registerPrimitiveValueType(new IntegerValueType());
        registerPrimitiveValueType(new LongValueType());
        registerPrimitiveValueType(new BooleanValueType());
        registerPrimitiveValueType(new DateValueType());
        registerPrimitiveValueType(new LinkValueType(idGenerator));
    }

    public void registerPrimitiveValueType(PrimitiveValueType primitiveValueType) {
        primitiveValueTypes.put(primitiveValueType.getName(), primitiveValueType);
    }

    public ValueType getValueType(String primitiveValueTypeName, boolean multivalue, boolean hierarchy) {
        return new ValueTypeImpl(primitiveValueTypes.get(primitiveValueTypeName), multivalue, hierarchy);
    }

}
