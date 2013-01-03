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
package org.lilyproject.repository.impl;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.UUID;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;
import org.lilyproject.bytes.api.DataInput;
import org.lilyproject.bytes.api.DataOutput;
import org.lilyproject.bytes.impl.DataInputImpl;
import org.lilyproject.bytes.impl.DataOutputImpl;
import org.lilyproject.repository.api.ConcurrentUpdateTypeException;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.FieldTypeEntry;
import org.lilyproject.repository.api.FieldTypeExistsException;
import org.lilyproject.repository.api.FieldTypeNotFoundException;
import org.lilyproject.repository.api.FieldTypeUpdateException;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.RecordTypeExistsException;
import org.lilyproject.repository.api.RecordTypeNotFoundException;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.TypeBucket;
import org.lilyproject.repository.api.TypeException;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.api.ValueType;
import org.lilyproject.repository.impl.id.SchemaIdImpl;
import org.lilyproject.util.ArgumentValidator;
import org.lilyproject.util.Pair;
import org.lilyproject.util.hbase.HBaseTableFactory;
import org.lilyproject.util.hbase.LilyHBaseSchema;
import org.lilyproject.util.hbase.LilyHBaseSchema.TypeCf;
import org.lilyproject.util.hbase.LilyHBaseSchema.TypeColumn;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.repo.VersionTag;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

import static org.lilyproject.util.hbase.LilyHBaseSchema.DELETE_MARKER;
import static org.lilyproject.util.hbase.LilyHBaseSchema.EXISTS_FLAG;

public class HBaseTypeManager extends AbstractTypeManager implements TypeManager {

    private static final Long CONCURRENT_TIMEOUT = 5000L; // The concurrent timeout should be large enough to allow for type caches to be refreshed an a clock skew between the servers

    private HTableInterface typeTable;

    public HBaseTypeManager(IdGenerator idGenerator, Configuration configuration, ZooKeeperItf zooKeeper, HBaseTableFactory hbaseTableFactory)
            throws IOException, InterruptedException, KeeperException, RepositoryException {
        schemaCache = new LocalSchemaCache(zooKeeper, this);
        this.idGenerator = idGenerator;

        this.typeTable = LilyHBaseSchema.getTypeTable(hbaseTableFactory);
        registerDefaultValueTypes();
        schemaCache.start();

        // The 'last' vtag should always exist in the system (at least, for everything index-related). Therefore we
        // create it here.
        try {
            FieldType fieldType = newFieldType(getValueType("LONG"), VersionTag.LAST, Scope.NON_VERSIONED);
            createFieldType(fieldType);
        } catch (FieldTypeExistsException e) {
            // ok
        } catch (ConcurrentUpdateTypeException e) {
            // ok, another lily-server is starting up and doing the same thing
        }
    }

    @Override
    @PreDestroy
    public void close() throws IOException {
        schemaCache.close();
    }

    @Override
    public RecordType createRecordType(RecordType recordType) throws TypeException {
        ArgumentValidator.notNull(recordType, "recordType");
        ArgumentValidator.notNull(recordType.getName(), "recordType.name");

        RecordType newRecordType = recordType.clone();
        Long recordTypeVersion = Long.valueOf(1);
        try {
            SchemaId id = getValidId();
            byte[] rowId = id.getBytes();
            // Take a counter on a row with the name as key
            byte[] nameBytes = encodeName(recordType.getName());

            // Prepare put
            Put put = new Put(rowId);
            put.add(TypeCf.DATA.bytes, TypeColumn.VERSION.bytes, Bytes.toBytes(recordTypeVersion));
            put.add(TypeCf.DATA.bytes, TypeColumn.RECORDTYPE_NAME.bytes, nameBytes);

            // Prepare newRecordType
            newRecordType.setId(id);
            newRecordType.setVersion(recordTypeVersion);

            // Check for concurrency
            long now = System.currentTimeMillis();
            checkConcurrency(recordType.getName(), nameBytes, now);

            // FIXME: is this a reliable check? Cache might be out of date in case changes happen on other nodes?
            if (getRecordTypeFromCache(recordType.getName()) != null) {
                clearConcurrency(nameBytes, now);
                throw new RecordTypeExistsException(recordType);
            }

            Collection<FieldTypeEntry> fieldTypeEntries = recordType.getFieldTypeEntries();
            for (FieldTypeEntry fieldTypeEntry : fieldTypeEntries) {
                putFieldTypeEntry(recordTypeVersion, put, fieldTypeEntry);
            }

            Map<SchemaId, Long> mixins = recordType.getMixins();
            for (Entry<SchemaId, Long> mixin : mixins.entrySet()) {
                newRecordType.addMixin(mixin.getKey(), putMixinOnRecordType(recordTypeVersion, put, mixin.getKey(),
                        mixin.getValue()));
            }

            // Put the record type on the table
            getTypeTable().put(put);

            // Refresh the caches
            updateRecordTypeCache(newRecordType);

            // Clear the concurrency timestamp
            clearConcurrency(nameBytes, now);
        } catch (IOException e) {
            throw new TypeException("Exception occurred while creating recordType '" + recordType.getName()
                    + "' on HBase", e);
        } catch (InterruptedException e) {
            throw new TypeException("Exception occurred while creating recordType '" + recordType.getName()
                    + "' on HBase", e);
        }
        return newRecordType;
    }

    private Long putMixinOnRecordType(Long recordTypeVersion, Put put, SchemaId mixinId, Long mixinVersion)
            throws TypeException {
        Long newMixinVersion = getRecordTypeByIdWithoutCache(mixinId, mixinVersion).getVersion();
        put.add(TypeCf.MIXIN.bytes, mixinId.getBytes(), recordTypeVersion, Bytes.toBytes(newMixinVersion));
        return newMixinVersion;
    }

    @Override
    public RecordType updateRecordType(RecordType recordType) throws RepositoryException, InterruptedException {
        ArgumentValidator.notNull(recordType, "recordType");

        if (recordType.getId() == null && recordType.getName() == null) {
            throw new IllegalArgumentException("No id or name specified in the supplied record type.");
        }

        SchemaId id;
        if (recordType.getId() == null) {
            // Map the name to id
            RecordType existingType = getRecordTypeFromCache(recordType.getName());
            if (existingType == null) {
                throw new RecordTypeNotFoundException(recordType.getName(), null);
            } else {
                id = existingType.getId();
            }
        } else {
            id = recordType.getId();
        }

        RecordType newRecordType = recordType.clone();
        newRecordType.setId(id);

        byte[] rowId = id.getBytes();
        byte[] nameBytes = null;
        Long now = null;

        try {

            // Do an exists check first
            if (!getTypeTable().exists(new Get(rowId))) {
                throw new RecordTypeNotFoundException(recordType.getId(), null);
            }

            // Only do the concurrency check when a name was given
            QName name = recordType.getName();
            if (name != null) {
                nameBytes = encodeName(name);

                // Check for concurrency
                now = System.currentTimeMillis();
                checkConcurrency(recordType.getName(), nameBytes, now);
            }

            // Prepare the update
            RecordType latestRecordType = getRecordTypeByIdWithoutCache(id, null);
            // If no name was given, continue to use the name that was already on the record type
            if (name == null) {
                newRecordType.setName(latestRecordType.getName());
            }
            Long latestRecordTypeVersion = latestRecordType.getVersion();
            Long newRecordTypeVersion = latestRecordTypeVersion + 1;

            Put put = new Put(rowId);
            boolean fieldTypeEntriesChanged = updateFieldTypeEntries(put, newRecordTypeVersion, newRecordType,
                    latestRecordType);

            boolean mixinsChanged = updateMixins(put, newRecordTypeVersion, newRecordType, latestRecordType);

            boolean nameChanged = updateName(put, newRecordType, latestRecordType);

            // Update the record type on the table
            if (fieldTypeEntriesChanged || mixinsChanged || nameChanged) {
                put.add(TypeCf.DATA.bytes, TypeColumn.VERSION.bytes, Bytes.toBytes(newRecordTypeVersion));
                getTypeTable().put(put);
                newRecordType.setVersion(newRecordTypeVersion);
            } else {
                newRecordType.setVersion(latestRecordTypeVersion);
            }

            // Refresh the caches
            updateRecordTypeCache(newRecordType);

        } catch (IOException e) {
            throw new TypeException("Exception occurred while updating recordType '" + newRecordType.getId()
                    + "' on HBase", e);
        } catch (InterruptedException e) {
            throw new TypeException("Exception occurred while updating recordType '" + newRecordType.getId()
                    + "' on HBase", e);
        } finally {
            if (nameBytes != null && now != null) {
                clearConcurrency(nameBytes, now);
            }
        }
        return newRecordType;
    }

    @Override
    public RecordType createOrUpdateRecordType(RecordType recordType) throws RepositoryException, InterruptedException {
        if (recordType.getId() != null) {
            return updateRecordType(recordType);
        } else {
            if (recordType.getName() == null) {
                throw new IllegalArgumentException("No id or name specified in supplied record type.");
            }

            boolean exists = getRecordTypeFromCache(recordType.getName()) != null;

            int attempts;
            for (attempts = 0; attempts < 3; attempts++) {
                if (exists) {
                    try {
                        return updateRecordType(recordType);
                    } catch (RecordTypeNotFoundException e) {
                        // record type was renamed in the meantime, retry
                        exists = false;
                    }
                } else {
                    try {
                        return createRecordType(recordType);
                    } catch (RecordTypeExistsException e) {
                        // record type was created in the meantime, retry
                        exists = true;
                    }
                }
            }
            throw new TypeException("Record type create-or-update failed after " + attempts +
                    " attempts, toggling between create and update mode.");

        }
    }

    private boolean updateName(Put put, RecordType recordType, RecordType latestRecordType) throws TypeException, RepositoryException, InterruptedException {
        if (!recordType.getName().equals(latestRecordType.getName())) {
            try {
                getRecordTypeByName(recordType.getName(), null);
                throw new TypeException("Changing the name '" + recordType.getName() + "' of a recordType '"
                        + recordType.getId() + "' to a name that already exists is not allowed; old '"
                        + latestRecordType.getName() + "' new '" + recordType.getName() + "'");
            } catch (RecordTypeNotFoundException allowed) {
            }
            put.add(TypeCf.DATA.bytes, TypeColumn.RECORDTYPE_NAME.bytes, encodeName(recordType.getName()));
            return true;
        }
        return false;
    }

    @Override
    protected RecordType getRecordTypeByIdWithoutCache(SchemaId id, Long version) throws RecordTypeNotFoundException,
            TypeException {
        ArgumentValidator.notNull(id, "recordTypeId");
        Get get = new Get(id.getBytes());
        if (version != null) {
            get.setMaxVersions();
        }
        Result result;
        try {
            result = getTypeTable().get(get);
            // This covers the case where a given id would match a name that was
            // used for setting the concurrent counters
            if (result == null || result.isEmpty()
                    || result.getValue(TypeCf.DATA.bytes, TypeColumn.VERSION.bytes) == null) {
                throw new RecordTypeNotFoundException(id, null);
            }
        } catch (IOException e) {
            throw new TypeException("Exception occurred while retrieving recordType '" + id + "' from HBase table", e);
        }
        return extractRecordType(id, version, result);
    }

    private RecordType extractRecordType(SchemaId id, Long version, Result result) throws RecordTypeNotFoundException {
        NavigableMap<byte[], byte[]> nonVersionableColumnFamily = result.getFamilyMap(TypeCf.DATA.bytes);
        QName name;
        name = decodeName(nonVersionableColumnFamily.get(TypeColumn.RECORDTYPE_NAME.bytes));
        RecordType recordType = newRecordType(id, name);
        Long currentVersion = Bytes.toLong(result.getValue(TypeCf.DATA.bytes, TypeColumn.VERSION.bytes));
        if (version != null) {
            if (currentVersion < version) {
                throw new RecordTypeNotFoundException(id, version);
            }
            recordType.setVersion(version);
        } else {
            recordType.setVersion(currentVersion);
        }
        extractFieldTypeEntries(result, version, recordType);
        extractMixins(result, version, recordType);
        return recordType;
    }

    private boolean updateFieldTypeEntries(Put put, Long newRecordTypeVersion, RecordType recordType,
                                           RecordType latestRecordType) throws FieldTypeNotFoundException, TypeException {
        boolean changed = false;
        Collection<FieldTypeEntry> latestFieldTypeEntries = latestRecordType.getFieldTypeEntries();
        // Update FieldTypeEntries
        for (FieldTypeEntry fieldTypeEntry : recordType.getFieldTypeEntries()) {
            FieldTypeEntry latestFieldTypeEntry = latestRecordType.getFieldTypeEntry(fieldTypeEntry.getFieldTypeId());
            if (!fieldTypeEntry.equals(latestFieldTypeEntry)) {
                putFieldTypeEntry(newRecordTypeVersion, put, fieldTypeEntry);
                changed = true;
            }
            latestFieldTypeEntries.remove(latestFieldTypeEntry);
        }
        // Remove remaining FieldTypeEntries
        for (FieldTypeEntry fieldTypeEntry : latestFieldTypeEntries) {
            put.add(TypeCf.FIELDTYPE_ENTRY.bytes, fieldTypeEntry.getFieldTypeId().getBytes(), newRecordTypeVersion,
                    DELETE_MARKER);
            changed = true;
        }
        return changed;
    }

    private void putFieldTypeEntry(Long version, Put put, FieldTypeEntry fieldTypeEntry)
            throws FieldTypeNotFoundException, TypeException {
        byte[] idBytes = fieldTypeEntry.getFieldTypeId().getBytes();
        Get get = new Get(idBytes);
        try {
            if (!getTypeTable().exists(get)) {
                throw new FieldTypeNotFoundException(fieldTypeEntry.getFieldTypeId());
            }
        } catch (IOException e) {
            throw new TypeException("Exception occurred while checking existance of FieldTypeEntry '"
                    + fieldTypeEntry.getFieldTypeId() + "' on HBase", e);
        }
        put.add(TypeCf.FIELDTYPE_ENTRY.bytes, idBytes, version, encodeFieldTypeEntry(fieldTypeEntry));
    }

    private boolean updateMixins(Put put, Long newRecordTypeVersion, RecordType recordType, RecordType latestRecordType) {
        boolean changed = false;
        Map<SchemaId, Long> latestMixins = latestRecordType.getMixins();
        // Update mixins
        for (Entry<SchemaId, Long> entry : recordType.getMixins().entrySet()) {
            SchemaId mixinId = entry.getKey();
            Long mixinVersion = entry.getValue();
            if (!mixinVersion.equals(latestMixins.get(mixinId))) {
                put.add(TypeCf.MIXIN.bytes, mixinId.getBytes(), newRecordTypeVersion, Bytes.toBytes(mixinVersion));
                changed = true;
            }
            latestMixins.remove(mixinId);
        }
        // Remove remaining mixins
        for (Entry<SchemaId, Long> entry : latestMixins.entrySet()) {
            put.add(TypeCf.MIXIN.bytes, entry.getKey().getBytes(), newRecordTypeVersion, DELETE_MARKER);
            changed = true;
        }
        return changed;
    }

    private void extractFieldTypeEntries(Result result, Long version, RecordType recordType) {
        if (version != null) {
            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> allVersionsMap = result.getMap();
            NavigableMap<byte[], NavigableMap<Long, byte[]>> fieldTypeEntriesVersionsMap = allVersionsMap
                    .get(TypeCf.FIELDTYPE_ENTRY.bytes);
            if (fieldTypeEntriesVersionsMap != null) {
                for (Entry<byte[], NavigableMap<Long, byte[]>> entry : fieldTypeEntriesVersionsMap.entrySet()) {
                    SchemaId fieldTypeId = new SchemaIdImpl(entry.getKey());
                    Entry<Long, byte[]> ceilingEntry = entry.getValue().ceilingEntry(version);
                    if (ceilingEntry != null) {
                        FieldTypeEntry fieldTypeEntry = decodeFieldTypeEntry(ceilingEntry.getValue(), fieldTypeId);
                        if (fieldTypeEntry != null) {
                            recordType.addFieldTypeEntry(fieldTypeEntry);
                        }
                    }
                }
            }
        } else {
            NavigableMap<byte[], byte[]> versionableMap = result.getFamilyMap(TypeCf.FIELDTYPE_ENTRY.bytes);
            if (versionableMap != null) {
                for (Entry<byte[], byte[]> entry : versionableMap.entrySet()) {
                    SchemaId fieldTypeId = new SchemaIdImpl(entry.getKey());
                    FieldTypeEntry fieldTypeEntry = decodeFieldTypeEntry(entry.getValue(), fieldTypeId);
                    if (fieldTypeEntry != null) {
                        recordType.addFieldTypeEntry(fieldTypeEntry);
                    }
                }
            }
        }
    }

    private void extractMixins(Result result, Long version, RecordType recordType) {
        if (version != null) {
            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> allVersionsMap = result.getMap();
            NavigableMap<byte[], NavigableMap<Long, byte[]>> mixinVersionsMap = allVersionsMap.get(TypeCf.MIXIN.bytes);
            if (mixinVersionsMap != null) {
                for (Entry<byte[], NavigableMap<Long, byte[]>> entry : mixinVersionsMap.entrySet()) {
                    SchemaId mixinId = new SchemaIdImpl(entry.getKey());
                    Entry<Long, byte[]> ceilingEntry = entry.getValue().ceilingEntry(version);
                    if (ceilingEntry != null) {
                        if (!EncodingUtil.isDeletedField(ceilingEntry.getValue())) {
                            recordType.addMixin(mixinId, Bytes.toLong(ceilingEntry.getValue()));
                        }
                    }
                }
            }
        } else {
            NavigableMap<byte[], byte[]> mixinMap = result.getFamilyMap(TypeCf.MIXIN.bytes);
            if (mixinMap != null) {
                for (Entry<byte[], byte[]> entry : mixinMap.entrySet()) {
                    if (!EncodingUtil.isDeletedField(entry.getValue())) {
                        recordType.addMixin(new SchemaIdImpl(entry.getKey()), Bytes.toLong(entry.getValue()));
                    }
                }
            }
        }
    }

    /**
     * Encoding the fields: FD-version, mandatory, alias
     */
    private byte[] encodeFieldTypeEntry(FieldTypeEntry fieldTypeEntry) {
        byte[] bytes = new byte[0];
        bytes = Bytes.add(bytes, Bytes.toBytes(fieldTypeEntry.isMandatory()));
        return EncodingUtil.prefixValue(bytes, EXISTS_FLAG);
    }

    private FieldTypeEntry decodeFieldTypeEntry(byte[] bytes, SchemaId fieldTypeId) {
        if (EncodingUtil.isDeletedField(bytes)) {
            return null;
        }
        byte[] encodedBytes = EncodingUtil.stripPrefix(bytes);
        boolean mandatory = Bytes.toBoolean(encodedBytes);
        return new FieldTypeEntryImpl(fieldTypeId, mandatory);
    }

    @Override
    public FieldType createFieldType(ValueType valueType, QName name, Scope scope) throws RepositoryException,
            InterruptedException {
        return createFieldType(newFieldType(valueType, name, scope));
    }

    @Override
    public FieldType createFieldType(String valueType, QName name, Scope scope) throws RepositoryException,
            InterruptedException {
        return createFieldType(newFieldType(getValueType(valueType), name, scope));
    }

    @Override
    public FieldType createFieldType(FieldType fieldType) throws RepositoryException {
        ArgumentValidator.notNull(fieldType, "fieldType");
        ArgumentValidator.notNull(fieldType.getName(), "fieldType.name");
        ArgumentValidator.notNull(fieldType.getValueType(), "fieldType.valueType");
        ArgumentValidator.notNull(fieldType.getScope(), "fieldType.scope");

        FieldType newFieldType;
        Long version = Long.valueOf(1);
        try {
            SchemaId id = getValidId();
            byte[] rowId = id.getBytes();
            byte[] nameBytes = encodeName(fieldType.getName());

            // Prepare the put
            Put put = new Put(rowId);
            put.add(TypeCf.DATA.bytes, TypeColumn.FIELDTYPE_VALUETYPE.bytes, encodeValueType(fieldType.getValueType()));
            put.add(TypeCf.DATA.bytes, TypeColumn.FIELDTYPE_SCOPE.bytes, Bytes
                    .toBytes(fieldType.getScope().name()));
            put.add(TypeCf.DATA.bytes, TypeColumn.FIELDTYPE_NAME.bytes, nameBytes);

            // Prepare newFieldType
            newFieldType = fieldType.clone();
            newFieldType.setId(id);

            // Check if there is already a fieldType with this name
            if (schemaCache.fieldTypeExists(fieldType.getName())) {
                throw new FieldTypeExistsException(fieldType);
            }

            // FIXME: the flow here is different than for record types, were first the name reservation is taken
            // and then the existence is checked.
            // Check for concurrency
            long now = System.currentTimeMillis();
            checkConcurrency(fieldType.getName(), nameBytes, now);

            // Create the actual field type
            getTypeTable().put(put);

            // Refresh the caches
            updateFieldTypeCache(newFieldType);

            // Clear the concurrency timestamp
            clearConcurrency(nameBytes, now);
        } catch (IOException e) {
            throw new TypeException("Exception occurred while creating fieldType '" + fieldType.getName()
                    + "' version: '" + version + "' on HBase", e);
        } catch (InterruptedException e) {
            throw new TypeException("Exception occurred while creating fieldType '" + fieldType.getName()
                    + "' version: '" + version + "' on HBase", e);
        }
        return newFieldType;
    }

    @Override
    public FieldType updateFieldType(FieldType fieldType) throws RepositoryException, InterruptedException {

        ArgumentValidator.notNull(fieldType, "fieldType");

        if (fieldType.getId() == null || fieldType.getName() == null) {
            // Since the only updateable property of a field type is its name, it only makes sense
            // that both ID and name are present (without ID, the name cannot be updated, without
            // name, there is nothing to update)
            throw new TypeException("ID and name must be specified in the field type to update.");
        }

        FieldType newFieldType = fieldType.clone();

        byte[] rowId = fieldType.getId().getBytes();
        byte[] nameBytes = null;
        Long now = null;

        try {
            // Do an exists check first
            if (!getTypeTable().exists(new Get(rowId))) {
                throw new FieldTypeNotFoundException(fieldType.getId());
            }
            // First increment the counter on the row with the name as key, then
            // read the field type
            nameBytes = encodeName(fieldType.getName());

            // Check for concurrency
            now = System.currentTimeMillis();
            checkConcurrency(fieldType.getName(), nameBytes, now);

            // Prepare the update
            FieldType latestFieldType = getFieldTypeByIdWithoutCache(fieldType.getId());
            copyUnspecifiedFields(newFieldType, latestFieldType);
            checkImmutableFieldsCorrespond(newFieldType, latestFieldType);
            if (!newFieldType.getName().equals(latestFieldType.getName())) {
                try {
                    // TODO FIXME: doesn't this rely on the field type cache being up to date?
                    getFieldTypeByName(newFieldType.getName());
                    throw new FieldTypeUpdateException("Changing the name '" + newFieldType.getName()
                            + "' of a fieldType '" + newFieldType.getId()
                            + "' to a name that already exists is not allowed; old '" + latestFieldType.getName()
                            + "' new '" + newFieldType.getName() + "'");
                } catch (FieldTypeNotFoundException allowed) {
                }
                // Update the field type on the table
                Put put = new Put(rowId);
                put.add(TypeCf.DATA.bytes, TypeColumn.FIELDTYPE_NAME.bytes, nameBytes);

                getTypeTable().put(put);
            }

            // Update the caches
            updateFieldTypeCache(newFieldType.clone());
        } catch (IOException e) {
            throw new TypeException("Exception occurred while updating fieldType '" + fieldType.getId() + "' on HBase",
                    e);
        } catch (InterruptedException e) {
            throw new TypeException("Exception occurred while updating fieldType '" + fieldType.getId() + "' on HBase",
                    e);
        } finally {
            if (nameBytes != null && now != null) {
                clearConcurrency(nameBytes, now);
            }
        }

        return newFieldType;
    }

    @Override
    public FieldType createOrUpdateFieldType(FieldType fieldType) throws RepositoryException, InterruptedException {
        ArgumentValidator.notNull(fieldType, "fieldType");

        if (fieldType.getId() == null && fieldType.getName() == null) {
            throw new TypeException("No ID or name specified in the field type to create-or-update.");
        }

        if (fieldType.getId() != null && fieldType.getName() != null) {
            // If the ID is specified, we can assume it is a field type which is supposed to exist already
            // so we call update. We also require the name to be specified, since the name is the only
            // property of field type which can be updated, so otherwise it makes no sense to call update
            // (update will throw an exception in that case).
            return updateFieldType(fieldType);
        } else if (fieldType.getId() != null) {
            // There's nothing to update or create: just fetch the field type, check its state corresponds
            // and return it
            FieldType latestFieldType = getFieldTypeByIdWithoutCache(fieldType.getId());
            // don't modify input object
            FieldType newFieldType = fieldType.clone();
            copyUnspecifiedFields(newFieldType, latestFieldType);
            checkImmutableFieldsCorrespond(newFieldType, latestFieldType);
            // The supplied name was null, so no need to check if it corresponds
            return latestFieldType;
        } else { // if (fieldType.getName() != null) {
            int attempts;
            for (attempts = 0; attempts < 3; attempts++) {
                FieldType existingFieldType = schemaCache.getFieldTypeByNameReturnNull(fieldType.getName());
                if (existingFieldType != null) {
                    // Field types cannot be deleted so there is no possibility that it would have been deleted
                    // in the meantime.
                    FieldType latestFieldType = getFieldTypeByIdWithoutCache(existingFieldType.getId());
                    if (!latestFieldType.getName().equals(fieldType.getName())) {
                        // Between what we got from the cache and from the persistent storage, the name could
                        // be different if the cache was out of date. In such case, we could loop a few times
                        // before giving up, but for now just throwing an exception since this is not expected
                        // to occur much
                        throw new TypeException("Field type create-or-update: id-name mapping in cache different" +
                                " than what is stored. This is not a user error, just retry please.");
                    }
                    // don't modify input object
                    FieldType newFieldType = fieldType.clone();
                    copyUnspecifiedFields(newFieldType, latestFieldType);
                    checkImmutableFieldsCorrespond(newFieldType, latestFieldType);
                    return latestFieldType;
                } else {
                    try {
                        return createFieldType(fieldType);
                    } catch (FieldTypeExistsException e) {
                        // someone created the field type since we checked, we try again
                    }
                }
            }
            throw new TypeException("Field type create-or-update failed after " + attempts +
                    " attempts, toggling between create and exists mode: this should be impossible" +
                    "since field types cannot be deleted.");
        }
    }

    private void checkImmutableFieldsCorrespond(FieldType userFieldType, FieldType latestFieldType)
            throws FieldTypeUpdateException {

        if (!userFieldType.getValueType().equals(latestFieldType.getValueType())) {
            throw new FieldTypeUpdateException("Changing the valueType of a fieldType '" + latestFieldType.getId() +
                    "' (current name: " + latestFieldType.getName() + ") is not allowed; old '" +
                    latestFieldType.getValueType() + "' new '" + userFieldType.getValueType() + "'");
        }

        if (!userFieldType.getScope().equals(latestFieldType.getScope())) {
            throw new FieldTypeUpdateException("Changing the scope of a fieldType '" + latestFieldType.getId() +
                    "' (current name: " + latestFieldType.getName() + ") is not allowed; old '" +
                    latestFieldType.getScope() + "' new '" + userFieldType.getScope() + "'");
        }
    }

    private void copyUnspecifiedFields(FieldType userFieldType, FieldType latestFieldType)
            throws FieldTypeUpdateException {
        if (userFieldType.getScope() == null) {
            userFieldType.setScope(latestFieldType.getScope());
        }
        if (userFieldType.getValueType() == null) {
            userFieldType.setValueType(latestFieldType.getValueType());
        }
    }

    /**
     * Checks if a name for a field type or record type is not being used by some other create or update operation 'at the same time'.
     * This is to avoid that concurrent updates would result in the same name being used for two different types.
     * <p/>
     * <p>A timestamp 'now' is put in a row with nameBytes as key. As long as this timestamp is present and the timeout (concurrentTimeout)
     * has not expired, no other create or update operation can happen with the same name for the type.
     * <p/>
     * <p>When the create or update operation has finished {@link #clearConcurrency(byte[], long)} should be called to clear the timestamp
     * and to allow new updates.
     */
    private void checkConcurrency(QName name, byte[] nameBytes, long now) throws IOException, ConcurrentUpdateTypeException {
        // Get the timestamp of when the last update happened for the field name
        byte[] originalTimestampBytes = null;
        Long originalTimestamp = null;
        Get get = new Get(nameBytes);
        get.addColumn(TypeCf.DATA.bytes, TypeColumn.CONCURRENT_TIMESTAMP.bytes);
        Result result = getTypeTable().get(get);
        if (result != null && !result.isEmpty()) {
            originalTimestampBytes = result.getValue(TypeCf.DATA.bytes, TypeColumn.CONCURRENT_TIMESTAMP.bytes);
            if (originalTimestampBytes != null && originalTimestampBytes.length != 0) {
                originalTimestamp = Bytes.toLong(originalTimestampBytes);
            }
        }
        // Check if the timestamp is older than the concurrent timeout
        // The concurrent timeout should be large enough to allow fieldType caches to be refreshed
        if (originalTimestamp != null) {
            if ((originalTimestamp + CONCURRENT_TIMEOUT) >= now) {
                throw new ConcurrentUpdateTypeException(name.toString());
            }

        }
        // Try to put our own timestamp with a check and put to make sure we're the only one doing this
        Put put = new Put(nameBytes);
        put.add(TypeCf.DATA.bytes, TypeColumn.CONCURRENT_TIMESTAMP.bytes, Bytes.toBytes(now));
        if (!getTypeTable().checkAndPut(nameBytes, TypeCf.DATA.bytes, TypeColumn.CONCURRENT_TIMESTAMP.bytes, originalTimestampBytes, put)) {
            throw new ConcurrentUpdateTypeException(name.toString());
        }
    }

    /**
     * Clears the timestamp from the row with nameBytes as key.
     * <p/>
     * <p>This method should be called when a create or update operation has finished to allow new updates to happen before the concurrent timeout would expire.
     *
     * @param now the timestamp that was used when calling {@link #checkConcurrency(QName, byte[], long)}
     */
    private void clearConcurrency(byte[] nameBytes, long now) {
        Put put = new Put(nameBytes);
        put.add(TypeCf.DATA.bytes, TypeColumn.CONCURRENT_TIMESTAMP.bytes, null);
        try {
            // Using check and put to avoid clearing a timestamp that was not ours.
            getTypeTable().checkAndPut(nameBytes, TypeCf.DATA.bytes, TypeColumn.CONCURRENT_TIMESTAMP.bytes, Bytes.toBytes(now), put);
        } catch (IOException e) {
            // Ignore, too late to clear the timestamp
        }
    }


    private FieldType getFieldTypeByIdWithoutCache(SchemaId id) throws FieldTypeNotFoundException, TypeException, RepositoryException, InterruptedException {
        ArgumentValidator.notNull(id, "id");
        Result result;
        Get get = new Get(id.getBytes());
        try {
            result = getTypeTable().get(get);
            // This covers the case where a given id would match a name that was
            // used for setting the concurrent counters
            if (result == null || result.isEmpty()
                    || result.getValue(TypeCf.DATA.bytes, TypeColumn.FIELDTYPE_NAME.bytes) == null) {
                throw new FieldTypeNotFoundException(id);
            }
        } catch (IOException e) {
            throw new TypeException("Exception occurred while retrieving fieldType '" + id + "' from HBase", e);
        }
        return extractFieldType(id, result);
    }

    private FieldType extractFieldType(SchemaId id, Result result) throws RepositoryException, InterruptedException {
        NavigableMap<byte[], byte[]> nonVersionableColumnFamily = result.getFamilyMap(TypeCf.DATA.bytes);
        QName name;
        name = decodeName(nonVersionableColumnFamily.get(TypeColumn.FIELDTYPE_NAME.bytes));
        ValueType valueType = decodeValueType(nonVersionableColumnFamily.get(TypeColumn.FIELDTYPE_VALUETYPE.bytes));
        Scope scope = Scope.valueOf(Bytes.toString(nonVersionableColumnFamily.get(TypeColumn.FIELDTYPE_SCOPE.bytes)));
        return new FieldTypeImpl(id, valueType, name, scope);
    }

    @Override
    public List<FieldType> getFieldTypesWithoutCache() throws FieldTypeNotFoundException,
            TypeException, RepositoryException, InterruptedException {
        List<FieldType> fieldTypes = new ArrayList<FieldType>();
        ResultScanner scanner;
        try {
            Scan scan = new Scan();
            scan.setCaching(1000);
            scan.addColumn(TypeCf.DATA.bytes, TypeColumn.FIELDTYPE_NAME.bytes);
            scan.addColumn(TypeCf.DATA.bytes, TypeColumn.FIELDTYPE_VALUETYPE.bytes);
            scan.addColumn(TypeCf.DATA.bytes, TypeColumn.FIELDTYPE_SCOPE.bytes);
            scanner = getTypeTable().getScanner(scan);
        } catch (IOException e) {
            throw new TypeException("Exception occurred while retrieving field types without cache ", e);
        }
        for (Result result : scanner) {
            fieldTypes.add(extractFieldType(new SchemaIdImpl(result.getRow()), result));
        }
        Closer.close(scanner);

        return fieldTypes;
    }

    @Override
    public List<RecordType> getRecordTypesWithoutCache() throws TypeException {
        List<RecordType> recordTypes = new ArrayList<RecordType>();
        ResultScanner scanner;
        try {
            Scan scan = new Scan();
            scan.setCaching(1000);
            scan.addColumn(TypeCf.DATA.bytes, TypeColumn.RECORDTYPE_NAME.bytes);
            scan.addColumn(TypeCf.DATA.bytes, TypeColumn.VERSION.bytes);
            scan.addFamily(TypeCf.FIELDTYPE_ENTRY.bytes);
            scan.addFamily(TypeCf.MIXIN.bytes);

            scanner = getTypeTable().getScanner(scan);
        } catch (IOException e) {
            throw new TypeException("Exception occurred while retrieving record types without cache ", e);
        }
        for (Result result : scanner) {
            recordTypes.add(extractRecordType(new SchemaIdImpl(result.getRow()), null, result));
        }
        Closer.close(scanner);

        return recordTypes;
    }

    @Override
    public Pair<List<FieldType>, List<RecordType>> getTypesWithoutCache() throws RepositoryException,
            InterruptedException {
        List<FieldType> fieldTypes = new ArrayList<FieldType>();
        List<RecordType> recordTypes = new ArrayList<RecordType>();

        ResultScanner scanner;

        Scan scan = new Scan();
        scan.setCaching(1000);
        // Field type columns
        scan.addColumn(TypeCf.DATA.bytes, TypeColumn.FIELDTYPE_NAME.bytes);
        scan.addColumn(TypeCf.DATA.bytes, TypeColumn.FIELDTYPE_VALUETYPE.bytes);
        scan.addColumn(TypeCf.DATA.bytes, TypeColumn.FIELDTYPE_SCOPE.bytes);
        // Record type columns
        scan.addColumn(TypeCf.DATA.bytes, TypeColumn.RECORDTYPE_NAME.bytes);
        scan.addColumn(TypeCf.DATA.bytes, TypeColumn.VERSION.bytes);
        scan.addFamily(TypeCf.FIELDTYPE_ENTRY.bytes);
        scan.addFamily(TypeCf.MIXIN.bytes);

        try {
            scanner = getTypeTable().getScanner(scan);
        } catch (IOException e) {
            throw new TypeException("Exception occurred while retrieving field types and record types without cache ",
                    e);
        }

        // Collect the results first and close the scanner as fast as possible
        List<Result> results = new ArrayList<Result>();
        for (Result scanResult : scanner) {
            // Skip empty results from the scanner
            if (scanResult != null && !scanResult.isEmpty()) {
                results.add(scanResult);
            }
        }
        Closer.close(scanner);

        // Now extract the record and field types from the results
        for (Result scanResult : results) {
            if (scanResult.getValue(TypeCf.DATA.bytes, TypeColumn.FIELDTYPE_NAME.bytes) != null) {
                fieldTypes.add(extractFieldType(new SchemaIdImpl(scanResult.getRow()), scanResult));
            } else if (scanResult.getValue(TypeCf.DATA.bytes, TypeColumn.RECORDTYPE_NAME.bytes) != null) {
                recordTypes.add(extractRecordType(new SchemaIdImpl(scanResult.getRow()), null, scanResult));
            }
        }
        return new Pair<List<FieldType>, List<RecordType>>(fieldTypes, recordTypes);
    }

    @Override
    public TypeBucket getTypeBucketWithoutCache(String bucketId) throws RepositoryException, InterruptedException {
        Scan scan = new Scan();
        // With 20000 types, each bucket will have around 80 rows
        // Putting the cache size to 50 or 100 seems to slow things down
        // significantly.
        scan.setCaching(10);

        // Field type columns
        scan.addColumn(TypeCf.DATA.bytes, TypeColumn.FIELDTYPE_NAME.bytes);
        scan.addColumn(TypeCf.DATA.bytes, TypeColumn.FIELDTYPE_VALUETYPE.bytes);
        scan.addColumn(TypeCf.DATA.bytes, TypeColumn.FIELDTYPE_SCOPE.bytes);
        // Record type columns
        scan.addColumn(TypeCf.DATA.bytes, TypeColumn.RECORDTYPE_NAME.bytes);
        scan.addColumn(TypeCf.DATA.bytes, TypeColumn.VERSION.bytes);
        scan.addFamily(TypeCf.FIELDTYPE_ENTRY.bytes);
        scan.addFamily(TypeCf.MIXIN.bytes);

        TypeBucket typeBucket = new TypeBucket(bucketId);
        ResultScanner scanner = null;
        byte[] rowPrefix = AbstractSchemaCache.decodeHexAndNextHex(bucketId);
        scan.setStartRow(new byte[]{rowPrefix[0]});
        if (!bucketId.equals("ff")) // In case of ff, just scan until
        // the end
        {
            scan.setStopRow(new byte[]{rowPrefix[1]});
        }
        try {
            scanner = getTypeTable().getScanner(scan);
        } catch (IOException e) {
            throw new TypeException("Exception occurred while retrieving field types and record types without cache ",
                    e);
        }

        // Collect the results first and close the scanner as fast as possible
        List<Result> results = new ArrayList<Result>();
        for (Result scanResult : scanner) {
            // Skip empty results from the scanner
            if (scanResult != null && !scanResult.isEmpty()) {
                results.add(scanResult);
            }
        }
        Closer.close(scanner);

        // Now extract the record and field types from the results
        for (Result scanResult : results) {
            if (scanResult.getValue(TypeCf.DATA.bytes, TypeColumn.FIELDTYPE_NAME.bytes) != null) {
                typeBucket.add(extractFieldType(new SchemaIdImpl(scanResult.getRow()), scanResult));
            } else if (scanResult.getValue(TypeCf.DATA.bytes, TypeColumn.RECORDTYPE_NAME.bytes) != null) {
                typeBucket.add(extractRecordType(new SchemaIdImpl(scanResult.getRow()), null, scanResult));
            }
        }
        return typeBucket;
    }

    public static byte[] encodeName(QName qname) {
        String name = qname.getName();
        String namespace = qname.getNamespace();

        int sizeEstimate = (((name == null) ? 1 : (name.length() * 2)) + ((namespace == null) ? 1 : (namespace.length() * 2)));
        DataOutput dataOutput = new DataOutputImpl(sizeEstimate);

        dataOutput.writeUTF(namespace);
        dataOutput.writeUTF(name);
        return dataOutput.toByteArray();
    }

    public static QName decodeName(byte[] bytes) {
        DataInput dataInput = new DataInputImpl(bytes);
        String namespace = dataInput.readUTF();
        String name = dataInput.readUTF();
        return new QName(namespace, name);
    }

    /**
     * Generates a SchemaId based on a uuid and checks if it's not already in use
     */
    private SchemaId getValidId() throws IOException {
        SchemaId id = new SchemaIdImpl(UUID.randomUUID());
        byte[] rowId = id.getBytes();
        // The chance it would already exist is small
        if (typeTable.exists(new Get(rowId))) {
            return getValidId();
        }
        // The chance a same uuid is generated after doing the exists check is
        // even smaller
        // If it would still happen, the incrementColumnValue would return a
        // number bigger than 1
        if (1L != typeTable
                .incrementColumnValue(rowId, TypeCf.DATA.bytes, TypeColumn.CONCURRENT_COUNTER.bytes, 1L)) {
            return getValidId();
        }
        return id;
    }

    protected HTableInterface getTypeTable() {
        return typeTable;
    }

    // ValueType encoding
    public static final byte valueTypeEncodingVersion = (byte)1;

    public static byte[] encodeValueType(ValueType valueType) {
        return encodeValueType(valueType.getName());
    }

    public static byte[] encodeValueType(String valueTypeName) {
        DataOutput dataOutput = new DataOutputImpl();
        dataOutput.writeByte(valueTypeEncodingVersion);
        dataOutput.writeUTF(valueTypeName);
        return dataOutput.toByteArray();
    }

    private ValueType decodeValueType(byte[] bytes) throws RepositoryException, InterruptedException {
        DataInput dataInput = new DataInputImpl(bytes);
        if (valueTypeEncodingVersion != dataInput.readByte()) {
            throw new TypeException("Unknown value type encoding version encountered in schema");
        }

        return getValueType(dataInput.readUTF());
    }

    @Override
    public void enableSchemaCacheRefresh() throws TypeException, InterruptedException {
        ((LocalSchemaCache)schemaCache).enableSchemaCacheRefresh();
    }

    @Override
    public void disableSchemaCacheRefresh() throws TypeException, InterruptedException {
        ((LocalSchemaCache)schemaCache).disableSchemaCacheRefresh();
    }

    @Override
    public void triggerSchemaCacheRefresh() throws TypeException, InterruptedException {
        ((LocalSchemaCache)schemaCache).triggerRefresh();
    }

    @Override
    public boolean isSchemaCacheRefreshEnabled() {
        return ((LocalSchemaCache)schemaCache).isRefreshEnabled();
    }

    public TypeManagerMBean getMBean() {
        return new TypeManagerMBeanImpl();
    }

    public class TypeManagerMBeanImpl implements TypeManagerMBean {
        @Override
        public void enableSchemaCacheRefresh() throws TypeException, InterruptedException {
            HBaseTypeManager.this.enableSchemaCacheRefresh();
        }

        @Override
        public void disableSchemaCacheRefresh() throws TypeException, InterruptedException {
            HBaseTypeManager.this.disableSchemaCacheRefresh();
        }

        @Override
        public void triggerSchemaCacheRefresh() throws TypeException, InterruptedException {
            HBaseTypeManager.this.triggerSchemaCacheRefresh();
        }

        @Override
        public boolean isSchemaCacheRefreshEnabled() {
            return HBaseTypeManager.this.isSchemaCacheRefreshEnabled();
        }
    }
}
