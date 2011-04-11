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

import static org.lilyproject.util.hbase.LilyHBaseSchema.DELETE_MARKER;
import static org.lilyproject.util.hbase.LilyHBaseSchema.EXISTS_FLAG;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.UUID;
import java.util.Map.Entry;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;
import org.lilyproject.bytes.api.DataInput;
import org.lilyproject.bytes.api.DataOutput;
import org.lilyproject.bytes.impl.DataInputImpl;
import org.lilyproject.bytes.impl.DataOutputImpl;
import org.lilyproject.repository.api.*;
import org.lilyproject.util.ArgumentValidator;
import org.lilyproject.util.hbase.HBaseTableFactory;
import org.lilyproject.util.hbase.LilyHBaseSchema;
import org.lilyproject.util.hbase.LilyHBaseSchema.TypeCf;
import org.lilyproject.util.hbase.LilyHBaseSchema.TypeColumn;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.repo.VersionTag;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

public class HBaseTypeManager extends AbstractTypeManager implements TypeManager {

    private static final Long CONCURRENT_TIMEOUT = 5000L; // The concurrent timeout should be large enough to allow for type caches to be refreshed an a clock skew between the servers

    private HTableInterface typeTable;

    // TODO this is temporary, should be removed once the reason mentioned in TypeManagerMBean is resolved
    private boolean cacheInvalidationEnabled = true;

    public HBaseTypeManager(IdGenerator idGenerator, Configuration configuration, ZooKeeperItf zooKeeper, HBaseTableFactory hbaseTableFactory)
            throws IOException, InterruptedException, KeeperException, RepositoryException {
        super(zooKeeper);
        log = LogFactory.getLog(getClass());
        this.idGenerator = idGenerator;

        this.typeTable = LilyHBaseSchema.getTypeTable(hbaseTableFactory);
        registerDefaultValueTypes();
        setupCaches();

        // The 'last' vtag should always exist in the system (at least, for everything index-related). Therefore we
        // create it here.
        try {
            FieldType fieldType = newFieldType(getValueType("LONG"), VersionTag.LAST, Scope.NON_VERSIONED);
            createFieldType(fieldType);
        } catch (FieldTypeExistsException e) {
            // ok
        }
    }
    
    protected void cacheInvalidationReconnected() throws InterruptedException {
        super.cacheInvalidationReconnected();
        try {
            // A previous notify might have failed because of a disconnection
            // To be sure we try to send a notify again
            notifyCacheInvalidate();
        } catch (KeeperException e) {
            log.info("Exception occurred while sending a cache invalidation notification after reconnecting to zookeeper");
        }
    }
    
    // Only invalidate the cache from the server-side
    // Updates from the RemoteTypeManager will have to pass through (server-side) HBaseTypeManager anyway
    private void notifyCacheInvalidate() throws KeeperException, InterruptedException {
        if (cacheInvalidationEnabled) {
            zooKeeper.setData(CACHE_INVALIDATION_PATH, null, -1);
        }
    }

    public RecordType createRecordType(RecordType recordType) throws RecordTypeExistsException,
            RecordTypeNotFoundException, FieldTypeNotFoundException, TypeException {
        ArgumentValidator.notNull(recordType, "recordType");
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
            
            if (getRecordTypeFromCache(recordType.getName()) != null)
                throw new RecordTypeExistsException(recordType);

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
            updateRecordTypeCache(newRecordType.clone());
            notifyCacheInvalidate();
            
            // Clear the concurrency timestamp
            clearConcurrency(nameBytes, now);
        } catch (IOException e) {
            throw new TypeException("Exception occurred while creating recordType <" + recordType.getName()
                    + "> on HBase", e);
        } catch (KeeperException e) {
            throw new TypeException("Exception occurred while creating recordType <" + recordType.getName()
                    + "> on HBase", e);
        } catch (InterruptedException e) {
            throw new TypeException("Exception occurred while creating recordType <" + recordType.getName()
                    + "> on HBase", e);
        }
        return newRecordType;
    }

    private Long putMixinOnRecordType(Long recordTypeVersion, Put put, SchemaId mixinId, Long mixinVersion)
            throws RecordTypeNotFoundException, TypeException {
        Long newMixinVersion = getRecordTypeByIdWithoutCache(mixinId, mixinVersion).getVersion();
        put.add(TypeCf.MIXIN.bytes, mixinId.getBytes(), recordTypeVersion, Bytes.toBytes(newMixinVersion));
        return newMixinVersion;
    }

    public RecordType updateRecordType(RecordType recordType) throws RecordTypeNotFoundException,
            FieldTypeNotFoundException, TypeException, RepositoryException, InterruptedException {
        ArgumentValidator.notNull(recordType, "recordType");
        RecordType newRecordType = recordType.clone();
        SchemaId id = newRecordType.getId();
        if (id == null) {
            throw new RecordTypeNotFoundException(newRecordType.getName(), null);
        }
        byte[] rowId = id.getBytes();
        try {

            // Do an exists check first 
            if (!getTypeTable().exists(new Get(rowId))) {
                throw new RecordTypeNotFoundException(recordType.getId(), null);
            }
            
            byte[] nameBytes = encodeName(recordType.getName());

            // Check for concurrency
            long now = System.currentTimeMillis();
            checkConcurrency(recordType.getName(), nameBytes, now);
            
            // Prepare the update
            RecordType latestRecordType = getRecordTypeByIdWithoutCache(id, null);
            Long latestRecordTypeVersion = latestRecordType.getVersion();
            Long newRecordTypeVersion = latestRecordTypeVersion + 1;

            Put put = new Put(rowId);
            boolean fieldTypeEntriesChanged = updateFieldTypeEntries(put, newRecordTypeVersion, newRecordType,
                    latestRecordType);

            boolean mixinsChanged = updateMixins(put, newRecordTypeVersion, newRecordType, latestRecordType);

            boolean nameChanged = updateName(put, recordType, latestRecordType);

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
            notifyCacheInvalidate();
            
            // Clear the concurrency timestamp
            clearConcurrency(nameBytes, now);
        } catch (IOException e) {
            throw new TypeException("Exception occurred while updating recordType <" + newRecordType.getId()
                    + "> on HBase", e);
        } catch (KeeperException e) {
            throw new TypeException("Exception occurred while updating recordType <" + newRecordType.getId()
                    + "> on HBase", e);
        } catch (InterruptedException e) {
            throw new TypeException("Exception occurred while updating recordType <" + newRecordType.getId()
                    + "> on HBase", e);
        }
        return newRecordType;
    }

    private boolean updateName(Put put, RecordType recordType, RecordType latestRecordType) throws TypeException, RepositoryException, InterruptedException {
        if (!recordType.getName().equals(latestRecordType.getName())) {
            try {
                getRecordTypeByName(recordType.getName(), null);
                throw new TypeException("Changing the name <" + recordType.getName() + "> of a recordType <"
                        + recordType.getId() + "> to a name that already exists is not allowed; old<"
                        + latestRecordType.getName() + "> new<" + recordType.getName() + ">");
            } catch (RecordTypeNotFoundException allowed) {
            }
            put.add(TypeCf.DATA.bytes, TypeColumn.RECORDTYPE_NAME.bytes, encodeName(recordType.getName()));
            return true;
        }
        return false;
    }

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
            if (result == null || result.isEmpty() || result.getValue(TypeCf.DATA.bytes, TypeColumn.VERSION.bytes) == null) {
                throw new RecordTypeNotFoundException(id, null);
            }
        } catch (IOException e) {
            throw new TypeException("Exception occurred while retrieving recordType <" + id + "> from HBase table", e);
        }
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
            throw new TypeException("Exception occurred while checking existance of FieldTypeEntry <"
                    + fieldTypeEntry.getFieldTypeId() + "> on HBase", e);
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

    public FieldType createFieldType(FieldType fieldType) throws FieldTypeExistsException, TypeException {
        ArgumentValidator.notNull(fieldType, "fieldType");

        FieldType newFieldType;
        Long version = Long.valueOf(1);
        try {
            SchemaId id = getValidId();
            byte[] rowId = id.getBytes();
            byte[] nameBytes = encodeName(fieldType.getName());

            // Prepare the put
            Put put = new Put(rowId);
            put.add(TypeCf.DATA.bytes, TypeColumn.FIELDTYPE_VALUETYPE.bytes, fieldType.getValueType().toBytes());
            put.add(TypeCf.DATA.bytes, TypeColumn.FIELDTYPE_SCOPE.bytes, Bytes
                    .toBytes(fieldType.getScope().name()));
            put.add(TypeCf.DATA.bytes, TypeColumn.FIELDTYPE_NAME.bytes, nameBytes);
            
            // Prepare newFieldType
            newFieldType = fieldType.clone();
            newFieldType.setId(id);

            // Check for concurrency
            long now = System.currentTimeMillis();
            checkConcurrency(fieldType.getName(), nameBytes, now);
            
            // Check if there is already a fieldType with this name 
            try {
                if (getFieldTypesSnapshot().getFieldTypeByName(fieldType.getName()) != null)
                    throw new FieldTypeExistsException(fieldType);
            } catch (FieldTypeNotFoundException ignore) {
            }

            // Create the actual field type
            getTypeTable().put(put);

            // Refresh the caches
            updateFieldTypeCache(newFieldType);
            notifyCacheInvalidate();
            
            // Clear the concurrency timestamp
            clearConcurrency(nameBytes, now);
        } catch (IOException e) {
            throw new TypeException("Exception occurred while creating fieldType <" + fieldType.getName()
                    + "> version: <" + version + "> on HBase", e);
        } catch (KeeperException e) {
            throw new TypeException("Exception occurred while creating fieldType <" + fieldType.getName()
                    + "> version: <" + version + "> on HBase", e);
        } catch (InterruptedException e) {
            throw new TypeException("Exception occurred while creating fieldType <" + fieldType.getName()
                    + "> version: <" + version + "> on HBase", e);
        }
        return newFieldType;
    }

    public FieldType updateFieldType(FieldType fieldType) throws FieldTypeNotFoundException, FieldTypeUpdateException,
            TypeException {
        byte[] rowId = fieldType.getId().getBytes();
        try {
            // Do an exists check first 
            if (!getTypeTable().exists(new Get(rowId))) {
                throw new FieldTypeNotFoundException(fieldType.getId());
            }
            // First increment the counter on the row with the name as key, then
            // read the field type
            byte[] nameBytes = encodeName(fieldType.getName());
            
            // Check for concurrency
            long now = System.currentTimeMillis();
            checkConcurrency(fieldType.getName(), nameBytes, now);
            
            // Prepare the update
            FieldType latestFieldType = getFieldTypeByIdWithoutCache(fieldType.getId());
            if (!fieldType.getValueType().equals(latestFieldType.getValueType())) {
                throw new FieldTypeUpdateException("Changing the valueType of a fieldType <" + fieldType.getId()
                        + "> is not allowed; old<" + latestFieldType.getValueType() + "> new<"
                        + fieldType.getValueType() + ">");
            }
            if (!fieldType.getScope().equals(latestFieldType.getScope())) {
                throw new FieldTypeUpdateException("Changing the scope of a fieldType <" + fieldType.getId()
                        + "> is not allowed; old<" + latestFieldType.getScope() + "> new<" + fieldType.getScope() + ">");
            }
            if (!fieldType.getName().equals(latestFieldType.getName())) {
                try {
                    getFieldTypeByName(fieldType.getName());
                    throw new FieldTypeUpdateException("Changing the name <" + fieldType.getName()
                            + "> of a fieldType <" + fieldType.getId()
                            + "> to a name that already exists is not allowed; old<" + latestFieldType.getName()
                            + "> new<" + fieldType.getName() + ">");
                } catch (FieldTypeNotFoundException allowed) {
                }
                // Update the field type on the table
                Put put = new Put(rowId);
                put.add(TypeCf.DATA.bytes, TypeColumn.FIELDTYPE_NAME.bytes, nameBytes);

                getTypeTable().put(put);
            }
            
            // Refresh the caches
            updateFieldTypeCache(fieldType);
            notifyCacheInvalidate();
            
            // Clear the concurrency timestamp
            clearConcurrency(nameBytes, now);
        } catch (IOException e) {
            throw new TypeException("Exception occurred while updating fieldType <" + fieldType.getId() + "> on HBase",
                    e);
        } catch (KeeperException e) {
            throw new TypeException("Exception occurred while updating fieldType <" + fieldType.getId() + "> on HBase",
                    e);
        } catch (InterruptedException e) {
            throw new TypeException("Exception occurred while updating fieldType <" + fieldType.getId() + "> on HBase",
                    e);
        }
        return fieldType.clone();
    }

    /**
     * Checks if a name for a field type or record type is not being used by some other create or update operation 'at the same time'.
     * This is to avoid that concurrent updates would result in the same name being used for two different types.
     * 
     *  <p>A timestamp 'now' is put in a row with nameBytes as key. As long as this timestamp is present and the timeout (concurrentTimeout)
     *  has not expired, no other create or update operation can happen with the same name for the type.
     *  
     *  <p>When the create or update operation has finished {@link #clearConcurrency(byte[], long)} should be called to clear the timestamp
     *  and to allow new updates.
     */
    private void checkConcurrency(QName name, byte[] nameBytes, long now) throws IOException, TypeException {
        // Get the timestamp of when the last update happened for the field name
        byte[] originalTimestampBytes = null;
        Long originalTimestamp = null;
        Get get = new Get(nameBytes);
        get.addColumn(TypeCf.DATA.bytes, TypeColumn.CONCURRENT_TIMESTAMP.bytes);
        Result result = getTypeTable().get(get);
        if (result != null && !result.isEmpty()) {
            originalTimestampBytes = result.getValue(TypeCf.DATA.bytes, TypeColumn.CONCURRENT_TIMESTAMP.bytes);
            if (originalTimestampBytes != null && originalTimestampBytes.length != 0)
                originalTimestamp = Bytes.toLong(originalTimestampBytes);
        }
        // Check if the timestamp is older than the concurrent timeout
        // The concurrent timeout should be large enough to allow fieldType caches to be refreshed
        if (originalTimestamp != null) {
            if ((originalTimestamp + CONCURRENT_TIMEOUT) >= now) {
                throw new TypeException("Concurrent create or update occurred for field or record type <" + name + ">");
            }
            
        }
        // Try to put our own timestamp with a check and put to make sure we're the only one doing this
        Put put = new Put(nameBytes);
        put.add(TypeCf.DATA.bytes, TypeColumn.CONCURRENT_TIMESTAMP.bytes, Bytes.toBytes(now));
        if (!getTypeTable().checkAndPut(nameBytes, TypeCf.DATA.bytes, TypeColumn.CONCURRENT_TIMESTAMP.bytes, originalTimestampBytes, put)) {
            throw new TypeException("Concurrent create or update occurred for field or record type <" + name + ">");
        }
    }
    
    /**
     * Clears the timestamp from the row with nameBytes as key.
     * 
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

    
    private FieldType getFieldTypeByIdWithoutCache(SchemaId id) throws FieldTypeNotFoundException, TypeException {
        ArgumentValidator.notNull(id, "id");
        Result result;
        Get get = new Get(id.getBytes());
        try {
            result = getTypeTable().get(get);
            // This covers the case where a given id would match a name that was
            // used for setting the concurrent counters
            if (result == null || result.isEmpty() || result.getValue(TypeCf.DATA.bytes, TypeColumn.FIELDTYPE_NAME.bytes) == null) {
                throw new FieldTypeNotFoundException(id);
            }
        } catch (IOException e) {
            throw new TypeException("Exception occurred while retrieving fieldType <" + id + "> from HBase", e);
        }
        NavigableMap<byte[], byte[]> nonVersionableColumnFamily = result.getFamilyMap(TypeCf.DATA.bytes);
        QName name;
        name = decodeName(nonVersionableColumnFamily.get(TypeColumn.FIELDTYPE_NAME.bytes));
        ValueType valueType = ValueTypeImpl.fromBytes(nonVersionableColumnFamily.get(TypeColumn.FIELDTYPE_VALUETYPE.bytes),
                this);
        Scope scope = Scope.valueOf(Bytes.toString(nonVersionableColumnFamily.get(TypeColumn.FIELDTYPE_SCOPE.bytes)));
        return new FieldTypeImpl(id, valueType, name, scope);
    }

    public List<FieldType> getFieldTypesWithoutCache() throws FieldTypeNotFoundException,
            TypeException {
        List<FieldType> fieldTypes = new ArrayList<FieldType>();
        ResultScanner scanner = null;
        try {
            scanner = getTypeTable().getScanner(TypeCf.DATA.bytes, TypeColumn.FIELDTYPE_NAME.bytes);
            for (Result result : scanner) {
                FieldType fieldType = getFieldTypeByIdWithoutCache(new SchemaIdImpl(result.getRow()));
                fieldTypes.add(fieldType);
            }
        } catch (IOException e) {
            throw new TypeException("Exception occurred while retrieving field types without cache ", e);
        } finally {
            Closer.close(scanner);
        }
        return fieldTypes;
    }

    public List<RecordType> getRecordTypesWithoutCache() throws RecordTypeNotFoundException,
            TypeException {
        List<RecordType> recordTypes = new ArrayList<RecordType>();
        ResultScanner scanner = null;
        try {
            scanner = getTypeTable().getScanner(TypeCf.DATA.bytes, TypeColumn.RECORDTYPE_NAME.bytes);
            for (Result result : scanner) {
                RecordType recordType = getRecordTypeByIdWithoutCache(new SchemaIdImpl(result.getRow()), null);
                recordTypes.add(recordType);
            }
        } catch (IOException e) {
            throw new TypeException("Exception occurred while retrieving record types without cache ", e);
        } finally {
            Closer.close(scanner);
        }
        return recordTypes;
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
        if (typeTable.exists(new Get(rowId)))
            return getValidId();
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

    // TODO this is temporary, should be removed once the reason mentioned in TypeManagerMBean is resolved
    public TypeManagerMBean getMBean() {
        return new TypeManagerMBeanImpl();
    }

    // TODO this is temporary, should be removed once the reason mentioned in TypeManagerMBean is resolved
    public class TypeManagerMBeanImpl implements TypeManagerMBean {
        public void enableCacheInvalidationTrigger() {
            cacheInvalidationEnabled = true;
        }

        public void disableCacheInvalidationTrigger() {
            cacheInvalidationEnabled = false;
        }

        public void notifyCacheInvalidate() throws Exception {
            HBaseTypeManager.this.notifyCacheInvalidate();
        }

        public boolean isCacheInvalidationTriggerEnabled() {
            return cacheInvalidationEnabled;
        }
    }
}
