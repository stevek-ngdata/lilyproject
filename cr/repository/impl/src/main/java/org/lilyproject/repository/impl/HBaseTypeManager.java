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
import org.lilyproject.repository.api.*;
import org.lilyproject.util.ArgumentValidator;
import org.lilyproject.util.hbase.HBaseTableFactory;
import org.lilyproject.util.hbase.LilyHBaseSchema;
import org.lilyproject.util.hbase.LilyHBaseSchema.TypeCf;
import org.lilyproject.util.hbase.LilyHBaseSchema.TypeColumn;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

public class HBaseTypeManager extends AbstractTypeManager implements TypeManager {

    private HTableInterface typeTable;

    // TODO this is temporary, should be removed once the reason mentioned in TypeManagerMBean is resolved
    private boolean cacheInvalidationEnabled = true;

    public HBaseTypeManager(IdGenerator idGenerator, Configuration configuration, ZooKeeperItf zooKeeper, HBaseTableFactory hbaseTableFactory)
            throws IOException, InterruptedException, KeeperException {
        super(zooKeeper);
        log = LogFactory.getLog(getClass());
        this.idGenerator = idGenerator;

        this.typeTable = LilyHBaseSchema.getTypeTable(hbaseTableFactory);
        registerDefaultValueTypes();
        setupCaches();
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
            long concurrentCounter = getTypeTable().incrementColumnValue(nameBytes, TypeCf.DATA.bytes,
                    TypeColumn.CONCURRENT_COUNTER.bytes, 1);

            if (getRecordTypeFromCache(recordType.getName()) != null)
                throw new RecordTypeExistsException(recordType);

            Put put = new Put(rowId);
            put.add(TypeCf.DATA.bytes, TypeColumn.VERSION.bytes, Bytes.toBytes(recordTypeVersion));
            put.add(TypeCf.DATA.bytes, TypeColumn.RECORDTYPE_NAME.bytes, nameBytes);

            Collection<FieldTypeEntry> fieldTypeEntries = recordType.getFieldTypeEntries();
            for (FieldTypeEntry fieldTypeEntry : fieldTypeEntries) {
                putFieldTypeEntry(recordTypeVersion, put, fieldTypeEntry);
            }

            Map<SchemaId, Long> mixins = recordType.getMixins();
            for (Entry<SchemaId, Long> mixin : mixins.entrySet()) {
                newRecordType.addMixin(mixin.getKey(), putMixinOnRecordType(recordTypeVersion, put, mixin.getKey(),
                        mixin.getValue()));
            }

            getTypeTable().put(put);
            /*
            TODO FIXME bogus name reservation code
            if (!getTypeTable().checkAndPut(nameBytes, TypeCf.DATA.bytes, TypeColumn.CONCURRENT_COUNTER.bytes,
                    Bytes.toBytes(concurrentCounter), put)) {
                throw new TypeException("Concurrent create occurred for recordType <" + recordType.getName() + ">");
            }
            */
            newRecordType.setId(id);
            newRecordType.setVersion(recordTypeVersion);
            updateRecordTypeCache(newRecordType.clone());
            notifyCacheInvalidate();
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
            FieldTypeNotFoundException, TypeException {
        ArgumentValidator.notNull(recordType, "recordType");
        RecordType newRecordType = recordType.clone();
        SchemaId id = newRecordType.getId();
        if (id == null) {
            throw new RecordTypeNotFoundException(newRecordType.getName(), null);
        }
        byte[] rowId = id.getBytes();
        try {
            // Do an exists check first and avoid useless creation of the row
            // due to an incrementColumnValue call
            if (!getTypeTable().exists(new Get(rowId))) {
                throw new RecordTypeNotFoundException(recordType.getId(), null);
            }
            // First increment the counter, then read the record type
            byte[] nameBytes = encodeName(recordType.getName());
            long concurrentCount = getTypeTable().incrementColumnValue(nameBytes, TypeCf.DATA.bytes,
                    TypeColumn.CONCURRENT_COUNTER.bytes, 1);
            RecordType latestRecordType = getRecordTypeByIdWithoutCache(id, null);
            Long latestRecordTypeVersion = latestRecordType.getVersion();
            Long newRecordTypeVersion = latestRecordTypeVersion + 1;

            Put put = new Put(rowId);
            boolean fieldTypeEntriesChanged = updateFieldTypeEntries(put, newRecordTypeVersion, newRecordType,
                    latestRecordType);

            boolean mixinsChanged = updateMixins(put, newRecordTypeVersion, newRecordType, latestRecordType);

            boolean nameChanged = updateName(put, recordType, latestRecordType);

            if (fieldTypeEntriesChanged || mixinsChanged || nameChanged) {
                put.add(TypeCf.DATA.bytes, TypeColumn.VERSION.bytes, Bytes.toBytes(newRecordTypeVersion));
                getTypeTable().put(put);
                /*
                TODO FIXME bogus name reservation code
                getTypeTable().checkAndPut(nameBytes, TypeCf.DATA.bytes, TypeColumn.CONCURRENT_COUNTER.bytes,
                        Bytes.toBytes(concurrentCount), put);
                */
                newRecordType.setVersion(newRecordTypeVersion);
            } else {
                newRecordType.setVersion(latestRecordTypeVersion);
            }

            updateRecordTypeCache(newRecordType);
            notifyCacheInvalidate();
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

    private boolean updateName(Put put, RecordType recordType, RecordType latestRecordType) throws TypeException {
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

    public FieldType createFieldType(FieldType fieldType) throws FieldTypeExistsException, TypeException {
        ArgumentValidator.notNull(fieldType, "fieldType");

        FieldType newFieldType;
        Long version = Long.valueOf(1);
        try {
            SchemaId id = getValidId();
            byte[] rowId = id.getBytes();
            // Take a counter on a row with the name as key
            byte[] nameBytes = encodeName(fieldType.getName());
            long concurrentCounter = getTypeTable().incrementColumnValue(nameBytes, TypeCf.DATA.bytes,
                    TypeColumn.CONCURRENT_COUNTER.bytes, 1);

            try {
                if (getFieldTypesSnapshot().getFieldTypeByName(fieldType.getName()) != null)
                    throw new FieldTypeExistsException(fieldType);
            } catch (FieldTypeNotFoundException ignore) {
            }

            Put put = new Put(rowId);
            put.add(TypeCf.DATA.bytes, TypeColumn.FIELDTYPE_VALUETYPE.bytes, fieldType.getValueType().toBytes());
            put.add(TypeCf.DATA.bytes, TypeColumn.FIELDTYPE_SCOPE.bytes, Bytes
                    .toBytes(fieldType.getScope().name()));
            put.add(TypeCf.DATA.bytes, TypeColumn.FIELDTYPE_NAME.bytes, nameBytes);

            getTypeTable().put(put);

            /*
            TODO FIXME bogus name reservation code
            if (!getTypeTable().checkAndPut(nameBytes, TypeCf.DATA.bytes, TypeColumn.CONCURRENT_COUNTER.bytes,
                    Bytes.toBytes(concurrentCounter), put)) {
                throw new TypeException("Concurrent create occurred for fieldType <" + fieldType.getName() + ">");
            }
            */
            newFieldType = fieldType.clone();
            newFieldType.setId(id);
            updateFieldTypeCache(newFieldType);
            notifyCacheInvalidate();
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
            // Do an exists check first and avoid useless creation of the row
            // due to an incrementColumnValue call
            if (!getTypeTable().exists(new Get(rowId))) {
                throw new FieldTypeNotFoundException(fieldType.getId());
            }
            // First increment the counter on the row with the name as key, then
            // read the field type
            byte[] nameBytes = encodeName(fieldType.getName());
            long concurrentCounter = getTypeTable().incrementColumnValue(nameBytes, TypeCf.DATA.bytes,
                    TypeColumn.CONCURRENT_COUNTER.bytes, 1);
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
                Put put = new Put(rowId);
                put.add(TypeCf.DATA.bytes, TypeColumn.FIELDTYPE_NAME.bytes, nameBytes);

                getTypeTable().put(put);

                /*
                TODO FIXME bogus name reservation code
                getTypeTable().checkAndPut(nameBytes, TypeCf.DATA.bytes, TypeColumn.CONCURRENT_COUNTER.bytes,
                        Bytes.toBytes(concurrentCounter), put);
                */
            }
            updateFieldTypeCache(fieldType);
            notifyCacheInvalidate();
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

    private byte[] encodeName(QName qname) {
        byte[] encodedName = new byte[0];
        String name = qname.getName();
        String namespace = qname.getNamespace();

        if (namespace == null) {
            encodedName = Bytes.add(encodedName, Bytes.toBytes(0));
        } else {
            byte[] namespaceBytes = Bytes.toBytes(namespace);
            encodedName = Bytes.add(encodedName, Bytes.toBytes(namespaceBytes.length));
            encodedName = Bytes.add(encodedName, namespaceBytes);
        }
        byte[] nameBytes = Bytes.toBytes(name);
        encodedName = Bytes.add(encodedName, Bytes.toBytes(nameBytes.length));
        encodedName = Bytes.add(encodedName, nameBytes);
        return encodedName;
    }

    private QName decodeName(byte[] bytes) {
        int offset = 0;
        String namespace = null;
        int namespaceLength = Bytes.toInt(bytes);
        offset = offset + Bytes.SIZEOF_INT;
        if (namespaceLength > 0) {
            namespace = Bytes.toString(bytes, offset, namespaceLength);
        }
        offset = offset + namespaceLength;
        int nameLength = Bytes.toInt(bytes, offset, Bytes.SIZEOF_INT);
        offset = offset + Bytes.SIZEOF_INT;
        String name = Bytes.toString(bytes, offset, nameLength);
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
