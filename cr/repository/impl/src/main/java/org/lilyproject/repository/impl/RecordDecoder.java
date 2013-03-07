/*
 * Copyright 2012 NGDATA nv
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

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilyproject.bytes.impl.DataInputImpl;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.FieldTypes;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.IdRecord;
import org.lilyproject.repository.api.Metadata;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.api.ValueType;
import org.lilyproject.repository.impl.id.SchemaIdImpl;
import org.lilyproject.util.Pair;
import org.lilyproject.util.hbase.LilyHBaseSchema;

import static org.lilyproject.util.hbase.LilyHBaseSchema.RecordCf;
import static org.lilyproject.util.hbase.LilyHBaseSchema.RecordColumn;

/**
 * Methods related to decoding HBase Result objects into Lily Record objects.
 *
 * <p>The methods in this class assume they are supplied with non-deleted records, thus where the
 * {@link LilyHBaseSchema.RecordColumn#DELETED} flag is false.</p>
 */
public class RecordDecoder {
    public static final Map<Scope, byte[]> RECORD_TYPE_ID_QUALIFIERS = new EnumMap<Scope, byte[]>(Scope.class);
    public static final Map<Scope, byte[]> RECORD_TYPE_VERSION_QUALIFIERS = new EnumMap<Scope, byte[]>(Scope.class);

    static {
        RECORD_TYPE_ID_QUALIFIERS.put(Scope.NON_VERSIONED, RecordColumn.NON_VERSIONED_RT_ID.bytes);
        RECORD_TYPE_ID_QUALIFIERS.put(Scope.VERSIONED, RecordColumn.VERSIONED_RT_ID.bytes);
        RECORD_TYPE_ID_QUALIFIERS.put(Scope.VERSIONED_MUTABLE, RecordColumn.VERSIONED_MUTABLE_RT_ID.bytes);

        RECORD_TYPE_VERSION_QUALIFIERS.put(Scope.NON_VERSIONED, RecordColumn.NON_VERSIONED_RT_VERSION.bytes);
        RECORD_TYPE_VERSION_QUALIFIERS.put(Scope.VERSIONED, RecordColumn.VERSIONED_RT_VERSION.bytes);
        RECORD_TYPE_VERSION_QUALIFIERS.put(Scope.VERSIONED_MUTABLE, RecordColumn.VERSIONED_MUTABLE_RT_VERSION.bytes);
    }

    public static final List<byte[]> SYSTEM_FIELDS = new ArrayList<byte[]>();

    static {
        SYSTEM_FIELDS.add(RecordColumn.OCC.bytes);
        SYSTEM_FIELDS.add(RecordColumn.DELETED.bytes);
        SYSTEM_FIELDS.add(RecordColumn.VERSION.bytes);
        SYSTEM_FIELDS.add(RecordColumn.NON_VERSIONED_RT_ID.bytes);
        SYSTEM_FIELDS.add(RecordColumn.NON_VERSIONED_RT_VERSION.bytes);
        SYSTEM_FIELDS.add(RecordColumn.VERSIONED_RT_ID.bytes);
        SYSTEM_FIELDS.add(RecordColumn.VERSIONED_RT_VERSION.bytes);
        SYSTEM_FIELDS.add(RecordColumn.VERSIONED_MUTABLE_RT_ID.bytes);
        SYSTEM_FIELDS.add(RecordColumn.VERSIONED_MUTABLE_RT_VERSION.bytes);
    }

    private TypeManager typeManager;
    private IdGenerator idGenerator;

    public RecordDecoder(TypeManager typeManager, IdGenerator idGenerator) {
        this.typeManager = typeManager;
        this.idGenerator = idGenerator;
    }

    /**
     * Decode the given HBase result into a Lily Record assuming it contains the state of the latest version of
     * the record (if the record has any versions, otherwise it only contains non-versioned fields).
     */
    public Record decodeRecord(Result result) throws InterruptedException, RepositoryException {
        Long latestVersion = getLatestVersion(result);
        RecordId recordId = idGenerator.fromBytes(result.getRow());
        return decodeRecord(recordId, latestVersion, null, result, typeManager.getFieldTypesSnapshot());
    }

    /**
     * Gets the requested version of the record (fields and recordTypes) from the Result object.
     */
    public Record decodeRecord(RecordId recordId, Long requestedVersion, ReadContext readContext,
                               Result result, FieldTypes fieldTypes) throws InterruptedException, RepositoryException {
        Record record = newRecord(recordId);
        record.setVersion(requestedVersion);

        // If the version is null, this means the record has no version an thus only contains non-versioned fields (if any)
        // All non-versioned fields are stored at version 1, so we extract the fields at version 1
        Long versionToRead = (requestedVersion == null) ? 1L : requestedVersion;

        // Get a map of all fields with their values for each (cell-)version
        NavigableMap<byte[], NavigableMap<Long, byte[]>> mapWithVersions = result.getMap().get(RecordCf.DATA.bytes);
        if (mapWithVersions != null) {
            // Iterate over all columns
            for (Map.Entry<byte[], NavigableMap<Long, byte[]>> columnWithAllVersions : mapWithVersions.entrySet()) {
                // Check if the retrieved column is from a data field, and not a system field
                byte[] key = columnWithAllVersions.getKey();
                if (key[0] == LilyHBaseSchema.RecordColumn.DATA_PREFIX) {
                    NavigableMap<Long, byte[]> allValueVersions = columnWithAllVersions.getValue();
                    // Get the entry for the version (can be a cell with a lower version number if the field was not changed)
                    Map.Entry<Long, byte[]> ceilingEntry = allValueVersions.ceilingEntry(versionToRead);
                    if (ceilingEntry != null) {
                        // Extract and decode the value of the field
                        ExtractedField field =
                                extractField(key, ceilingEntry.getValue(), readContext, fieldTypes);
                        if (field != null) {
                            record.setField(field.type.getName(), field.value);
                            if (field.metadata != null) {
                                record.setMetadata(field.type.getName(), field.metadata);
                            }
                        }
                    }
                }
            }
        }

        for (Scope scope : Scope.values()) {
            Pair<SchemaId, Long> recordTypePair =  requestedVersion == null ? extractLatestRecordType(scope, result) :
                extractVersionRecordType(scope, result, requestedVersion);
            if (recordTypePair != null) {
                RecordType recordType =
                        typeManager.getRecordTypeById(recordTypePair.getV1(), recordTypePair.getV2());
                record.setRecordType(scope, recordType.getName(), recordType.getVersion());
                if (readContext != null) {
                    readContext.setRecordTypeId(scope, recordType);
                }
            }
        }

        return record;
    }

    /**
     * Version of #decodeRecord() which returns an {@link IdRecord} instance rather than a {@link Record} instance.
     */
    public IdRecord decodeRecordWithIds(Result result) throws InterruptedException, RepositoryException {
        Long latestVersion = getLatestVersion(result);
        RecordId recordId = idGenerator.fromBytes(result.getRow());
        return decodeRecordWithIds(recordId, latestVersion, result, typeManager.getFieldTypesSnapshot());
    }

    /**
     * Version of #decodeRecord() which returns an {@link IdRecord} instance rather than a {@link Record} instance.
     */
    public IdRecord decodeRecordWithIds(RecordId recordId, Long requestedVersion, Result result, FieldTypes fieldTypes)
            throws InterruptedException, RepositoryException {
        final ReadContext readContext = new ReadContext();
        final Record record = decodeRecord(recordId, requestedVersion, readContext, result, fieldTypes);

        Map<SchemaId, QName> idToQNameMapping = new HashMap<SchemaId, QName>();
        for (FieldType fieldType : readContext.getFieldTypes().values()) {
            idToQNameMapping.put(fieldType.getId(), fieldType.getName());
        }

        Map<Scope, SchemaId> recordTypeIds = new EnumMap<Scope, SchemaId>(Scope.class);
        for (Map.Entry<Scope, RecordType> entry : readContext.getRecordTypes().entrySet()) {
            recordTypeIds.put(entry.getKey(), entry.getValue().getId());
        }

        return new IdRecordImpl(record, idToQNameMapping, recordTypeIds);
    }

    /**
     * Gets the requested version of the record (fields and recordTypes) from the Result object.
     * This method is optimized for reading multiple versions.
     */
    public List<Record> decodeRecords(RecordId recordId, List<Long> requestedVersions, Result result,
                                      FieldTypes fieldTypes) throws InterruptedException, RepositoryException {
        Map<Long, Record> records = new HashMap<Long, Record>(requestedVersions.size());
        Map<Long, Set<Scope>> scopes = new HashMap<Long, Set<Scope>>(requestedVersions.size());
        for (Long requestedVersion : requestedVersions) {
            Record record = newRecord(recordId);
            record.setVersion(requestedVersion);
            records.put(requestedVersion, record);
            scopes.put(requestedVersion, EnumSet.noneOf(Scope.class));
        }

        // Get a map of all fields with their values for each (cell-)version
        NavigableMap<byte[], NavigableMap<Long, byte[]>> mapWithVersions = result.getMap().get(RecordCf.DATA.bytes);
        if (mapWithVersions != null) {

            // Iterate over all columns
            for (Map.Entry<byte[], NavigableMap<Long, byte[]>> columnWithAllVersions : mapWithVersions.entrySet()) {

                // Check if the retrieved column is from a data field, and not a system field
                byte[] key = columnWithAllVersions.getKey();
                if (key[0] == RecordColumn.DATA_PREFIX) {
                    NavigableMap<Long, byte[]> allValueVersions = columnWithAllVersions.getValue();

                    // Keep the last decoded field value, to avoid decoding the same value again and again if unchanged
                    // between versions (sparse storage). Note that lastDecodedField can be null, in case of a field
                    // deletion marker
                    Long lastDecodedFieldVersion = null;
                    ExtractedField lastDecodedField = null;
                    for (Long versionToRead : requestedVersions) {
                        Record record = records.get(versionToRead);
                        // Get the entry for the version (can be a cell with a lower version number if the field was
                        // not changed)
                        Map.Entry<Long, byte[]> ceilingEntry = allValueVersions.ceilingEntry(versionToRead);
                        if (ceilingEntry != null) {
                            if (lastDecodedFieldVersion == null ||
                                    !lastDecodedFieldVersion.equals(ceilingEntry.getKey())) {
                                // Not yet decoded, do it now
                                lastDecodedFieldVersion = ceilingEntry.getKey();
                                lastDecodedField = extractField(key, ceilingEntry.getValue(), null, fieldTypes);
                            }
                            if (lastDecodedField != null) {
                                record.setField(lastDecodedField.type.getName(), lastDecodedField.value);
                                scopes.get(versionToRead).add(lastDecodedField.type.getScope());
                                if (lastDecodedField.metadata != null) {
                                    record.setMetadata(lastDecodedField.type.getName(), lastDecodedField.metadata);
                                }
                            }
                        }
                    }
                }
            }
        }

        // Add the record types to the records
        for (Map.Entry<Long, Record> recordEntry : records.entrySet()) {
            Set<Scope> scopesForVersion = scopes.get(recordEntry.getKey());

            // We're only adding the record types if any fields were read.
            if (!scopesForVersion.isEmpty()) {
                // Get the record type for each scope

                // At least the non-versioned record type should be read since that is also the record type of the whole record
                scopesForVersion.add(Scope.NON_VERSIONED);
                for (Scope scope : scopesForVersion) {
                    Pair<SchemaId, Long> recordTypePair = extractVersionRecordType(scope, result, recordEntry.getKey());
                    if (recordTypePair != null) {
                        RecordType recordType =
                                typeManager.getRecordTypeById(recordTypePair.getV1(), recordTypePair.getV2());
                        recordEntry.getValue().setRecordType(scope, recordType.getName(), recordType.getVersion());
                    }
                }
            }
        }

        return new ArrayList<Record>(records.values());
    }

    public Record newRecord() {
        return new RecordImpl();
    }

    public Record newRecord(RecordId recordId) {
        return new RecordImpl(recordId);
    }

    private static class ExtractedField {
        FieldType type;
        Object value;
        Metadata metadata;

        ExtractedField(FieldType type, Object value, Metadata metadata) {
            this.type = type;
            this.value = value;
            this.metadata = metadata;
        }
    }

    private ExtractedField extractField(byte[] key, byte[] prefixedValue, ReadContext context,
                                                 FieldTypes fieldTypes)
            throws RepositoryException, InterruptedException {
        byte flags = prefixedValue[0];
        if (FieldFlags.isDeletedField(flags)) {
            return null;
        }
        FieldType fieldType = fieldTypes.getFieldType(new SchemaIdImpl(Bytes.tail(key, key.length - 1)));
        if (context != null) {
            context.addFieldType(fieldType);
        }
        ValueType valueType = fieldType.getValueType();

        Metadata metadata = null;
        int metadataSpace = 0; // space taken up by metadata (= metadata itself + length suffix)
        int metadataEncodingVersion = FieldFlags.getFieldMetadataVersion(flags);
        if (metadataEncodingVersion == 0) {
            // there is no metadata
        } else if (metadataEncodingVersion == 1) {
            int metadataSize = Bytes.toInt(prefixedValue, prefixedValue.length - Bytes.SIZEOF_INT, Bytes.SIZEOF_INT);
            metadataSpace = metadataSize + Bytes.SIZEOF_INT;
            metadata = MetadataSerDeser.read(
                    new DataInputImpl(prefixedValue, prefixedValue.length - metadataSpace, metadataSize));
        } else {
            throw new RuntimeException("Unsupported field metadata encoding version: " + metadataEncodingVersion);
        }

        Object value = valueType.read(new DataInputImpl(prefixedValue, FieldFlags.SIZE_OF_FIELD_FLAGS,
                prefixedValue.length - FieldFlags.SIZE_OF_FIELD_FLAGS - metadataSpace));

        return new ExtractedField(fieldType, value, metadata);
    }

    /**
     * Extracts the latest record type for a specific scope from the Result.
     */
    private Pair<SchemaId, Long> extractLatestRecordType(Scope scope, Result result) {
        byte[] idBytes = getLatest(result, RecordCf.DATA.bytes, RECORD_TYPE_ID_QUALIFIERS.get(scope));
        byte[] versionBytes = getLatest(result, RecordCf.DATA.bytes, RECORD_TYPE_VERSION_QUALIFIERS.get(scope));
        if ((idBytes == null || idBytes.length == 0) || (versionBytes == null || versionBytes.length == 0)) {
            return null; // No record type was found
        }
        return new Pair<SchemaId, Long>(new SchemaIdImpl(idBytes), Bytes.toLong(versionBytes));
    }

    /**
     * Gets the latest value for a family/qualifier from a Result object, using its
     * getMap(). Most of the time this will be more efficient than using
     * Result.getValue() since the map will need to built anyway when reading the
     * record.
     */
    public byte[] getLatest(Result result, byte[] family, byte[] qualifier) {
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result.getMap();
        if (map == null) {
            return null;
        }

        NavigableMap<byte[], NavigableMap<Long, byte[]>> qualifiers = map.get(family);
        if (qualifiers == null) {
            return null;
        }

        NavigableMap<Long, byte[]> timestamps = qualifiers.get(qualifier);
        if (timestamps == null) {
            return null;
        }

        Map.Entry<Long, byte[]> entry = timestamps.lastEntry();
        return entry == null ? null : entry.getValue();
    }

    /**
     * Collects the FieldType and RecordType objects used during reading a record.
     * Allows to reliably map QName's to types even if these would have been renamed during the reading
     * operation.
     */
    public static class ReadContext {
        private Map<SchemaId, FieldType> fieldTypes = new HashMap<SchemaId, FieldType>();
        private Map<Scope, RecordType> recordTypes = new EnumMap<Scope, RecordType>(Scope.class);

        public void addFieldType(FieldType fieldType) {
            fieldTypes.put(fieldType.getId(), fieldType);
        }

        public void setRecordTypeId(Scope scope, RecordType recordType) {
            recordTypes.put(scope, recordType);
        }

        public Map<Scope, RecordType> getRecordTypes() {
            return recordTypes;
        }

        public Map<SchemaId, FieldType> getFieldTypes() {
            return fieldTypes;
        }
    }

    /**
     * Extracts the record type for a specific version and a specific scope
     */
    public Pair<SchemaId, Long> extractVersionRecordType(Scope scope, Result result, Long version) {
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> allVersionsMap = result.getMap();
        NavigableMap<byte[], NavigableMap<Long, byte[]>> allColumnsAllVersionsMap = allVersionsMap
                .get(RecordCf.DATA.bytes);

        byte[] recordTypeIdColumnName = RECORD_TYPE_ID_QUALIFIERS.get(scope);
        byte[] recordTypeVersionColumnName = RECORD_TYPE_VERSION_QUALIFIERS.get(scope);
        // Get recordTypeId
        Map.Entry<Long, byte[]> idCeilingEntry = null;
        NavigableMap<Long, byte[]> recordTypeIdMap = allColumnsAllVersionsMap.get(recordTypeIdColumnName);
        if (recordTypeIdMap != null) {
            idCeilingEntry = recordTypeIdMap.ceilingEntry(version);
        }
        SchemaId recordTypeId;
        if (idCeilingEntry == null) {
            return null; // No record type was found
        }
        recordTypeId = new SchemaIdImpl(idCeilingEntry.getValue());

        // Get recordTypeVersion
        Long recordTypeVersion;
        Map.Entry<Long, byte[]> versionCeilingEntry = null;
        NavigableMap<Long, byte[]> recordTypeVersionMap = allColumnsAllVersionsMap.get(recordTypeVersionColumnName);
        if (recordTypeVersionMap != null) {
            versionCeilingEntry = recordTypeVersionMap.ceilingEntry(version);
        }
        if (versionCeilingEntry == null) {
            return null; // No record type was found, we should never get here: if there is an id there should also be a version
        }
        recordTypeVersion = Bytes.toLong(versionCeilingEntry.getValue());
        return new Pair<SchemaId, Long>(recordTypeId, recordTypeVersion);
    }

    public Long getLatestVersion(Result result) {
        byte[] latestVersionBytes = getLatest(result, RecordCf.DATA.bytes, LilyHBaseSchema.RecordColumn.VERSION.bytes);
        return latestVersionBytes != null ? Bytes.toLong(latestVersionBytes) : null;
    }

    public static void addSystemColumnsToGet(Get get) {
        for (byte[] field : SYSTEM_FIELDS) {
            get.addColumn(RecordCf.DATA.bytes, field);
        }
    }

    public static void addSystemColumnsToScan(Scan scan) {
        for (byte[] field : SYSTEM_FIELDS) {
            scan.addColumn(RecordCf.DATA.bytes, field);
        }
    }
}
