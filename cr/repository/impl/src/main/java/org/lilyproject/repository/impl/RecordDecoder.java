package org.lilyproject.repository.impl;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilyproject.repository.api.*;
import org.lilyproject.util.Pair;
import org.lilyproject.util.hbase.LilyHBaseSchema;

import java.util.*;

/**
 * Methods related to decoding HBase Result objects into Lily Record objects.
 * 
 * <p>The methods in this class assume they get non-deleted records, thus where the
 * {@link LilyHBaseSchema.RecordColumn#DELETED} flag is false.</p>
 */
public class RecordDecoder {
    public static Map<Scope, byte[]> recordTypeIdColumnNames = new EnumMap<Scope, byte[]>(Scope.class);
    public static Map<Scope, byte[]> recordTypeVersionColumnNames = new EnumMap<Scope, byte[]>(Scope.class);

    private TypeManager typeManager;
    private IdGenerator idGenerator;

    static {
        recordTypeIdColumnNames.put(Scope.NON_VERSIONED, LilyHBaseSchema.RecordColumn.NON_VERSIONED_RT_ID.bytes);
        recordTypeIdColumnNames.put(Scope.VERSIONED, LilyHBaseSchema.RecordColumn.VERSIONED_RT_ID.bytes);
        recordTypeIdColumnNames.put(Scope.VERSIONED_MUTABLE, LilyHBaseSchema.RecordColumn.VERSIONED_MUTABLE_RT_ID.bytes);
        recordTypeVersionColumnNames.put(Scope.NON_VERSIONED, LilyHBaseSchema.RecordColumn.NON_VERSIONED_RT_VERSION.bytes);
        recordTypeVersionColumnNames.put(Scope.VERSIONED, LilyHBaseSchema.RecordColumn.VERSIONED_RT_VERSION.bytes);
        recordTypeVersionColumnNames.put(Scope.VERSIONED_MUTABLE, LilyHBaseSchema.RecordColumn.VERSIONED_MUTABLE_RT_VERSION.bytes);
    }

    public RecordDecoder(TypeManager typeManager, IdGenerator idGenerator) {
        this.typeManager = typeManager;
        this.idGenerator = idGenerator;
    }

    /**
     * Decode the given record object assuming it contains the state of the latest version of the record
     * (if the record has any versions, otherwise it only contains non-versioned fields).
     */
    public Record decodeRecord(Result result) throws InterruptedException, RepositoryException {
        Long latestVersion = getLatestVersion(result);
        RecordId recordId = idGenerator.fromBytes(result.getRow());
        return getRecordFromRowResult(recordId, latestVersion, null, result, typeManager.getFieldTypesSnapshot());
    }

    /**
     *  Gets the requested version of the record (fields and recordTypes) from the Result object.
     */
    public Record getRecordFromRowResult(RecordId recordId, Long requestedVersion, ReadContext readContext,
            Result result, FieldTypes fieldTypes) throws InterruptedException, RepositoryException {
        Record record = new RecordImpl(recordId);
        record.setVersion(requestedVersion);
        Set<Scope> scopes = EnumSet.noneOf(Scope.class); // Set of scopes for which a field has been read

        // If the version is null, this means the record has no version an thus only contains non-versioned fields (if any)
        // All non-versioned fields are stored at version 1, so we extract the fields at version 1
        Long versionToRead = (requestedVersion == null) ? 1L : requestedVersion;

        // Get a map of all fields with their values for each (cell-)version
        NavigableMap<byte[], NavigableMap<Long, byte[]>> mapWithVersions = result.getMap().get(LilyHBaseSchema.RecordCf.DATA.bytes);
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
                        Pair<FieldType, Object> field = extractField(key, ceilingEntry.getValue(), readContext, fieldTypes);
                        if (field != null) {
                            record.setField(field.getV1().getName(), field.getV2());
                            scopes.add(field.getV1().getScope());
                        }
                    }
                }
            }
        }

        // We're only adding the record types if any fields were read.
        if (!scopes.isEmpty()) {
            if (requestedVersion == null) {
                // There can only be non-versioned fields, so only read the non-versioned record type
                Pair<SchemaId, Long> recordTypePair = extractLatestRecordType(Scope.NON_VERSIONED, result);
                if (recordTypePair != null) {
                    RecordType recordType = typeManager.getRecordTypeById(recordTypePair.getV1(), recordTypePair.getV2());
                    record.setRecordType(recordType.getName(), recordType.getVersion());
                    if (readContext != null)
                        readContext.setRecordTypeId(Scope.NON_VERSIONED, recordType);
                }
            } else {
                // Get the record type for each scope
                // At least the non-versioned record type should be read since that is also the record type of the whole record
                scopes.add(Scope.NON_VERSIONED);
                for (Scope scope : scopes) {
                    Pair<SchemaId, Long> recordTypePair = extractVersionRecordType(scope, result, requestedVersion);
                    if (recordTypePair != null) {
                        RecordType recordType = typeManager.getRecordTypeById(recordTypePair.getV1(), recordTypePair.getV2());
                        record.setRecordType(scope, recordType.getName(), recordType.getVersion());
                        if (readContext != null)
                            readContext.setRecordTypeId(scope, recordType);
                    }
                }
            }
        }

        return record;
    }

    public Pair<FieldType, Object> extractField(byte[] key, byte[] prefixedValue, ReadContext context,
            FieldTypes fieldTypes) throws RepositoryException, InterruptedException {
        byte prefix = prefixedValue[0];
        if (LilyHBaseSchema.DELETE_FLAG == prefix) {
            return null;
        }
        FieldType fieldType = fieldTypes.getFieldType(new SchemaIdImpl(Bytes.tail(key, key.length-1)));
        if (context != null)
            context.addFieldType(fieldType);
        ValueType valueType = fieldType.getValueType();
        Object value = valueType.read(EncodingUtil.stripPrefix(prefixedValue));
        return new Pair<FieldType, Object>(fieldType, value);
    }

    /**
     * Extracts the latest record type for a specific scope from the Result.
     */
    public Pair<SchemaId, Long> extractLatestRecordType(Scope scope, Result result) {
        byte[] idBytes = getLatest(result, LilyHBaseSchema.RecordCf.DATA.bytes, recordTypeIdColumnNames.get(scope));
        byte[] versionBytes = getLatest(result, LilyHBaseSchema.RecordCf.DATA.bytes, recordTypeVersionColumnNames.get(scope));
        if ((idBytes == null || idBytes.length == 0) || (versionBytes == null || versionBytes.length == 0))
            return null; // No record type was found
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
        if (map == null)
            return null;

        NavigableMap<byte[], NavigableMap<Long, byte[]>> qualifiers = map.get(family);
        if (qualifiers == null)
            return null;

        NavigableMap<Long, byte[]> timestamps = qualifiers.get(qualifier);
        if (timestamps == null)
            return null;

        Map.Entry<Long, byte[]> entry = timestamps.lastEntry();
        return entry == null ? null : entry.getValue();
    }

    public static class ReadContext {
        private Map<SchemaId, FieldType> fieldTypes = new HashMap<SchemaId, FieldType>();
        private Map<Scope, RecordType> recordTypes = new EnumMap<Scope, RecordType>(Scope.class);
        private Set<Scope> scopes = EnumSet.noneOf(Scope.class);

        public void addFieldType(FieldType fieldType) {
            fieldTypes.put(fieldType.getId(), fieldType);
            scopes.add(fieldType.getScope());
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

        public Set<Scope> getScopes() {
            return scopes;
        }
    }

    /**
     * Extracts the record type for a specific version and a specific scope
     */
    public Pair<SchemaId, Long> extractVersionRecordType(Scope scope, Result result, Long version) {
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> allVersionsMap = result.getMap();
        NavigableMap<byte[], NavigableMap<Long, byte[]>> allColumnsAllVersionsMap = allVersionsMap
                .get(LilyHBaseSchema.RecordCf.DATA.bytes);

        byte[] recordTypeIdColumnName = recordTypeIdColumnNames.get(scope);
        byte[] recordTypeVersionColumnName = recordTypeVersionColumnNames.get(scope);
        // Get recordTypeId
        Map.Entry<Long, byte[]> idCeilingEntry = null;
        NavigableMap<Long, byte[]> recordTypeIdMap = allColumnsAllVersionsMap.get(recordTypeIdColumnName);
        if (recordTypeIdMap != null) {
            idCeilingEntry = recordTypeIdMap.ceilingEntry(version);
        }
        SchemaId recordTypeId;
        if (idCeilingEntry == null)
            return null; // No record type was found
        recordTypeId = new SchemaIdImpl(idCeilingEntry.getValue());

        // Get recordTypeVersion
        Long recordTypeVersion;
        Map.Entry<Long, byte[]> versionCeilingEntry = null;
        NavigableMap<Long, byte[]> recordTypeVersionMap = allColumnsAllVersionsMap.get(recordTypeVersionColumnName);
        if (recordTypeVersionMap != null) {
            versionCeilingEntry = recordTypeVersionMap.ceilingEntry(version);
        }
        if (versionCeilingEntry == null)
            return null; // No record type was found, we should never get here: if there is an id there should also be a version
        recordTypeVersion = Bytes.toLong(versionCeilingEntry.getValue());
        Pair<SchemaId, Long> recordType = new Pair<SchemaId, Long>(recordTypeId, recordTypeVersion);
        return recordType;
    }

    public Long getLatestVersion(Result result) {
        byte[] latestVersionBytes = getLatest(result, LilyHBaseSchema.RecordCf.DATA.bytes, LilyHBaseSchema.RecordColumn.VERSION.bytes);
        Long latestVersion = latestVersionBytes != null ? Bytes.toLong(latestVersionBytes) : null;
        return latestVersion;
    }
}
