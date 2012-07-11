package org.lilyproject.indexer.engine;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import com.gotometrics.orderly.StringRowKey;
import com.gotometrics.orderly.StructBuilder;
import com.gotometrics.orderly.StructRowKey;
import com.gotometrics.orderly.Termination;
import com.gotometrics.orderly.VariableLengthByteArrayRowKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilyproject.hbaseindex.Index;
import org.lilyproject.hbaseindex.IndexDefinition;
import org.lilyproject.hbaseindex.IndexEntry;
import org.lilyproject.hbaseindex.IndexManager;
import org.lilyproject.hbaseindex.IndexNotFoundException;
import org.lilyproject.hbaseindex.Query;
import org.lilyproject.hbaseindex.QueryResult;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.util.ArgumentValidator;
import org.lilyproject.util.io.Closer;

/**
 * @author Jan Van Besien
 */
public class DerefMapHbaseImpl implements DerefMap {

    private static final byte[] DEPENDENCIES_KEY = Bytes.toBytes("dependencies");

    private static final byte[] FIELDS_KEY = Bytes.toBytes("fields");
    private static final byte[] VARIANT_PROPERTIES_PATTERN_KEY = Bytes.toBytes("pattern");

    private final static int SCHEMA_ID_BYTE_LENGTH = 16; // see SchemaIdImpl

    private static final byte[] DUMMY_IDENTIFIER = new byte[]{0};


    private Index forwardDerefIndex;

    private Index backwardDerefIndex;

    private IdGenerator idGenerator;

    /**
     * Private constructor. Clients should use static factory methods {@link #delete(String,
     * org.apache.hadoop.conf.Configuration)} and {@link #create(String, org.apache.hadoop.conf.Configuration,
     * org.lilyproject.repository.api.IdGenerator)}
     */
    private DerefMapHbaseImpl(final String indexName, final Configuration hbaseConfiguration,
                              final IdGenerator idGenerator)
            throws IndexNotFoundException, IOException, InterruptedException {

        this.idGenerator = idGenerator;

        final IndexManager indexManager = new IndexManager(hbaseConfiguration);

        IndexDefinition forwardIndexDef = new IndexDefinition(forwardIndexName(indexName));
        // For the record ID we use a variable length byte array field of which the first two bytes are fixed length
        // The first byte is actually the record identifier byte.
        // The second byte really is the first byte of the record id. We put this in the fixed length part
        // (safely because a record id should at least be a single byte long) because this prevents BCD encoding
        // on the first byte, thus making it easier to configure table splitting based on the original input.
        forwardIndexDef.addVariableLengthByteField("dependant_recordid", 2);
        forwardIndexDef.addByteField("dependant_vtag", SCHEMA_ID_BYTE_LENGTH);
        forwardDerefIndex = indexManager.getIndex(forwardIndexDef);

        IndexDefinition backwardIndexDef = new IndexDefinition(backwardIndexName(indexName));
        // Same remark as in the forward index.
        backwardIndexDef.addVariableLengthByteField("dependency_masterrecordid", 2);
        backwardIndexDef.addVariableLengthByteField("variant_properties_pattern");
        backwardIndexDef.addByteField("dependant_vtag", SCHEMA_ID_BYTE_LENGTH);
        backwardDerefIndex = indexManager.getIndex(backwardIndexDef);
    }

    /**
     * Create a DerefMap for a given index. If this is the first time the DerefMap is constructed for this index,
     * the forward and backward index tables will be created.
     *
     * @param indexName          name of the index
     * @param hbaseConfiguration hbase configuration
     * @param idGenerator        id generator
     * @throws IndexNotFoundException
     * @throws IOException
     * @throws InterruptedException
     */
    public static DerefMap create(final String indexName, final Configuration hbaseConfiguration,
                                  final IdGenerator idGenerator)
            throws IndexNotFoundException, IOException, InterruptedException {
        return new DerefMapHbaseImpl(indexName, hbaseConfiguration, idGenerator);
    }

    /**
     * Delete a DerefMap. This will delete the corresponding hbase tables.
     *
     * @param indexName          name of the index to delete
     * @param hbaseConfiguration hbase configuration
     * @throws IOException
     * @throws IndexNotFoundException if the index doesn't exist (maybe it was already deleted?)
     */
    public static void delete(final String indexName, final Configuration hbaseConfiguration)
            throws IOException, IndexNotFoundException {
        final IndexManager manager = new IndexManager(hbaseConfiguration);
        manager.deleteIndex(forwardIndexName(indexName));
        manager.deleteIndex(backwardIndexName(indexName));
    }

    private static String forwardIndexName(String indexName) {
        return "deref-forward-" + indexName;
    }

    private static String backwardIndexName(String indexName) {
        return "deref-backward-" + indexName;
    }

    @Override
    public void updateDependencies(RecordId dependantRecordId, SchemaId dependantVtagId,
                                   Map<DependencyEntry, Set<SchemaId>> newDependencyEntries)
            throws IOException {
        final Set<DependencyEntry> existingEntries = findDependencies(dependantRecordId, dependantVtagId);

        // Figure out what changed
        final Set<DependencyEntry> removedDependencies =
                figureOutRemovedDependencies(newDependencyEntries.keySet(), existingEntries);
        final Collection<DependencyEntry> addedDependencies =
                figureOutAddedDependencies(newDependencyEntries.keySet(), existingEntries);

        // IMPORTANT implementation note: the order in which changes are applied is not arbitrary. It is such that if
        // the process would fail in between, there will never be left any state in the backward index which would not
        // be found via the forward index.

        // delete removed from bwd index
        for (DependencyEntry removed : removedDependencies) {
            final IndexEntry backwardEntry =
                    createBackwardEntry(removed.getDependency(), dependantRecordId, dependantVtagId, null,
                            removed.getMoreDimensionedVariants());
            backwardDerefIndex.removeEntry(backwardEntry);
        }

        // update fwd index (added and removed at the same time, it is a single row)
        final IndexEntry fwdEntry =
                createForwardEntry(dependantRecordId, dependantVtagId, newDependencyEntries.keySet());
        forwardDerefIndex.addEntry(fwdEntry);

        // add added to bwd idx
        for (DependencyEntry added : addedDependencies) {
            final Set<SchemaId> fields = newDependencyEntries.get(added);
            final IndexEntry backwardEntry =
                    createBackwardEntry(added.getDependency(), dependantRecordId, dependantVtagId, fields,
                            added.getMoreDimensionedVariants());
            backwardDerefIndex.addEntry(backwardEntry);
        }
    }

    private Set<DependencyEntry> figureOutRemovedDependencies(Collection<DependencyEntry> newDependencies,
                                                              Set<DependencyEntry> existingDependencies) {
        final Set<DependencyEntry> removed = new HashSet<DependencyEntry>();

        // add all existing
        removed.addAll(existingDependencies);

        // remove all new
        removed.removeAll(newDependencies);

        return removed;
    }

    private Collection<DependencyEntry> figureOutAddedDependencies(Set<DependencyEntry> newDependencyEntries,
                                                                   Set<DependencyEntry> existingDependencies) {
        final Set<DependencyEntry> added = new HashSet<DerefMap.DependencyEntry>();

        // add all new
        added.addAll(newDependencyEntries);

        // remove all existing
        added.removeAll(existingDependencies);

        return added;
    }

    private IndexEntry createForwardEntry(RecordId dependantRecordId, SchemaId dependantVtagId,
                                          Collection<DependencyEntry> newDependencies) throws IOException {
        final IndexEntry fwdEntry = new IndexEntry(forwardDerefIndex.getDefinition());
        fwdEntry.addField("dependant_recordid", dependantRecordId.toBytes());
        fwdEntry.addField("dependant_vtag", dependantVtagId.getBytes());

        // we do not really use the identifier... all we are interested in is in the data of the entry
        fwdEntry.setIdentifier(DUMMY_IDENTIFIER);

        // the data contains the dependencies of the dependant (master record ids and vtags)
        fwdEntry.addData(DEPENDENCIES_KEY, serializeDependenciesForward(newDependencies));

        return fwdEntry;
    }

    private IndexEntry createBackwardEntry(RecordId dependency, RecordId dependantRecordId, SchemaId dependantVtagId,
                                           Set<SchemaId> fields, Set<String> moreDimensionedVariantProperties)
            throws IOException {

        final byte[] serializedVariantPropertiesPattern = serializeVariantPropertiesPattern(
                createVariantPropertiesPattern(dependency.getVariantProperties(),
                        moreDimensionedVariantProperties));


        final IndexEntry bwdEntry = new IndexEntry(backwardDerefIndex.getDefinition());
        bwdEntry.addField("dependency_masterrecordid", dependency.getMaster().toBytes());
        bwdEntry.addField("variant_properties_pattern", serializedVariantPropertiesPattern);
        bwdEntry.addField("dependant_vtag", dependantVtagId.getBytes());

        // the identifier is the dependant which depends on the dependency
        bwdEntry.setIdentifier(dependantRecordId.toBytes());

        // the fields which the dependant uses of the dependency (null if used for deleting the entry)
        if (fields != null)
            bwdEntry.addData(FIELDS_KEY, serializeFields(fields));

        // we add the variant properties in the data as well, for easy access during querying
        // TODO: provide a mechanism to get access to the index fields in the query (hbase-index)?
        bwdEntry.addData(VARIANT_PROPERTIES_PATTERN_KEY, serializedVariantPropertiesPattern);

        return bwdEntry;
    }

    private VariantPropertiesPattern createVariantPropertiesPattern(SortedMap<String, String> propsWithValue,
                                                                    Set<String> propsWithoutValue) {
        final Map<String, String> pattern = new HashMap<String, String>();
        for (Map.Entry<String, String> prop : propsWithValue.entrySet()) {
            pattern.put(prop.getKey(), prop.getValue());
        }
        for (String name : propsWithoutValue) {
            pattern.put(name, null);
        }
        return new VariantPropertiesPattern(pattern);
    }

    /**
     * Serializes a variant properties pattern. The serialization format is simply a list of variable length strings.
     * <code>null</code> values (meaning "any value" in the pattern) are written literaly as <code>null</code> Strings.
     *
     * @param variantPropertiesPattern pattern to serialize
     * @return serialized pattern
     */
    byte[] serializeVariantPropertiesPattern(VariantPropertiesPattern variantPropertiesPattern)
            throws IOException {
        final StringRowKey stringRowKey = createTerminatedStringRowKey();

        // calculate length
        int totalLength = 0;
        // this map stores the strings to serialize (in order, thus a linked hash map!!) with their serialization length
        final Map<String, Integer> stringsWithSerializedLength = new LinkedHashMap<String, Integer>();
        for (Map.Entry<String, String> patternEntry : variantPropertiesPattern.pattern.entrySet()) {
            // name
            final String name = patternEntry.getKey();
            final int nameLength = stringRowKey.getSerializedLength(name);
            stringsWithSerializedLength.put(name, nameLength);
            totalLength += nameLength;

            // value (potentially null)
            final String value = patternEntry.getValue();
            final int valueLength = stringRowKey.getSerializedLength(value);
            stringsWithSerializedLength.put(value, valueLength);
            totalLength += valueLength;
        }

        // serialize
        final byte[] serialized = new byte[totalLength];
        int offset = 0;
        for (Map.Entry<String, Integer> mapEntry : stringsWithSerializedLength.entrySet()) {
            final String string = mapEntry.getKey();
            stringRowKey.serialize(string, serialized, offset);
            final Integer length = mapEntry.getValue();
            offset += length;
        }

        return serialized;
    }

    public VariantPropertiesPattern deserializeVariantPropertiesPattern(byte[] serialized) throws IOException {
        final StringRowKey stringRowKey = createTerminatedStringRowKey();

        final Map<String, String> pattern = new HashMap<String, String>();

        final ImmutableBytesWritable bw = new ImmutableBytesWritable(serialized);

        while (bw.getSize() > 0) {
            final String name = (String) stringRowKey.deserialize(bw);
            final String value = (String) stringRowKey.deserialize(bw); // potentially null
            pattern.put(name, value);
        }
        return new VariantPropertiesPattern(pattern);
    }

    private StringRowKey createTerminatedStringRowKey() {
        final StringRowKey stringRowKey = new StringRowKey();
        stringRowKey.setTermination(Termination.MUST);
        return stringRowKey;
    }

    /**
     * Serializes a list of {@link DependencyEntry}s into a byte array for
     * usage in the forward index table. It uses a variable length byte array encoding schema.
     *
     * @param dependencies list of dependencies to serialize
     * @return byte array with the serialized format
     */
    byte[] serializeDependenciesForward(Collection<DependencyEntry> dependencies) throws IOException {
        final StructRowKey singleEntryRowKey = entrySerializationRowKey();

        // calculate length
        int totalLength = 0;
        final Map<Object[], Integer> entriesWithSerializedLength = new HashMap<Object[], Integer>();
        for (DependencyEntry dependencyEntry : dependencies) {
            final Object[] toSerialize = {
                    // we store the master record id, because that is how they are stored in the backward table
                    dependencyEntry.getDependency().getMaster().toBytes(),
                    serializeVariantPropertiesPattern(createVariantPropertiesPattern(
                            dependencyEntry.getDependency().getVariantProperties(),
                            dependencyEntry.getMoreDimensionedVariants()))
            };
            final int serializedLength = singleEntryRowKey.getSerializedLength(toSerialize);
            entriesWithSerializedLength.put(toSerialize, serializedLength);
            totalLength += serializedLength;
        }

        // serialize
        final byte[] serialized = new byte[totalLength];
        int offset = 0;
        for (Map.Entry<Object[], Integer> mapEntry : entriesWithSerializedLength.entrySet()) {
            final Object[] toSerialize = mapEntry.getKey();
            singleEntryRowKey.serialize(toSerialize, serialized, offset);
            final Integer length = mapEntry.getValue();
            offset += length;
        }

        return serialized;
    }

    public Set<DependencyEntry> deserializeDependenciesForward(byte[] serialized) throws IOException {
        final StructRowKey singleEntryRowKey = entrySerializationRowKey();

        final Set<DependencyEntry> result = new HashSet<DependencyEntry>();

        final ImmutableBytesWritable bw = new ImmutableBytesWritable(serialized);

        while (bw.getSize() > 0) {
            final Object[] deserializedEntry = (Object[]) singleEntryRowKey.deserialize(bw);
            final VariantPropertiesPattern variantPropertiesPattern =
                    deserializeVariantPropertiesPattern((byte[]) deserializedEntry[1]);

            result.add(new DependencyEntry(
                    idGenerator.newRecordId(idGenerator.fromBytes((byte[]) deserializedEntry[0]),
                            variantPropertiesPattern.getConcreteProperties()),
                    variantPropertiesPattern.getPatternProperties()));
        }
        return result;
    }


    private StructRowKey entrySerializationRowKey() {
        final StructRowKey singleEntryRowKey = new StructBuilder()
                .add(new VariableLengthByteArrayRowKey()) // dependency master record id
                .add(new VariableLengthByteArrayRowKey()) // variant property pattern
                .toRowKey();
        singleEntryRowKey.setTermination(Termination.MUST);
        return singleEntryRowKey;
    }

    /**
     * Serializes a list of field ids into a byte array. Each field id has a fixed length, thus the serialization
     * simply appends all the field ids byte representations.
     *
     * @param fields list of field ids to serialize
     * @return byte array containing all the byte representations of the field ids
     */
    byte[] serializeFields(Collection<SchemaId> fields) {
        final byte[] serialized = new byte[SCHEMA_ID_BYTE_LENGTH * fields.size()];
        final Iterator<SchemaId> iterator = fields.iterator();
        int idx = 0;
        while (iterator.hasNext()) {
            final byte[] bytes = iterator.next().getBytes();
            assert SCHEMA_ID_BYTE_LENGTH == bytes.length;
            System.arraycopy(bytes, 0, serialized, idx * SCHEMA_ID_BYTE_LENGTH, SCHEMA_ID_BYTE_LENGTH);
            idx++;
        }

        return serialized;
    }

    Set<SchemaId> deserializeFields(byte[] serialized) {
        final HashSet<SchemaId> result = new HashSet<SchemaId>();
        for (int i = 0; i < serialized.length; i += SCHEMA_ID_BYTE_LENGTH) {
            byte[] bytes = new byte[SCHEMA_ID_BYTE_LENGTH];
            System.arraycopy(serialized, i, bytes, 0, SCHEMA_ID_BYTE_LENGTH);
            result.add(idGenerator.getSchemaId(bytes));
        }
        return result;
    }

    /**
     * Find the set of record ids (and corresponding version tags) on which a given record (in a given version tag)
     * depends.
     *
     * @param recordId record id of the record to find dependencies for
     * @param vtag     vtag of the record to find dependencies for
     * @return the record ids and vtags on which the given record depends
     */
    Set<DependencyEntry> findDependencies(RecordId recordId, SchemaId vtag) throws IOException {
        final Query query = new Query();
        query.addEqualsCondition("dependant_recordid", recordId.toBytes());
        query.addEqualsCondition("dependant_vtag", vtag.getBytes());

        final Set<DependencyEntry> result;

        final QueryResult queryResult = forwardDerefIndex.performQuery(query);
        if (queryResult.next() != null) {
            final byte[] serializedEntries = queryResult.getData(DEPENDENCIES_KEY);
            result = deserializeDependenciesForward(serializedEntries);

            if (queryResult.next() != null) {
                throw new IllegalStateException(
                        "Expected only a single matching entry in " + forwardDerefIndex.getDefinition().getName());
            }

        } else {
            result = new HashSet<DependencyEntry>();
        }

        // Not closed in finally block: avoid HBase contact when there could be connection problems.
        Closer.close(queryResult);

        return result;
    }

    @Override
    public DependantRecordIdsIterator findDependantsOf(RecordId dependency, SchemaId field)
            throws IOException {
        final RecordId master = dependency.getMaster();

        final Query query = new Query();
        query.addEqualsCondition("dependency_masterrecordid", master.toBytes());

        final QueryResult queryResult = backwardDerefIndex.performQuery(query);
        return new DependantRecordIdsIteratorImpl(queryResult, dependency, field);
    }


    final class DependantRecordIdsIteratorImpl implements DependantRecordIdsIterator {
        final QueryResult queryResult;
        final RecordId dependencyRecordId;
        final SchemaId queriedField;

        /**
         * @param queryResult        the query result to filter
         * @param dependencyRecordId the dependency record, used to match the variant property pattern with
         * @param queriedField       the queried field, used to match with the field information stored in the deref
         *                           map (the queried field is allowed to be <code>null</code> in order to only match
         *                           results that express dependencies that do not go to a field)
         */
        DependantRecordIdsIteratorImpl(QueryResult queryResult, RecordId dependencyRecordId, SchemaId queriedField) {
            ArgumentValidator.notNull(queryResult, "queryResult");
            ArgumentValidator.notNull(dependencyRecordId, "dependencyRecordId");

            this.queryResult = queryResult;
            this.dependencyRecordId = dependencyRecordId;
            this.queriedField = queriedField;
        }

        @Override
        public void close() throws IOException {
            queryResult.close();
        }

        RecordId next = null;

        private RecordId getNextFromQueryResult() throws IOException {
            // TODO: we could optimize the implementation somehow to make this filtering happen on region server... (but how to integrate that with hbase index library in a generic fashion?)

            byte[] nextIdentifier = null;
            while ((nextIdentifier = queryResult.next()) != null) {
                // the identifier is the record id of the record that depends on the queried record
                // but we only include it if the dependency is via the specified field AND if the variant properties
                // are matching

                final Set<SchemaId> dependencyFields = deserializeFields(queryResult.getData(FIELDS_KEY));
                final VariantPropertiesPattern variantPropertiesPattern =
                        deserializeVariantPropertiesPattern(queryResult.getData(VARIANT_PROPERTIES_PATTERN_KEY));

                if ((queriedField == null || dependencyFields.contains(queriedField)) &&
                        variantPropertiesPattern.matches(dependencyRecordId.getVariantProperties())) {
                    return idGenerator.fromBytes(nextIdentifier);
                }
            }

            return null; // query result exhausted
        }


        @Override
        public boolean hasNext() throws IOException {
            synchronized (this) { // to protect setting/resetting the next value from race conditions
                if (next != null) {
                    // the next was already set, but not yet used
                    return true;
                } else {
                    // try setting a next value
                    next = getNextFromQueryResult();
                    return next != null;
                }
            }
        }

        @Override
        public RecordId next() throws IOException {
            synchronized (this) { // to protect setting/resetting the next value from race conditions
                if (next != null) {
                    // the next was already set, but not yet used
                    RecordId nextToReturn = next;
                    next = null;
                    return nextToReturn;
                } else {
                    // try setting a next value
                    next = getNextFromQueryResult();
                    return next;
                }
            }
        }

    }

    final static class VariantPropertiesPattern {

        /**
         * The pattern. Null values mean "any value", everything else must match exactly.
         */
        final Map<String, String> pattern;

        VariantPropertiesPattern(Map<String, String> pattern) {
            ArgumentValidator.notNull(pattern, "pattern");

            this.pattern = pattern;
        }

        private boolean matches(SortedMap<String, String> dependancyRecordVariantProperties) {
            if (dependancyRecordVariantProperties.size() != pattern.size()) {
                return false;
            } else {
                // all names should match exactly
                if (!dependancyRecordVariantProperties.keySet().equals(patternNames())) {
                    return false;
                } else {
                    // values should match if specified
                    for (Map.Entry<String, String> entry : dependancyRecordVariantProperties.entrySet()) {
                        final String name = entry.getKey();
                        final String value = entry.getValue();

                        final String patternValue = patternValue(name);

                        if (patternValue != null && !patternValue.equals(value)) {
                            return false;
                        }
                    }

                    // no unmatching values found
                    return true;
                }
            }
        }

        private Set<String> patternNames() {
            return pattern.keySet();
        }

        private String patternValue(String name) {
            return pattern.get(name);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            VariantPropertiesPattern that = (VariantPropertiesPattern) o;

            return !(pattern != null ? !pattern.equals(that.pattern) : that.pattern != null);
        }

        @Override
        public int hashCode() {
            return pattern != null ? pattern.hashCode() : 0;
        }

        @Override
        public String toString() {
            return "VariantPropertiesPattern{" +
                    "pattern=" + pattern +
                    '}';
        }

        public Map<String, String> getConcreteProperties() {
            final HashMap<String, String> result = new HashMap<String, String>();
            for (Map.Entry<String, String> patternEntry : pattern.entrySet()) {
                if (patternEntry.getValue() != null)
                    result.put(patternEntry.getKey(), patternEntry.getValue());
            }
            return result;
        }

        public Set<String> getPatternProperties() {
            final Set<String> result = new HashSet<String>();
            for (Map.Entry<String, String> patternEntry : pattern.entrySet()) {
                if (patternEntry.getValue() == null)
                    result.add(patternEntry.getKey());
            }
            return result;
        }
    }


}
