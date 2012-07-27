package org.lilyproject.indexer.derefmap;

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
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.SchemaId;

/**
 * Helper for the {@link org.lilyproject.indexer.derefmap.DerefMapHbaseImpl} to serialize/deserialize various elements
 * for storage in the hbase-index.
 *
 *
 */
final class DerefMapSerializationUtil {

    final static int SCHEMA_ID_BYTE_LENGTH = 16; // see SchemaIdImpl

    private final IdGenerator idGenerator;

    public DerefMapSerializationUtil(IdGenerator idGenerator) {
        this.idGenerator = idGenerator;
    }

    // LIST OF DEPENDENCIES IN THE FORWARD TABLE

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

    Set<DependencyEntry> deserializeDependenciesForward(byte[] serialized) throws IOException {
        final StructRowKey singleEntryRowKey = entrySerializationRowKey();

        final Set<DependencyEntry> result = new HashSet<DependencyEntry>();

        final ImmutableBytesWritable bw = new ImmutableBytesWritable(serialized);

        while (bw.getSize() > 0) {
            final Object[] deserializedEntry = (Object[]) singleEntryRowKey.deserialize(bw);
            final DerefMapVariantPropertiesPattern variantPropertiesPattern =
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

    // LIST OF FIELDS IN THE BACKWARD TABLE

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
        return deserializeFields(serialized, 0, serialized.length);
    }

    public Set<SchemaId> deserializeFields(byte[] serialized, int offset, int length) {
        final HashSet<SchemaId> result = new HashSet<SchemaId>();
        for (int i = offset; i < offset + length; i += SCHEMA_ID_BYTE_LENGTH) {
            byte[] bytes = new byte[SCHEMA_ID_BYTE_LENGTH];
            System.arraycopy(serialized, i, bytes, 0, SCHEMA_ID_BYTE_LENGTH);
            result.add(idGenerator.getSchemaId(bytes));
        }
        return result;
    }

    // VARIANT PROPERTIES PATTERN

    DerefMapVariantPropertiesPattern createVariantPropertiesPattern(SortedMap<String, String> propsWithValue,
                                                                    Set<String> propsWithoutValue) {
        final Map<String, String> pattern = new HashMap<String, String>();
        for (Map.Entry<String, String> prop : propsWithValue.entrySet()) {
            pattern.put(prop.getKey(), prop.getValue());
        }
        for (String name : propsWithoutValue) {
            pattern.put(name, null);
        }
        return new DerefMapVariantPropertiesPattern(pattern);
    }

    /**
     * Serializes a variant properties pattern. The serialization format is simply a list of variable length strings.
     * <code>null</code> values (meaning "any value" in the pattern) are written literaly as <code>null</code> Strings.
     *
     * @param variantPropertiesPattern pattern to serialize
     * @return serialized pattern
     */
    byte[] serializeVariantPropertiesPattern(DerefMapVariantPropertiesPattern variantPropertiesPattern)
            throws IOException {
        final StringRowKey stringRowKey = terminatedStringRowKey();

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

    DerefMapVariantPropertiesPattern deserializeVariantPropertiesPattern(byte[] serialized) {
        final StringRowKey stringRowKey = terminatedStringRowKey();

        final Map<String, String> pattern = new HashMap<String, String>();

        final ImmutableBytesWritable bw = new ImmutableBytesWritable(serialized);

        try {
            while (bw.getSize() > 0) {
                final String name;
                name = (String) stringRowKey.deserialize(bw);
                final String value = (String) stringRowKey.deserialize(bw); // potentially null
                pattern.put(name, value);
            }
        } catch (IOException e) {
            throw new RuntimeException("index inconsistency?", e);
        }

        return new DerefMapVariantPropertiesPattern(pattern);
    }

    private StringRowKey terminatedStringRowKey() {
        final StringRowKey stringRowKey = new StringRowKey();
        stringRowKey.setTermination(Termination.MUST);
        return stringRowKey;
    }

    // RECORD ID

    RecordId deserializeRecordId(byte[] serialized) {
        return idGenerator.fromBytes(serialized);
    }

    // SCHEMA ID

    SchemaId deserializeSchemaId(byte[] serialized) {
        return idGenerator.getSchemaId(serialized);
    }
}
