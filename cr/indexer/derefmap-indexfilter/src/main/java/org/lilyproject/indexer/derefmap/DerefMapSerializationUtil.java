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
package org.lilyproject.indexer.derefmap;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import org.apache.hadoop.hbase.util.Bytes;
import org.lilyproject.bytes.api.DataInput;
import org.lilyproject.bytes.api.DataOutput;
import org.lilyproject.bytes.impl.DataInputImpl;
import org.lilyproject.bytes.impl.DataOutputImpl;
import org.lilyproject.repository.api.AbsoluteRecordId;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.impl.id.AbsoluteRecordIdImpl;

/**
 * Helper for the {@link org.lilyproject.indexer.derefmap.DerefMapHbaseImpl} to serialize/deserialize various elements
 * for storage in the hbase-index.
 */
final class DerefMapSerializationUtil {

    final static int SCHEMA_ID_BYTE_LENGTH = 16; // see SchemaIdImpl

    private final IdGenerator idGenerator;

    DerefMapSerializationUtil(IdGenerator idGenerator) {
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
        final DataOutputImpl dataOutput = new DataOutputImpl();

        // total number of dependencies
        dataOutput.writeInt(dependencies.size());

        for (DependencyEntry dependencyEntry : dependencies) {
            // we store the master record id, because that is how they are stored in the backward table
            final byte[] masterTableBytes = Bytes.toBytes(dependencyEntry.getDependency().getTable());
            final byte[] masterBytes = dependencyEntry.getDependency().getRecordId().getMaster().toBytes();
            dataOutput.writeInt(masterTableBytes.length);
            dataOutput.writeBytes(masterTableBytes);
            dataOutput.writeInt(masterBytes.length);
            dataOutput.writeBytes(masterBytes);

            final byte[] variantPropertiesBytes = serializeVariantPropertiesPattern(createVariantPropertiesPattern(
                    dependencyEntry.getDependency().getRecordId().getVariantProperties(),
                    dependencyEntry.getMoreDimensionedVariants()));
            dataOutput.writeBytes(variantPropertiesBytes);
        }

        return dataOutput.toByteArray();
    }

    Set<DependencyEntry> deserializeDependenciesForward(byte[] serialized) throws IOException {
        final DataInputImpl dataInput = new DataInputImpl(serialized);
        final int nDependencies = dataInput.readInt();

        final Set<DependencyEntry> result = new HashSet<DependencyEntry>(nDependencies);

        while (result.size() < nDependencies) {
            final int tableLength = dataInput.readInt();
            final String table = Bytes.toString(dataInput.readBytes(tableLength));
            final int masterBytesLength = dataInput.readInt();
            final byte[] masterBytes = dataInput.readBytes(masterBytesLength);

            final DerefMapVariantPropertiesPattern variantPropertiesPattern =
                    deserializeVariantPropertiesPattern(dataInput);

            //FIXME: multi-repository
            result.add(new DependencyEntry(new AbsoluteRecordIdImpl("default",table,
                    idGenerator.newRecordId(idGenerator.fromBytes(masterBytes),
                            variantPropertiesPattern.getConcreteProperties())),
                    variantPropertiesPattern.getPatternProperties()));
        }

        return result;
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
        final DataOutput dataOutput = new DataOutputImpl();

        // total number of entries
        dataOutput.writeInt(variantPropertiesPattern.pattern.size());

        for (Map.Entry<String, String> patternEntry : variantPropertiesPattern.pattern.entrySet()) {
            // name
            final String name = patternEntry.getKey();
            dataOutput.writeUTF(name);

            // value (potentially null)
            final String value = patternEntry.getValue();
            dataOutput.writeUTF(value);
        }

        return dataOutput.toByteArray();
    }

    DerefMapVariantPropertiesPattern deserializeVariantPropertiesPattern(byte[] serialized) {
        return deserializeVariantPropertiesPattern(new DataInputImpl(serialized));
    }

    DerefMapVariantPropertiesPattern deserializeVariantPropertiesPattern(DataInput dataInput) {
        final Map<String, String> pattern = new HashMap<String, String>();

        final int nEntries = dataInput.readInt();

        while (pattern.size() < nEntries) {
            final String name = dataInput.readUTF();
            final String value = dataInput.readUTF();

            pattern.put(name, value);
        }

        return new DerefMapVariantPropertiesPattern(pattern);
    }

    // RECORD ID

    AbsoluteRecordId deserializeDependantRecordId(byte[] serialized) {
        return AbsoluteRecordIdImpl.fromBytes(serialized, idGenerator);
    }

    // SCHEMA ID

    SchemaId deserializeSchemaId(byte[] serialized) {
        return idGenerator.getSchemaId(serialized);
    }
}
