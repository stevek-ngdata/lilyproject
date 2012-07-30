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
package org.lilyproject.avro.repository;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

import org.lilyproject.bytes.api.DataInput;
import org.lilyproject.bytes.api.DataOutput;
import org.lilyproject.bytes.impl.DataOutputImpl;
import org.lilyproject.repository.api.FieldTypes;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.IdRecord;
import org.lilyproject.repository.api.IdentityRecordStack;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordException;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.ResponseStatus;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.api.ValueType;
import org.lilyproject.repository.impl.IdRecordImpl;

/**
 * (De)serialization of Record objects from/to bytes.
 *
 * <p>TODO: idea for further improvement: the namespaces of the QName's could be stored just once
 * and mapped to a short prefix, giving some compression.</p>
 */
public class RecordAsBytesConverter {
    private static final byte NULL_MARKER = 0;
    private static final byte NOT_NULL_MARKER = 1;
    private static final int VERSION_1 = 1;

    public static final byte[] write(Record record, Repository repository)
            throws RepositoryException, InterruptedException {
        DataOutput output = new DataOutputImpl();
        write(record, output, repository);
        return output.toByteArray();
    }

    public static final void write(Record record, DataOutput output, Repository repository)
            throws RepositoryException, InterruptedException {
        // Write serialization format version
        output.writeShort(VERSION_1);

        // Write ID or null
        writeNullOrBytes(record.getId() != null ? record.getId().toBytes() : null, output);

        // Write version or null
        writeNullOrVLong(record.getVersion(), output);

        // Write record type info for each scope (all parts can be null)
        // This assumes the Scope enum stays stable!
        for (Scope scope : Scope.values()) {
            writeNullOrQName(record.getRecordTypeName(scope), output);
            writeNullOrVLong(record.getRecordTypeVersion(scope), output);
        }

        // Write the fields array
        FieldTypes fieldTypes = repository.getTypeManager().getFieldTypesSnapshot();
        output.writeVInt(record.getFields().size());
        for (Map.Entry<QName, Object> entry : record.getFields().entrySet()) {
            if (entry.getKey() == null) {
                throw new IllegalArgumentException("Record contains field with null key.");
            }
            if (entry.getValue() == null) {
                throw new IllegalArgumentException("Record contains field with null value.");
            }

            ValueType valueType = fieldTypes.getFieldType(entry.getKey()).getValueType();

            writeQName(entry.getKey(), output);
            output.writeUTF(valueType.getName());
            try {
                valueType.write(entry.getValue(), output, new IdentityRecordStack());
            } catch (Exception e) {
                throw new RecordException("Error serializing field " + entry.getKey(), e);
            }
        }

        // Write the fields to delete
        output.writeVInt(record.getFieldsToDelete().size());
        for (QName name : record.getFieldsToDelete()) {
            writeQName(name, output);
        }


        // Write transient attributes
        if (record.hasAttributes()) {
            output.writeVInt(record.getAttributes().size());
            for (String key : record.getAttributes().keySet()) {
                String value = record.getAttributes().get(key);
                output.writeUTF(key);
                output.writeUTF(value);
            }
        } else {
            output.writeVInt(0);
        }

        // Write response status or null
        writeNullOrVInt(record.getResponseStatus() != null ? record.getResponseStatus().ordinal() : null, output);
    }

    public static final Record read(DataInput input, Repository repository)
            throws RepositoryException, InterruptedException {
        // Read & check version
        int version = input.readShort();
        if (version != VERSION_1) {
            throw new RuntimeException("Unsupported record serialization version: " + version);
        }

        Record record = repository.newRecord();

        // Read ID
        byte[] idBytes = readNullOrBytes(input);
        if (idBytes != null) {
            record.setId(repository.getIdGenerator().fromBytes(idBytes));
        }

        // Read version
        record.setVersion(readNullOrVLong(input));

        // Read record types for each scope
        for (Scope scope : Scope.values()) {
            QName recordType = readNullOrQName(input);
            Long rtVersion = readNullOrVLong(input);
            record.setRecordType(scope, recordType, rtVersion);
        }

        // Read fields array
        TypeManager typeManager = repository.getTypeManager();
        int size = input.readVInt();
        for (int i = 0; i < size; i++) {
            QName name = readQName(input);
            String valueTypeName = input.readUTF();
            ValueType valueType = typeManager.getValueType(valueTypeName);
            Object value = valueType.read(input);
            record.setField(name, value);
        }

        // Read fields to delete
        size = input.readVInt();
        for (int i = 0; i < size; i++) {
            record.getFieldsToDelete().add(readQName(input));
        }

        // Read transient attributes
        size = input.readVInt();
        for (int i = 0; i < size; i++) {
            String key = input.readUTF();
            String value = input.readUTF();

            record.getAttributes().put(key, value);
        }

        // Read response status or null
        Integer responseStatusOrdinal = readNullOrVInt(input);
        if (responseStatusOrdinal != null) {
            record.setResponseStatus(ResponseStatus.values()[responseStatusOrdinal]);
        }

        return record;
    }

    public static final byte[] writeIdRecord(IdRecord record, Repository repository)
            throws RepositoryException, InterruptedException {
        DataOutput output = new DataOutputImpl();
        writeIdRecord(record, output, repository);
        return output.toByteArray();
    }

    public static final void writeIdRecord(IdRecord record, DataOutput output, Repository repository)
            throws RepositoryException, InterruptedException {
        write(record, output, repository);

        output.writeVInt(record.getFieldIdToNameMapping().size());
        for (Map.Entry<SchemaId, QName> entry : record.getFieldIdToNameMapping().entrySet()) {
            writeBytes(entry.getKey().getBytes(), output);
            writeQName(entry.getValue(), output);
        }

        for (Scope scope : Scope.values()) {
            SchemaId schemaId = record.getRecordTypeId(scope);
            writeNullOrBytes(schemaId != null ? schemaId.getBytes() : null, output);
        }
    }

    public static final IdRecord readIdRecord(DataInput input, Repository repository)
            throws RepositoryException, InterruptedException {
        Record record = read(input, repository);

        IdGenerator idGenerator = repository.getIdGenerator();

        int size = input.readVInt();
        Map<SchemaId, QName> idToQNameMapping = new HashMap<SchemaId, QName>();
        for (int i = 0; i < size; i++) {
            byte[] schemaIdBytes = readBytes(input);
            QName name = readQName(input);

            SchemaId schemaId = idGenerator.getSchemaId(schemaIdBytes);
            idToQNameMapping.put(schemaId, name);
        }

        Map<Scope, SchemaId> recordTypeIds = new EnumMap(Scope.class);
        for (Scope scope : Scope.values()) {
            byte[] schemaIdBytes = readNullOrBytes(input);
            if (schemaIdBytes != null) {
                SchemaId schemaId = idGenerator.getSchemaId(schemaIdBytes);
                recordTypeIds.put(scope, schemaId);
            }
        }

        return new IdRecordImpl(record, idToQNameMapping, recordTypeIds);
    }

    private static final void writeQName(QName name, DataOutput output) {
        output.writeUTF(name.getNamespace());
        output.writeUTF(name.getName());
    }

    private static final QName readQName(DataInput input) {
        String namespace = input.readUTF();
        String name = input.readUTF();
        return new QName(namespace, name);
    }

    private static final void writeNullOrQName(QName name, DataOutput output) {
        if (name == null) {
            output.writeByte(NULL_MARKER);
        } else {
            output.writeByte(NOT_NULL_MARKER);
            writeQName(name, output);
        }
    }

    private static final QName readNullOrQName(DataInput input) {
        byte nullMarker = input.readByte();
        if (nullMarker == NULL_MARKER) {
            return null;
        } else {
            return readQName(input);
        }
    }

    private static final void writeBytes(byte[] bytes, DataOutput output) {
        output.writeVInt(bytes.length);
        output.writeBytes(bytes);
    }

    private static final byte[] readBytes(DataInput input) {
        int length = input.readVInt();
        return input.readBytes(length);
    }

    private static final void writeNullOrBytes(byte[] bytes, DataOutput output) {
        if (bytes == null) {
            output.writeByte(NULL_MARKER);
        } else {
            output.writeByte(NOT_NULL_MARKER);
            writeBytes(bytes, output);
        }
    }

    private static final byte[] readNullOrBytes(DataInput input) {
        byte nullMarker = input.readByte();
        if (nullMarker == NULL_MARKER) {
            return null;
        } else {
            return readBytes(input);
        }
    }

    private static final void writeNullOrVLong(Long value, DataOutput output) {
        if (value == null) {
            output.writeByte(NULL_MARKER);
        } else {
            output.writeByte(NOT_NULL_MARKER);
            output.writeVLong(value);
        }
    }

    private static final Long readNullOrVLong(DataInput input) {
        byte nullMarker = input.readByte();
        if (nullMarker == NULL_MARKER) {
            return null;
        } else {
            return input.readVLong();
        }
    }

    private static final void writeNullOrVInt(Integer value, DataOutput output) {
        if (value == null) {
            output.writeByte(NULL_MARKER);
        } else {
            output.writeByte(NOT_NULL_MARKER);
            output.writeVInt(value);
        }
    }

    private static final Integer readNullOrVInt(DataInput input) {
        byte nullMarker = input.readByte();
        if (nullMarker == NULL_MARKER) {
            return null;
        } else {
            return input.readVInt();
        }
    }

    private static final void writeNullOrString(String value, DataOutput output) {
        if (value == null) {
            output.writeByte(NULL_MARKER);
        } else {
            output.writeByte(NOT_NULL_MARKER);
            output.writeUTF(value);
        }
    }

}
