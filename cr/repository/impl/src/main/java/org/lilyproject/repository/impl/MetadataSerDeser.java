/*
 * Copyright 2013 NGDATA nv
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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.DateTimeZone;
import org.lilyproject.bytes.api.ByteArray;
import org.lilyproject.bytes.api.DataInput;
import org.lilyproject.bytes.api.DataOutput;
import org.lilyproject.repository.api.Metadata;
import org.lilyproject.repository.api.MetadataBuilder;

/**
 * Serialize and deserialize a {@link Metadata} object. The various value types supported by Metadata use a
 * type-specific serialization, so that type information is retained and storage size optimal.
 */
public class MetadataSerDeser {
    private MetadataSerDeser() {
    }

    /**
     * Writes the metadata to the given output. It can be read back from a variable-length input using
     * {@link #read(DataInput)}.
     */
    public static void write(Metadata metadata, DataOutput output) {
        // Write the fields
        Map<String, Object> map = metadata.getMap();
        output.writeVInt(map.size());
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            // write the key
            output.writeVUTF(entry.getKey());
            // write the value
            Object value = entry.getValue();
            ValueSerDeser serdeser = CLASS_TO_SERDESER.get(value.getClass());
            if (serdeser == null) {
                throw new IllegalArgumentException("Unsupported kind of metadata value: type of '" + value + "' is "
                        + value.getClass().getName());
            }
            output.writeByte(serdeser.getTypeByte());
            serdeser.serialize(value, output);
        }

        // Write the deleted fields
        Set<String> fieldsToDelete = metadata.getFieldsToDelete();
        output.writeVInt(fieldsToDelete.size());
        for (String field : fieldsToDelete) {
            output.writeVUTF(field);
        }
    }

    public static Metadata read(DataInput input) {
        MetadataBuilder metadataBuilder = new MetadataBuilder();

        // read fields
        int size = input.readVInt();
        for (int i = 0; i < size; i++) {
            String key = input.readVUTF();
            byte valueType = input.readByte();
            ValueSerDeser serdeser = BYTE_TO_SERDESER.get(valueType);
            if (serdeser == null) {
                throw new IllegalArgumentException("Unsupported kind of metadata value: type byte " + (int)valueType);
            }
            Object value = serdeser.deserialize(input);
            metadataBuilder.object(key, value);
        }

        // read deleted fields
        size = input.readVInt();
        for (int i = 0; i < size; i++) {
            metadataBuilder.delete(input.readVUTF());
        }

        return metadataBuilder.build();
    }

    private static interface ValueSerDeser {
        Class getTypeClass();

        byte getTypeByte();

        void serialize(Object value, DataOutput dataOutput);

        Object deserialize(DataInput dataInput);
    }

    private static final ValueSerDeser STRING_SERDESER = new StringValueSerDeser();

    private static class StringValueSerDeser implements ValueSerDeser {
        @Override
        public Class getTypeClass() {
            return String.class;
        }

        @Override
        public byte getTypeByte() {
            return 1;
        }

        @Override
        public void serialize(Object value, DataOutput dataOutput) {
            dataOutput.writeVUTF((String)value);
        }

        @Override
        public Object deserialize(DataInput dataInput) {
            return dataInput.readVUTF();
        }
    }

    private static final ValueSerDeser INT_SERDESER = new IntegerValueSerDeser();

    private static class IntegerValueSerDeser implements ValueSerDeser {
        @Override
        public Class getTypeClass() {
            return Integer.class;
        }

        @Override
        public byte getTypeByte() {
            return 2;
        }

        @Override
        public void serialize(Object value, DataOutput dataOutput) {
            dataOutput.writeVInt((Integer) value);
        }

        @Override
        public Object deserialize(DataInput dataInput) {
            return dataInput.readVInt();
        }
    }

    private static final ValueSerDeser LONG_SERDESER = new LongValueSerDeser();

    private static class LongValueSerDeser implements ValueSerDeser {
        @Override
        public Class getTypeClass() {
            return Long.class;
        }

        @Override
        public byte getTypeByte() {
            return 3;
        }

        @Override
        public void serialize(Object value, DataOutput dataOutput) {
            dataOutput.writeVLong((Long) value);
        }

        @Override
        public Object deserialize(DataInput dataInput) {
            return dataInput.readVLong();
        }
    }

    private static final ValueSerDeser FLOAT_SERDESER = new FloatValueSerDeser();

    private static class FloatValueSerDeser implements ValueSerDeser {
        @Override
        public Class getTypeClass() {
            return Float.class;
        }

        @Override
        public byte getTypeByte() {
            return 4;
        }

        @Override
        public void serialize(Object value, DataOutput dataOutput) {
            dataOutput.writeFloat((Float) value);
        }

        @Override
        public Object deserialize(DataInput dataInput) {
            return dataInput.readFloat();
        }
    }

    private static final ValueSerDeser DOUBLE_SERDESER = new DoubleValueSerDeser();

    private static class DoubleValueSerDeser implements ValueSerDeser {
        @Override
        public Class getTypeClass() {
            return Double.class;
        }

        @Override
        public byte getTypeByte() {
            return 5;
        }

        @Override
        public void serialize(Object value, DataOutput dataOutput) {
            dataOutput.writeDouble((Double) value);
        }

        @Override
        public Object deserialize(DataInput dataInput) {
            return dataInput.readDouble();
        }
    }

    private static final ValueSerDeser BOOLEAN_SERDESER = new BooleanValueSerDeser();

    private static class BooleanValueSerDeser implements ValueSerDeser {
        @Override
        public Class getTypeClass() {
            return Boolean.class;
        }

        @Override
        public byte getTypeByte() {
            return 6;
        }

        @Override
        public void serialize(Object value, DataOutput dataOutput) {
            dataOutput.writeBoolean((Boolean) value);
        }

        @Override
        public Object deserialize(DataInput dataInput) {
            return dataInput.readBoolean();
        }
    }

    private static final ValueSerDeser BYTES_SERDESER = new BytesValueSerDeser();

    private static class BytesValueSerDeser implements ValueSerDeser {
        @Override
        public Class getTypeClass() {
            return ByteArray.class;
        }

        @Override
        public byte getTypeByte() {
            return 7;
        }

        @Override
        public void serialize(Object value, DataOutput dataOutput) {
            dataOutput.writeVInt(((ByteArray)value).length());
            dataOutput.writeBytes(((ByteArray) value).getBytesUnsafe());
        }

        @Override
        public Object deserialize(DataInput dataInput) {
            int size = dataInput.readVInt();
            return new ByteArray(dataInput.readBytes(size));
        }
    }

    private static final ValueSerDeser DATETIME_SERDESER = new DateTimeValueSerDeser();

    private static class DateTimeValueSerDeser implements ValueSerDeser {

        @Override
        public Class getTypeClass() {
            return org.joda.time.DateTime.class;
        }

        @Override
        public byte getTypeByte() {
            return 8;
        }

        @Override
        public void serialize(Object value, DataOutput dataOutput) {
            DateTime dateTime = (DateTime) value;
            dateTime = dateTime.toDateTime(DateTimeZone.UTC); //store as UTC
            dataOutput.writeLong(dateTime.getMillis());
        }

        @Override
        public Object deserialize(DataInput dataInput) {
            // Always construct UTC such that it is not depending on the default timezone of the current host
            return new DateTime(dataInput.readLong(), DateTimeZone.UTC);
        }
    }

    private static final ValueSerDeser[] serdesers = new ValueSerDeser[] { STRING_SERDESER, INT_SERDESER, LONG_SERDESER,
            FLOAT_SERDESER, DOUBLE_SERDESER, BOOLEAN_SERDESER, BYTES_SERDESER, DATETIME_SERDESER };

    private static final Map<Class, ValueSerDeser> CLASS_TO_SERDESER = new HashMap<Class, ValueSerDeser>();
    static {
        for (ValueSerDeser serdeser : serdesers) {
            CLASS_TO_SERDESER.put(serdeser.getTypeClass(), serdeser);
        }

        // This is just to protect against double usage of the same type class
        if (CLASS_TO_SERDESER.size() != serdesers.length) {
            throw new RuntimeException("Incorrect number of serdesers.");
        }
    }

    private static final Map<Byte, ValueSerDeser> BYTE_TO_SERDESER = new HashMap<Byte, ValueSerDeser>();
    static {
        for (ValueSerDeser serdeser : serdesers) {
            BYTE_TO_SERDESER.put(serdeser.getTypeByte(), serdeser);
        }

        // This is just to protect against double usage of the same type byte
        if (BYTE_TO_SERDESER.size() != serdesers.length) {
            throw new RuntimeException("Incorrect number of serdesers.");
        }
    }

}
