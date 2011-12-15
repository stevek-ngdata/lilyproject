/*
 * Copyright 2011 Outerthought bvba
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
package org.lilyproject.repository.impl.valuetype;

import java.util.Comparator;

import org.lilyproject.bytes.api.ByteArray;
import org.lilyproject.bytes.api.DataInput;
import org.lilyproject.bytes.api.DataOutput;
import org.lilyproject.repository.api.IdentityRecordStack;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.ValueType;
import org.lilyproject.repository.api.ValueTypeFactory;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * The ByteArrayValueType allows to use byte arrays (byte[]) as field values.
 * <p>
 * To avoid issues with changing byte[] values while they are used in a field,
 * we require them to be wrapped in a read only {@link ByteArray} object.
 */
public class ByteArrayValueType extends AbstractValueType implements ValueType {

    public final static String NAME = "BYTEARRAY";

    private static final Comparator<ByteArray> COMPARATOR = new Comparator<ByteArray>() {
        @Override
        public int compare(ByteArray o1, ByteArray o2) {
            return Bytes.compareTo(o1.getBytesUnsafe(), o2.getBytesUnsafe());
        }
    };

    @Override
    public String getBaseName() {
        return NAME;
    }
    
    @Override
    public ValueType getDeepestValueType() {
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ByteArray read(DataInput dataInput) {
        int length = dataInput.readInt();
        // We can use ByteArray.wrap here since in this use case the dataInput
        // will always be a DataInputImpl which copies the bytes when reading
        // them.
        return ByteArray.wrap(dataInput.readBytes(length));
    }

    @Override
    public void write(Object value, DataOutput dataOutput, IdentityRecordStack parentRecords) {
        ByteArray byteArray = (ByteArray) value;
        dataOutput.writeInt(byteArray.length());
        // We can use getBytesUnsafe here since we know that in this use case
        // the
        // dataOutput will be a DataOutputImpl which copies the byte[] when
        // writing it.
        dataOutput.writeBytes(byteArray.getBytesUnsafe());
    }

    @Override
    public Class getType() {
        return ByteArray.class;
    }

    @Override
    public Comparator getComparator() {
        return COMPARATOR;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + NAME.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        return true;
    }

    //
    // Factory
    //
    public static ValueTypeFactory factory() {
        return new ByteArrayValueTypeFactory();
    }
    
    public static class ByteArrayValueTypeFactory implements ValueTypeFactory {
        private static ByteArrayValueType instance = new ByteArrayValueType();
        
        @Override
        public ValueType getValueType(String typeParams) {
            return instance;
        }
    }
}
