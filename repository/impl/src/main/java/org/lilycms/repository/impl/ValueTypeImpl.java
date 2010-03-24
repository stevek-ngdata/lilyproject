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
package org.lilycms.repository.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.lilycms.repository.api.PrimitiveValueType;
import org.lilycms.repository.api.ValueType;

public class ValueTypeImpl implements ValueType {

    private final PrimitiveValueType primitiveValueType;
    private final boolean multiValue;

    public ValueTypeImpl(PrimitiveValueType primitiveValueType, boolean multivalue) {
        this.primitiveValueType = primitiveValueType;
        this.multiValue = multivalue;
    }

    public boolean isMultiValue() {
        return multiValue;
    }
    
    public Object fromBytes(byte[] bytes) {
        if (isMultiValue()) {
            List result = new ArrayList();
            int offset = 0;
            while (offset < bytes.length) {
                int valueLenght = Bytes.toInt(bytes, offset, Bytes.SIZEOF_INT);
                offset = offset + Bytes.SIZEOF_INT;
                byte[] valueBytes = new byte[valueLenght];
                Bytes.putBytes(valueBytes, 0, bytes, offset, valueLenght);
                Object value = primitiveValueType.fromBytes(valueBytes);
                result.add(value);
                offset = offset + valueLenght;
            }
            return result;
        } else {
            return primitiveValueType.fromBytes(bytes);
        }
    }
    
    public byte[] toBytes(Object value) {
        if (isMultiValue()) {
            byte[] result = new byte[0];
            for (Object object : (List)value) {
                byte[] encodedValue = primitiveValueType.toBytes(object);
                result = Bytes.add(result, Bytes.toBytes(encodedValue.length));
                result = Bytes.add(result, encodedValue);
            }
            return result;
        } else {
            return primitiveValueType.toBytes(value);
        }
    }

    public Class getType() {
        if (isMultiValue()) {
            return List.class;
        }
        return primitiveValueType.getType();
    }
    
    public byte[] toBytes() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(primitiveValueType.getName());
        stringBuilder.append(",");
        stringBuilder.append(Boolean.toString(isMultiValue()));
        return Bytes.toBytes(stringBuilder.toString());
    }

    public static ValueType fromBytes(byte[] bytes, HBaseTypeManager typeManager) {
        String encodedString = Bytes.toString(bytes);
        int endOfPrimitiveValueTypeName = encodedString.indexOf(",");
        String primitiveValueTypeName = encodedString.substring(0, endOfPrimitiveValueTypeName);
        boolean multivalue = Boolean.parseBoolean(encodedString.substring(endOfPrimitiveValueTypeName + 1));
        return typeManager.getValueType(primitiveValueTypeName, multivalue);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (multiValue ? 1231 : 1237);
        result = prime * result + ((primitiveValueType == null) ? 0 : primitiveValueType.hashCode());
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
        ValueTypeImpl other = (ValueTypeImpl) obj;
        if (multiValue != other.multiValue)
            return false;
        if (primitiveValueType == null) {
            if (other.primitiveValueType != null)
                return false;
        } else if (!primitiveValueType.equals(other.primitiveValueType))
            return false;
        return true;
    }

}
