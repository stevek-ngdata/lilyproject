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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.util.Bytes;
import org.lilyproject.bytes.api.DataInput;
import org.lilyproject.bytes.api.DataOutput;
import org.lilyproject.bytes.impl.DataOutputImpl;
import org.lilyproject.hbaseext.ContainsValueComparator;
import org.lilyproject.repository.api.*;
import org.lilyproject.util.ArgumentValidator;

public class ValueTypeImpl  {

//    private final PrimitiveValueType primitiveValueType;
//
//    public ValueTypeImpl(PrimitiveValueType primitiveValueType) {
//        ArgumentValidator.notNull(primitiveValueType, "primitiveValueType");
//        this.primitiveValueType = primitiveValueType;
//    }
//
//    public Object read(DataInput dataInput) throws UnknownValueTypeEncodingException {
//        return primitiveValueType.read(dataInput);
//    }

//    private List readMultiValue(DataInput dataInput) throws UnknownValueTypeEncodingException {
//        List result = new ArrayList();
//        int nrOfValues = dataInput.readInt();
//        for (int i = 0 ; i < nrOfValues; i++) {
//            Object value = null;
//            if (isHierarchical()) {
//                value = readHierarchical(dataInput);
//            } else {
//                value = primitiveValueType.read(dataInput);
//            }
//            result.add(value);
//        }
//        return result;
//    }
//
//    private HierarchyPath readHierarchical(DataInput dataInput) throws UnknownValueTypeEncodingException {
//        List<Object> result = new ArrayList<Object>();
//        int nrOfValues = dataInput.readInt();
//        for (int i = 0 ; i < nrOfValues; i++) {
//            result.add(primitiveValueType.read(dataInput));
//        }
//        return new HierarchyPath(result.toArray(new Object[result.size()]));
//    }

//    /**
//     * Format of the bytes written
//     * - Multivalue count : int of 4 bytes (if it is multivalue)
//     *     - Hierarchy count : int of 4 bytes (if it is hierarchycal as well)
//     *         - Value bytes
//     * - Hierarchy count : int of 4 bytes (if it is only hierarchycal)
//     *     - Value bytes
//     * - Value bytes (if it is neither multivalue nor hierarchycal)        
//     * 
//     * <p> IMPORTANT: Any changes on this format has an impact on the {@link ContainsValueComparator}
//     */
//    public byte[] toBytes(Object value) {
//        DataOutput dataOutput = new DataOutputImpl();
//        if (isMultiValue()) {
//            writeMultiValue(value, dataOutput);
//        } else if (isHierarchical()) {
//            writeHierarchical(value, dataOutput);
//        } else {
//            primitiveValueType.write(value, dataOutput);
//        }
//        return dataOutput.toByteArray();
//    }
//
//    private void writeMultiValue(Object value, DataOutput dataOutput) {
//        List<Object> values = ((List<Object>) value);
//        dataOutput.writeInt(values.size());
//        for (Object element : values) {
//            if (isHierarchical()) {
//                writeHierarchical(element, dataOutput);
//            } else {
//                primitiveValueType.write(element, dataOutput);
//            }
//        }
//    }
//
//    private void writeHierarchical(Object value, DataOutput dataOutput) {
//        Object[] elements = ((HierarchyPath) value).getElements();
//        dataOutput.writeInt(elements.length);
//        for (Object element : elements) {
//            primitiveValueType.write(element, dataOutput);
//        }
//    }
//
//    public PrimitiveValueType getPrimitive() {
//        return primitiveValueType;
//    }
//
//    public Class getType() {
//        if (isMultiValue()) {
//            return List.class;
//        }
//        return primitiveValueType.getType();
//    }
//
//    public byte[] toBytes() {
//        StringBuilder stringBuilder = new StringBuilder();
//        stringBuilder.append(primitiveValueType.getName());
//        stringBuilder.append(",");
//        stringBuilder.append(Boolean.toString(isMultiValue()));
//        stringBuilder.append(",");
//        stringBuilder.append(Boolean.toString(isHierarchical()));
//        return Bytes.toBytes(stringBuilder.toString());
//    }
//
//    public Set<Object> getValues(Object value) {
//        Set<Object> result = new HashSet<Object>();
//        if (isMultiValue()) {
//            result.addAll(getMultiValueValues(value));
//        } else if (isHierarchical()) {
//            result.addAll(getHierarchyValues(value));
//        } else {
//            result.add(value);
//        }
//        return result;
//    }
//
//    private Set<Object> getMultiValueValues(Object value) {
//        Set<Object> result = new HashSet<Object>();
//        if (isHierarchical()) {
//            for (Object element : ((List<Object>) value)) {
//                result.addAll(getHierarchyValues(element));
//            }
//        } else {
//            result.addAll((List<Object>) value);
//        }
//        return result;
//    }
//
//    private Set<Object> getHierarchyValues(Object value) {
//        Set<Object> result = new HashSet<Object>();
//        result.addAll(Arrays.asList(((HierarchyPath) value).getElements()));
//        return result;
//    }
//    
//    public static ValueType fromBytes(byte[] bytes, AbstractTypeManager typeManager) {
//        String encodedString = Bytes.toString(bytes);
//        int endOfPrimitiveValueTypeName = encodedString.indexOf(",");
//        String primitiveValueTypeName = encodedString.substring(0, endOfPrimitiveValueTypeName);
//        int endOfMultiValueBoolean = encodedString.indexOf(",", endOfPrimitiveValueTypeName + 1);
//        boolean multiValue = Boolean.parseBoolean(encodedString.substring(endOfPrimitiveValueTypeName + 1,
//                endOfMultiValueBoolean));
//        boolean hierarchical = Boolean.parseBoolean(encodedString.substring(endOfMultiValueBoolean + 1));
//        return typeManager.getValueType(primitiveValueTypeName, multiValue, hierarchical);
//    }
//
//    @Override
//    public int hashCode() {
//        final int prime = 31;
//        int result = 1;
//        result = prime * result + (multiValue ? 1231 : 1237);
//        result = prime * result + ((primitiveValueType == null) ? 0 : primitiveValueType.hashCode());
//        return result;
//    }
//
//    @Override
//    public boolean equals(Object obj) {
//        if (this == obj)
//            return true;
//        if (obj == null)
//            return false;
//        if (getClass() != obj.getClass())
//            return false;
//        ValueTypeImpl other = (ValueTypeImpl) obj;
//        if (multiValue != other.multiValue)
//            return false;
//        if (primitiveValueType == null) {
//            if (other.primitiveValueType != null)
//                return false;
//        } else if (!primitiveValueType.equals(other.primitiveValueType))
//            return false;
//        return true;
//    }
}
