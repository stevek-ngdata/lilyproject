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
package org.lilycms.repository.api;

/**
 * The {@link ValueType} represents the type of the actual values of a
 * {@link FieldType}. A {@link ValueType} encapsulates a
 * {@link PrimitiveValueType} to be used by the value or values of the
 * {@link Field} following this {@link ValueType}. The implementors of this
 * interface are responsible for the encoding and decoding of the values
 * represented by their type.
 * 
 */
public interface ValueType {

    /**
     * If this type represents a multi value field.
     */
    boolean isMultiValue();
    
    /**
     * If this type represents a {@link HierarchyPath}.
     */
    boolean isHierarchical();

    /**
     * Decodes a byte[] to an object of the type represented by this {@link ValueType}. See {@link ValueType#getType()} 
     */
    public Object fromBytes(byte[] value);

    /**
     * Encodes an object of the type represented by this {@link ValueType} to a byte[].
     */
    byte[] toBytes(Object value);

    /**
     * @return the {@link PrimitiveValueType}
     */
    PrimitiveValueType getPrimitive();
    
    /**
     * @return the actual {@link Class} object represented by this {@link ValueType} (e.g. {@link String}). In case of multi value, this will be a {@link List}.
     */
    Class getType();

    /**
     * @return an encoded byte[] representing the {@link ValueType}
     */
    byte[] toBytes();
    
    boolean equals(Object obj);
}
