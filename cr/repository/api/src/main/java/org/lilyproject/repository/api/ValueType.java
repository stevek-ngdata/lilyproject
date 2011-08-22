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
package org.lilyproject.repository.api;

import java.util.Comparator;
import java.util.Set;

import org.lilyproject.bytes.api.DataInput;
import org.lilyproject.bytes.api.DataOutput;

/**
 * A value type represents the type of the value of a {@link FieldType}.
 *
 * <p>It consists of:
 * <ul>
 *  <li>A {@link ValueType}: this is a particular kind of value like a string, long, ... It could
 *      also be a composite value, e.g. a coordinate with x and y values.
 * </ul>
 *
 * <p>A field can be either multi-value or hierarchical, or both at the same time. In the last case, the value
 * is a java.util.List of {@link HierarchyPath} objects, not the other way round.
 *
 * <p>So you can have a multi-valued string, a multi-valued date, a single-valued hierarchical string path,
 * a multi-valued hierarchical string path, ...
 *
 * <p>It is the responsibility of a ValueType to convert the values to/from byte representation, as used for
 * storage in the repository. This should delegate to the PrimitiveValueType for the conversion of a single value.
 *
 * <p>It is not necessary to create value types in the repository, they simply exist for any kind of primitive
 * value type. You can get access to ValueType instances via {@link TypeManager#getValueType(String, boolean, boolean)}.
 *
 */
public interface ValueType {

    /**
     * Decodes a byte[] to an object of the type represented by this {@link ValueType}. See {@link ValueType#getType()} 
     * @throws UnknownValueTypeEncodingException 
     * @throws InterruptedException 
     * @throws RepositoryException 
     */
    <T> T read(DataInput dataInput, Repository repository) throws UnknownValueTypeEncodingException, RepositoryException, InterruptedException;
    

    void write(Object value, DataOutput dataOutput) throws RepositoryException, InterruptedException;
    
    /**
     * Encodes an object of the type represented by this {@link ValueType} to a byte[].
     * @throws InterruptedException 
     * @throws RepositoryException 
     */
    byte[] toBytes(Object value) throws RepositoryException, InterruptedException;

    /**
     * Returns the Java {@link Class} object for the values of this value type.
     */
    Class getType();

    String getTypeParams();
    
    /**
     * Returns a set of all values contained in this value.
     * It flattens out the aspects of multivalue and hierarchy values.
     */
    Set<Object> getValues(Object value);
    
    boolean equals(Object obj);
    
    /**
     * A comparator that can compare the values corresponding to this value type.
     *
     * <p>If comparing values is not supported, null is returned.</p>
     *
     * <p>This method should be lightweight to call, so preferably return the same instance on each invocation.</p>
     */
    Comparator getComparator();
    
    String getName();
    
    String getFullName();
    
    ValueType getBaseValueType();
    
    ValueType getNestedValueType();
    
    int getNestingLevel();
    
    boolean isMultiValue();
    
    boolean isHierarchical();
}
