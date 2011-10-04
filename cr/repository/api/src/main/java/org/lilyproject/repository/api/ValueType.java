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
import java.util.IdentityHashMap;
import java.util.Set;

import org.lilyproject.bytes.api.DataInput;
import org.lilyproject.bytes.api.DataOutput;

/**
 * A value type represents the type of the value of a {@link FieldType}.
 * 
 * <p>
 * It represents a particular kind of value like a String, Long, ...
 * 
 * <p>
 * Value types exist also to represent java.util.List (ListValueType) and
 * {@link HierarchyPath} (PathValueType) which can contain values of another
 * value type including ListValueType and PathValueType again thus allowing
 * multiple levels of nesting.</br>
 * 
 * <p>
 * A value type to represent a Record also exists. This record is used to
 * represent what is also known as a complex type. It can be regarded as normal
 * Record, but with a few restrictions:
 * <li>It has no version</li>
 * <li>It has one (non-versioned) record type, and all its fields must be
 * defined in that record type</li>
 * <li>All its fields are non-versioned</li>
 * <li>It is stored in its entirety inside a field of the surrounding record</li>
 * <li>Blob fields are not allowed</li>
 * 
 * <p>
 * It is the responsibility of a ValueType to convert the values to/from byte
 * representation, as used for storage in the repository.
 * 
 * <p>
 * A value type should be requested from the {@link TypeManager} using the
 * method {@link TypeManager#getValueType(String, String)}. A name (e.g.
 * "STRING") uniquely represents the value type, and some value types can
 * include extra information (typeParams) defining the value type. For example
 * to define a list should contain strings. <br>
 * See the javadoc of that method for the complete list of supported value types
 * and the parameters needed to get an instance of them.
 * 
 */
public interface ValueType {
 
    /**
     * Read and decodes object of the type represented by this value type from a {@link DataInput}.
     * 
     * <p>The ValueType is itself responsible for encoding (see {@link #write(Object, DataOutput)}) and decoding data. 
     * 
     * @param dataInput the DataInput from which the valueType should read and decode its data 
     * @throws UnknownValueTypeEncodingException 
     * @throws InterruptedException 
     * @throws RepositoryException 
     */
    <T> T read(DataInput dataInput) throws UnknownValueTypeEncodingException, RepositoryException, InterruptedException;

    <T> T read(byte[] data) throws UnknownValueTypeEncodingException, RepositoryException, InterruptedException;
    
    /**
     * Encodes an object of the type represented by this value type to a {@link DataOutput}.
     *
     * <p>The ValueType is itself responsible for encoding (see {@link #write(Object, DataOutput)}) and decoding data.
     *  
     * @param value the object to encode and write
     * @param dataOutput the DataOutput on which to write the data
     * @throws RepositoryException
     * @throws InterruptedException
     */
    // void write(Object value, DataOutput dataOutput) throws
    // RepositoryException, InterruptedException;
    
    void write(Object value, DataOutput dataOutput, IdentityHashMap<Record, Object> parentRecords)
            throws RepositoryException, InterruptedException;

    /**
     * Encodes an object of the type represented by this value type to a byte[].
     * 
     * <p>Should only be used internally for Avro data transport.
     * 
     * @throws InterruptedException 
     * @throws RepositoryException 
     */
    byte[] toBytes(Object value, IdentityHashMap<Record, Object> parentRecords) throws RepositoryException,
            InterruptedException;

    /**
     * Returns the Java class for the values of this value type.
     * 
     * <p>
     * For example: java.util.List
     */
    Class getType();

    /**
     * Returns a set of all values contained in this value.
     * <p>
     * In case of a ListValueType or PathValueType, these are the nested values.
     */
    Set<Object> getValues(Object value);
    
    public int hashCode();

    boolean equals(Object obj);
    
    /**
     * A comparator that can compare the values corresponding to this value type.
     *
     * <p>If comparing values is not supported, null is returned.</p>
     *
     * <p>This method should be lightweight to call, so preferably return the same instance on each invocation.</p>
     */
    Comparator getComparator();
    
    /**
     * @return the base name of the value type (e.g. "STRING") without any extra parameters for the type.
     * See {@link TypeManager#getValueType(String)} for a list of all possible value types and their names.
     */
    String getBaseName();
    
    /**
     * @return the name of the value type where the optional parameters of the type are  
     * enclosed in "&lt;&gt;" after the simple name. For example: "LIST&lt;STRING&gt;"   
     */
    String getName();

    /**
     * ListValueType and PathValueType can again contain other value types.
     * 
     * <p>
     * This method returns the nested value type (1 level deep) or itself if it
     * is not a ListValueType or PathValueType.
     */
    ValueType getNestedValueType();

    /**
     * ListValueType and PathValueType can again contain other value types.
     * 
     * <p>
     * This method returns the deepest level (non List or Path) value type.
     */
    ValueType getDeepestValueType();

    /**
     * ListValueType and PathValueType can again contain other value types.
     * <p>
     * This method returns the number of nesting levels until the base value
     * type is reached.<br/>
     * For non List or Path value types the returned value is 1.
     * 
     * <p>
     * This method is used by the Repository and BlobIncubator when checking if
     * a blob is already used by the record.
     */
    int getNestingLevel();

    /**
     * @return true in case of a ListValueType, false in all other cases.
     */
    boolean isMultiValue();
    

    /**
     * @return true in case of a PathValueType or if a PathValueType is nested in a ListValueType 
     */
    boolean isHierarchical();
}
