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
package org.lilyproject.repository.api;

/**
 * The RecordTypeBuilder is a builder utility that helps in setting the properties of
 * a record type with the goal to create or update it on the repository. 
 * <p>
 * A new RecordTypeBuilder object should be created by calling {@link TypeManager#rtBuilder()}
 * <p>
 * Each call either returns a new RecordTypeBuilder object on which the next method can
 * be called, or returns an actual RecordType object.
 * <p>
 * The builder has the same requirements as a record type wrt creating or updating 
 * it on the repository, but it does not provide any other functionality that one 
 * can get from a record type. </br>
 * For instance, after creating the record type on the repository, the builder
 * object does not contain the id that was generated for the record type by the repository.
 * 
 */
public interface RecordTypeBuilder {

    /**
     * Sets the name of the record type
     * @return the builder
     */
    RecordTypeBuilder name(QName name);
    
    /**
     * Sets the id of the record type (to be used when updating a record type)
     * @return the builder
     */
    RecordTypeBuilder id(SchemaId id);
    
    /**
     * Adds a field type to the record type
     * @param id SchemaId of the field type
     * @param mandatory true if it is a mandatory field
     * @return the builder
     */
    RecordTypeBuilder field(SchemaId id, boolean mandatory);
    
    /**
     * Clears all data from the builder.
     * @return the builder
     */
    RecordTypeBuilder reset() throws TypeException;

    /**
     * Creates a new record type on the repository with the properties
     * that were added to the builder.
     * @see {@link TypeManager#createRecordType(RecordType)}
     * @return the created record type
     */
    RecordType create() throws RepositoryException, InterruptedException ;
    
    /**
     * Updates a record type on the repository with the properties
     * that were added to the builder.
     * @see {@link TypeManager#updateRecordType(RecordType)}
     * @return the updated record type
     */
    RecordType update() throws RepositoryException, InterruptedException ;
    
    /**
     * Returns a RecordType object containing the properties that were added
     * to the builder without actually creating it on the repository.
     * @return the record type
     */
    RecordType newRecordType();
}
