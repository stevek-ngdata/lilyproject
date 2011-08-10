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
 * The RecordBuilder is a builder utility that helps in setting the properties of
 * a record with the goal to create or update it on the repository. 
 * <p>
 * A new RecordBuilder object should be created by calling {@link Repository#recordBuilder()}
 * <p>
 * Each call either returns a new RecordBuilder object on which the next method can
 * be called, or returns an actual Record object.
 * <p>
 * The builder has the same requirements as a record wrt creating or updating 
 * it on the repository, but it does not provide any other functionality that one 
 * can get from a record. </br>
 * For instance, after creating the record on the repository, the builder
 * object does not contain the id that was generated for the record by the repository.
 * 
 */
public interface RecordBuilder {

    /**
     * Sets the default namespace to use when setting the record type or field by
     * only using the name part of its QName.
     * @see {@link Record#setDefaultNamespace(String)}
     * @param namespace the default namespace to set
     * @return the builder
     */
    RecordBuilder defaultNameSpace(String namespace);
    
    /**
     * Sets record type of the record.
     * @see {@link Record#setRecordType(QName)}
     * @param name QName of the record type
     * @return the builder
     */
    RecordBuilder recordType(QName name);

    /**
     * Sets record type of the record.
     * @see {@link Record#setRecordType(QName, Long)}
     * @param name QName of the record type
     * @param the version of the record type
     * @return the builder
     */
    RecordBuilder recordType(QName name, Long version);

    /**
     * Sets record type of the record using only the name part of its QName.
     * @see {@link Record#setDefaultNamespace(String)}
     * @see {@link Record#setRecordType(String)}
     * @param name QName of the record type
     * @return the builder
     */
    RecordBuilder recordType(String name) throws RecordException;
    
    /**
     * Sets record type of the record using only the name part of its QName.
     * @see {@link Record#setDefaultNamespace(String)}
     * @see {@link Record#setRecordType(String)}
     * @param name QName of the record type
     * @param the version of the record type
     * @return the builder
     */
    RecordBuilder recordType(String name, Long version) throws RecordException;
    
    /**
     * Sets the version of the record (to be used when performing a mutable update)
     * @see {@link Record#setVersion(Long)}
     * @param version the version of the record
     * @return the builder
     */
    RecordBuilder version(Long version);
    
    /**
     * Sets the id of the record (to be used when updating a record)
     * @see {@link Record#setId(RecordId)}
     * @param id the RecordId
     * @return the builder
     */
    RecordBuilder recordId(RecordId id);
    
    /**
     * Adds a field to the record
     * @see {@link Record#setField(QName, Object)}
     * @param name the QName of the field
     * @param value the value of the field
     * @return the builder
     */
    RecordBuilder field(QName name, Object value);
    
    /**
     * Adds a field to the record without specifying its namespace
     * @see {@link Record#setDefaultNamespace(String)}
     * @see {@link Record#setField(String, Object)}    
     * @param name the name part of the QName of the field
     * @param value the value of the field
     * @return the builder
     */
    RecordBuilder field(String name, Object value) throws RecordException;
    
    /**
     * Adds a mutation condition that should be checked when updating a record.
     * <p>
     * When calling this method several times, the order of the method calls
     * defines the order of the mutation conditions.
     * @see {@link Repository#update(Record, boolean, boolean, java.util.List)}
     * @param condition the MutationCondition to add
     * @return the builder
     */
    RecordBuilder mutationCondition(MutationCondition condition);
    
    /**
     * Indicates if an update of the record should be a mutable update.
     * <p>
     * When this is put to true, the version of the record to update should also
     * be set.
     * @see {@link Repository#update(Record, boolean, boolean, java.util.List)}
     * @param updateVersion true if a specific version of the record should be updated (default: false)
     * @return the builder
     */
    RecordBuilder updateVersion(boolean updateVersion);
    
    /**
     * Indicates if the latest version of the record type should be used when updating
     * the record.
     * @see {@link Repository#update(Record, boolean, boolean, java.util.List)}
     * @param latestRT if the latest version of the record type should be used (default: true)
     * @return the builder
     */
    RecordBuilder useLatestRecordType(boolean latestRT);
    
    /**
     * Clears all data from the builder object.
     * @return the builder
     */
    RecordBuilder reset() throws RecordException;

    /**
     * Creates a record on the repository using the properties that have been
     * added to the builder.
     * @see {@link Repository#create(Record)}
     * @return the created record
     */
    Record create() throws RepositoryException, InterruptedException;
    
    /**
     * Creates a record on the repository using the properties that have been
     * added to the builder.
     * @see {@link Repository#createOrUpdate(Record)}
     * @return the created record
     */    
    Record createOrUpdate() throws RepositoryException, InterruptedException;
    
    /**
     * Updates a record on the repository using the properties that have been
     * added to the builder.
     * @see {@link Repository#update(Record)}
     * @return the updated record
     */ 
    Record update() throws RepositoryException, InterruptedException;
    
    /**
     * Returns a record object containing the properties that have been added
     * to the builder without actually creating it on the repository.
     * @return a record
     */
    Record newRecord();
}
