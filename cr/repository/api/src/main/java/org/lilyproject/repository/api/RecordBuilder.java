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

import java.util.Map;

/**
 * The RecordBuilder is a builder utility that helps in setting the properties of
 * a record with the goal to create or update it on the repository.
 * <p/>
 * A new RecordBuilder object should be created by calling {@link Repository#recordBuilder()}
 * <p/>
 * Each call either returns a new RecordBuilder object on which the next method can
 * be called, or returns an actual Record object.
 * <p/>
 * The builder has the same requirements as a record wrt creating or updating
 * it on the repository, but it does not provide any other functionality that one
 * can get from a record. </br>
 * For instance, after creating the record on the repository, the builder
 * object does not contain the id that was generated for the record by the repository.
 */
public interface RecordBuilder {

    /**
     * Sets the default namespace to use when setting the record type or field by
     * only using the name part of its QName.
     *
     * @param namespace the default namespace to set
     * @return the builder
     * @see {@link Record#setDefaultNamespace(String)}
     */
    RecordBuilder defaultNamespace(String namespace);

    /**
     * Sets record type of the record.
     *
     * @param name QName of the record type
     * @return the builder
     * @see {@link Record#setRecordType(QName)}
     */
    RecordBuilder recordType(QName name);

    /**
     * Sets record type of the record.
     *
     * @param name    QName of the record type
     * @param version the version of the record type
     * @return the builder
     * @see {@link Record#setRecordType(QName, Long)}
     */
    RecordBuilder recordType(QName name, Long version);

    /**
     * Sets record type of the record using only the name part of its QName.
     *
     * @param name QName of the record type
     * @return the builder
     * @see {@link Record#setDefaultNamespace(String)}
     * @see {@link Record#setRecordType(String)}
     */
    RecordBuilder recordType(String name) throws RecordException;

    /**
     * Sets record type of the record using only the name part of its QName.
     *
     * @param name    QName of the record type
     * @param version the version of the record type
     * @return the builder
     * @see {@link Record#setDefaultNamespace(String)}
     * @see {@link Record#setRecordType(String)}
     */
    RecordBuilder recordType(String name, Long version) throws RecordException;

    /**
     * Sets the version of the record (to be used when performing a mutable update)
     *
     * @param version the version of the record
     * @return the builder
     * @see {@link Record#setVersion(Long)}
     */
    RecordBuilder version(Long version);

    /**
     * Sets the id of the record (to be used when updating a record)
     *
     * @param id the RecordId
     * @return the builder
     * @see {@link Record#setId(RecordId)}
     */
    RecordBuilder id(RecordId id);

    /**
     * Set the id of the record to the master id of the given id (= the record id without any
     * variant properties), extended with the supplied variant properties.
     *
     * @see {@link IdGenerator#newRecordId(String, java.util.Map)}
     */
    RecordBuilder id(RecordId id, Map<String, String> variantProperties);

    /**
     * Set the id of the record to a user specified ID.
     *
     * @see {@link IdGenerator#newRecordId(String)}
     */
    RecordBuilder id(String userId);

    /**
     * Set the id of the record to a user specified id and the supplied
     * variant properties.
     *
     * @see {@link IdGenerator#newRecordId(String, java.util.Map)}
     */
    RecordBuilder id(String userId, Map<String, String> variantProperties);

    /**
     * Set the id of the record to a newly generated UUID.
     *
     * @see {@link IdGenerator#newRecordId()}
     */
    RecordBuilder assignNewUuid();

    /**
     * Set the id of the record to a newly generated UUID and the supplied
     * variant properties.
     *
     * @see {@link IdGenerator#newRecordId(java.util.Map)}
     */
    RecordBuilder assignNewUuid(Map<String, String> variantProperties);

    /**
     * Adds a field to the record
     *
     * @param name  the QName of the field
     * @param value the value of the field
     * @return the builder
     * @see {@link Record#setField(QName, Object)}
     */
    RecordBuilder field(QName name, Object value);

    /**
     * Adds a field to the record without specifying its namespace
     *
     * @param name  the name part of the QName of the field
     * @param value the value of the field
     * @return the builder
     * @see {@link Record#setDefaultNamespace(String)}
     * @see {@link Record#setField(String, Object)}
     */
    RecordBuilder field(String name, Object value) throws RecordException;

    /**
     * Returns a new, nested, builder to create a record to set as value
     * in a record field. Call {@link #set()} to return to the current
     * record's builders.
     */
    RecordBuilder recordField(String name) throws RecordException;

    /**
     * Returns a new, nested, builder to create a record to set as value
     * in a RECORD field. Call {@link #set()} to return to the current
     * record's builders.
     */
    RecordBuilder recordField(QName name) throws RecordException;

    /**
     * Returns a new, nested, builder to create a list of records to set
     * as value in a LIST&lt;RECORD> field. After creating each record, call
     * {@link #add}, after the last record, call {@link #endList} instead
     * of add.
     */
    RecordBuilder recordListField(String name) throws RecordException;

    /**
     * Returns a new, nested, builder to create a list of records to set
     * as value in a LIST&lt;RECORD> field. After creating each record, call
     * {@link #add}, after the last record, call {@link #endList} instead
     * of add.
     */
    RecordBuilder recordListField(QName name) throws RecordException;

    /**
     * Adds a mutation condition that should be checked when updating a record.
     * <p/>
     * When calling this method several times, the order of the method calls
     * defines the order of the mutation conditions.
     *
     * @param condition the MutationCondition to add
     * @return the builder
     * @see {@link Repository#update(Record, boolean, boolean, java.util.List)}
     */
    RecordBuilder mutationCondition(MutationCondition condition);

    /**
     * Indicates if an update of the record should be a mutable update.
     * <p/>
     * When this is put to true, the version of the record to update should also
     * be set.
     *
     * @param updateVersion true if a specific version of the record should be updated (default: false)
     * @return the builder
     * @see {@link Repository#update(Record, boolean, boolean, java.util.List)}
     */
    RecordBuilder updateVersion(boolean updateVersion);

    /**
     * Indicates if the latest version of the record type should be used when updating
     * the record.
     *
     * @param latestRT if the latest version of the record type should be used (default: true)
     * @return the builder
     * @see {@link Repository#update(Record, boolean, boolean, java.util.List)}
     */
    RecordBuilder useLatestRecordType(boolean latestRT);

    /**
     * Adds an attribute to the record, this is <b>transient</b> data attached to a
     * create/update/delete operation. Attributes are not returned on read.
     *
     * @return the builder
     */
    RecordBuilder attribute(String name, String value);

    /**
     * Clears all data from the builder object.
     *
     * @return the builder
     */
    RecordBuilder reset() throws RecordException;

    /**
     * Creates a record on the repository using the properties that have been
     * added to the builder.
     *
     * @return the created record
     * @see {@link Repository#create(Record)}
     */
    Record create() throws RepositoryException, InterruptedException;

    /**
     * Creates a record on the repository using the properties that have been
     * added to the builder.
     *
     * @return the created record
     * @see {@link Repository#createOrUpdate(Record)}
     */
    Record createOrUpdate() throws RepositoryException, InterruptedException;

    /**
     * Updates a record on the repository using the properties that have been
     * added to the builder.
     *
     * @return the updated record
     * @see {@link Repository#update(Record)}
     */
    Record update() throws RepositoryException, InterruptedException;

    /**
     * Returns a record object containing the properties that have been added
     * to the builder without actually creating it on the repository.
     *
     * @return a record
     */
    Record build();

    /**
     * Finishes the creation of a nested record and sets it as field value in
     * the parent record. The parent record builder is returned.
     * See {@link #recordField(String)}.
     */
    RecordBuilder set();

    /**
     * Finishes the creation of a nested record for a list record field.
     * After calling this, a new record builder is returned to create the
     * next record in the list. To finish the list, call {@link #endList()}.
     * See {@link #recordListField(String)}.
     */
    RecordBuilder add() throws RecordException;

    /**
     * Finishes the creation of a nested record for a list record field,
     * and also finishes the creation of the complete list. The list
     * will be set as field value in the parent record, and the parent record
     * builder is returned.
     */
    RecordBuilder endList();
}
