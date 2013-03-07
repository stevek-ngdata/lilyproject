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

import java.util.List;
import java.util.Map;

/**
 * A Record is the core entity managed by the {@link Repository}.
 *
 * <p>A Record can be instantiated via {@link Repository#newRecord() Repository.newRecord} or retrieved via
 * {@link Repository#read(RecordId) Repository.read}. As all entities within this API, records are dumb data objects.
 *
 * <p>All Record-related CRUD operations are available on the {@link Repository} interface:
 * {@link Repository#create(Record) Create}, {@link Repository#read(RecordId) Read},
 * {@link Repository#update(Record) Update}, {@link Repository#delete(RecordId) Delete}.
 *
 * <p>A Record object is not necessarily a representation of a complete record.
 * When {@link Repository#read(RecordId, java.util.List) reading}
 * a record, you can specify to only read certain fields. Likewise, when {@link Repository#update(Record) updating},
 * you only need to put the fields in this object that you want to be updated. But it is not necessary to remove
 * unchanged fields from this object, the repository will compare with the current situation and ignore
 * unchanged fields.
 *
 * <p>Since for an update, this object only needs to contain the fields you want to update, fields that are
 * not in this object will not be automatically removed from the record. Rather, you have to say explicitly which
 * fields should be deleted by adding them to the {@link #getFieldsToDelete() fields-to-delete} list. If the
 * fields-to-delete list contains field names that do not exist in the record, then these will be ignored upon
 * update, rather than causing an exception.
 *
 * <p>The {@link RecordType} and its version define the schema of the record. As is explained in more detail in
 * Lily's repository model documentation, a record has a pointer to three (possibly) different record types, one
 * for each scope.
 *
 */
public interface Record {
    void setId(RecordId recordId);

    RecordId getId();

    void setVersion(Long version);

    /**
     * Returns the version.
     *
     * <p>For a record without versions, this returns null. In all other cases, this returns the version number
     * of the loaded version (= the latest one by default), even if none of the versioned fields would actually be
     * loaded.
     */
    Long getVersion();

    /**
     * Sets the record type and record type version.
     *
     * <p>This actually sets the record type of the non-versioned scope, which is considered to be the primary
     * record type. Upon save, the record types of the other scopes will also be set to this record type (if there
     * are any fields changed in those scopes, thus if a new version will be created).
     *
     * @param version version number, or null if you want the repository to pick the last version available when
     *                storing the record.
     */
    void setRecordType(QName name, Long version);

    /**
     * Shortcut for setRecordType(name, null)
     */
    void setRecordType(QName name);

    /**
     * Returns the record type of the non-versioned scope.
     */
    QName getRecordTypeName();

    /**
     * Returns the record type version of the non-versioned scope.
     */
    Long getRecordTypeVersion();

    /**
     * Sets the record type of a specific scope
     */
    void setRecordType(Scope scope, QName name, Long version);

    /**
     * Returns the record type of a specific scope
     */
    QName getRecordTypeName(Scope scope);

    /**
     * Returns the record type version of a specific scope
     */
    Long getRecordTypeVersion(Scope scope);

    /**
     * Sets a field to a value, possibly replacing a previous value.
     *
     * <p>The provided QName should be the QName of {@link FieldType} that exists within the repository.
     *
     * <p>The type of the given value should correspond to the
     * {@link ValueType#getClass() value type} of the {@link FieldType#getValueType() field type}.
     */
    void setField(QName fieldName, Object value);

    /**
     * Deletes a field from this object and optionally adds it to the list of fields to delete upon save.
     *
     * <p>If the field is not present, this does not throw an exception. In this case, the field will still
     * be added to the list of fields to delete. Use {@link #hasField} if you want to check if a field is
     * present in this Record instance.
     *
     * @param addToFieldsToDelete if false, the field will only be removed from this value object, but not be added
     *                            to the {@link #getFieldsToDelete}.
     */
    void delete(QName fieldName, boolean addToFieldsToDelete);

    /**
     * Gets a field, throws an exception if it is not present.
     *
     * <p>To do anything useful with the returned value, you have to cast it to the type you expect.
     * See also {@link ValueType}.
     *
     * @throws FieldNotFoundException if the field is not present in this Record object. To avoid the exception,
     *         use {@link #hasField}.
     */
    <T> T getField(QName fieldName) throws FieldNotFoundException;

    /**
     * Checks if the field with the given name is present in this Record object.
     */
    boolean hasField(QName fieldName);

    /**
     * Returns the map of fields in this Record.
     *
     * <p>Important: changing this map or the values contained within them will alter the content of this Record.
     */
    Map<QName, Object> getFields();

    /**
     * Adds the given fields to the list of fields to delete.
     */
    void addFieldsToDelete(List<QName> fieldNames);

    /**
     * Removes the given fields to the list of fields to delete.
     */
    void removeFieldsToDelete(List<QName> fieldNames);

    /**
     * Gets the lists of fields to delete. Modifying the returned list modifies the state of this Record object.
     *
     * <p>This list will be used by {@link Repository#update(Record)} to delete fields.
     *
     * <p>If a record contains the field both as a normal field and in the list of fields to delete, updating the record
     * will result in the field to be deleted rather than updated. So, deleting a field takes preference over updating a field.
     */
    List<QName> getFieldsToDelete();

    /**
     * After performing a create or update, gives some information as to whether a record was created, updated,
     * or unchanged. Especially useful when using {@link Repository#createOrUpdate}.
     *
     * <p>This property is output-only (= assigned by the repository, ignored on input) and ephemeral (not stored).
     */
    ResponseStatus getResponseStatus();

    void setResponseStatus(ResponseStatus status);

    /**
     * Creates a clone of the record object
     * <p>
     * A new record object is created with the same id, version, recordTypes,
     * fields and deleteFields.
     * <p>
     * Of the fields that are not of an immutable type, a deep copy is
     * performed. This includes List, HierarchyPath, Blob and Record.
     * <p>
     * The response status is not copied into the new Record object.
     *
     * @throws RuntimeException
     *             when a record is nested in itself
     */
    Record clone() throws RuntimeException;

    /**
     * Creates a clone of the record object
     * <p>
     * A new record object is created with the same id, version, recordTypes,
     * fields and deleteFields. The response status is not copied into the
     * new Record object.
     * <p>
     * Of the fields that are not of an immutable type, a deep copy is
     * performed. This includes List, HierarchyPath, Blob and Record.
     * <p>
     * When checking if the record is nested in itself, it is also checked if
     * the record is contained in the map of parentRecords
     *
     * @param parentRecords
     *            a stack of parent records of the record used to check if the record
     *            is nested in itself or one of its parents.
     * @throws RecordException
     *             when the record is contained in the parentRecords map or if
     *             it is nested in itself.
     */
    Record cloneRecord(IdentityRecordStack parentRecords) throws RecordException;

    /**
     * Shortcut method for {@link #cloneRecord(IdentityRecordStack)} with
     * parentRecords set to an empty stack
     */
    Record cloneRecord() throws RecordException;

    boolean equals(Object obj);

    /**
     * Compares the two records, ignoring some aspects. This method is intended to compare
     * a record loaded from the repository with a newly instantiated one in which some things
     * are typically not supplied.
     *
     * <p>The aspects which are ignored are:
     * <ul>
     *   <li>the version
     *   <li>the record types: only the name of the non-versioned record type is compared, if
     *       it is not null in both. The version of the record type is not compared.
     * </ul>
     */
    boolean softEquals(Object obj);

    /**
     * Sets a default namespace on a record object.
     * <p>
     * The default namespace is used to resovle a QName when only the name part is given,
     * like for instance in {@link #getField(String)}. </br>
     * When resolving a name into a QName, it is first checked if the default namespace is set.
     * If not, the namespace of the recordType is used. If that is not set either,
     * a RecordException is thrown. </br>
     * The resolving happens at the moment a related method is called.
     * This means that if the default namespace or record type is changed afterwards, this has
     * no influence on the already resolved QNames.</br>
     * <p>
     * The default namespace is only used for resolving names into QNames. It is not sent to
     * the server, nor stored on the repository.
     *
     * @param namespace the default namespace to use
     */
    void setDefaultNamespace(String namespace);

    /**
     * Resolves the QName of the record type and sets it on the record.
     * <p>
     * @see {@link #setDefaultNamespace(String)}
     * @see {@link #setRecordType(QName)}
     * @param recordTypeName the name part of the record type to set
     * @throws RecordException when the QName cannot be resolved
     */
    void setRecordType(String recordTypeName) throws RecordException;

    /**
     * Resolves the QName of the record type and sets it on the record.
     * <p>
     * @see {@link #setDefaultNamespace(String)}
     * @see {@link #setRecordType(QName, Long)}
     * @param recordTypeName the name part of the record type to set
     * @throws RecordException when the QName cannot be resolved
     */
    void setRecordType(String recordTypeName, Long version) throws RecordException;

    /**
     * Resolves the QName of the record type and sets it on the record.
     * <p>
     * @see {@link #setDefaultNamespace(String)}
     * @see {@link #setRecordType(Scope, QName, Long)}
     * @param recordTypeName the name part of the record type to set
     * @throws RecordException when the QName cannot be resolved
     */
    void setRecordType(Scope scope, String recordTypeName, Long version) throws RecordException;

    /**
     * Resolves the QName of the field and sets it on the record.
     * <p>
     * @see {@link #setDefaultNamespace(String)}
     * @see {@link #setField(QName)}
     * @param fieldName the name part of the field to set
     * @param value the value to set on the field
     * @throws RecordException when the QName cannot be resolved
     */
    void setField(String fieldName, Object value) throws RecordException;

    /**
     * Resolves the QName of the field and gets it from the record.
     * <p>
     * @see {@link #setDefaultNamespace(String)}
     * @see {@link #getField(QName)}
     * @param fieldName the name part of the field to get
     * @return the value of the field
     * @throws FieldNotFoundException if the field does not exist on the record
     * @throws RecordException when the QName cannot be resolved
     */
    <T> T getField(String fieldName) throws FieldNotFoundException, RecordException;

    /**
     * Resolves the QName of the field and deletes it from the record.
     *
     * @see {@link #setDefaultNamespace(String)}
     * @see {@link #delete(QName, boolean)}
     * @param fieldName the name part of the field to delete
     * @param addToFieldsToDelete true if the field needs to be deleted from the record in the repository as well
     * @throws RecordException when the QName cannot be resolved
     */
    void delete(String fieldName, boolean addToFieldsToDelete) throws RecordException;

    /**
     * Resolves the QName of the field and checks if it exists.
     *
     * @see {@link #setDefaultNamespace(String)}
     * @see {@link #hasField(QName)}
     * @param fieldName the name part of the field to check
     * @return true if the field exists on the record
     * @throws RecordException when the QName cannot be resolved
     */
    boolean hasField(String fieldName) throws RecordException;

    /**
     * Gets the transient set of attributes associated with this record. The returned map is never null
     * and can be modified. For more details on transient attributes see {@link #setAttributes(Map)}.
     */
    Map<String, String> getAttributes();

    /**
     * Returns true if attributes are set in this record object. This method is cheaper than
     * calling {@link #getAttributes()} when the record has no attributes. See {@link #setAttributes(Map)}.
     */
    boolean hasAttributes();

    /**
     * Set the transient attributes associated with this record.
     *
     * <p>These transient attributes are not persisted in the repository, thus they do not behave like fields.
     * As such, they are not returned when doing a {@link Repository#read}. Instead, the attributes are
     * related to a particular create/update/delete operation.</p>
     *
     * <p>The purpose of these attributes is to be able to pass along data/hints to certain
     * components, such as server-side repository decorators, secondary actions, message queue listeners
     * and the like. The attributes are stored as part of the MQ payload of a repository mutation event.</p>
     *
     * <p>The attributes are not preserved when cloning the record, or on a round-trip from the repository,
     * thus in the record object returned from create, update, etc.</p>
     */
    void setAttributes(Map<String, String> attributes);

    /**
     * Returns the {@link Metadata} object for the specified field.
     *
     * <p>For more information on metadata, see {@link #setMetadata(QName, Metadata)}.</p>
     *
     * @return null if there is no metadata for the field
     */
    Metadata getMetadata(QName fieldName);

    /**
     * Sets the metadata for some field.
     *
     * <p>Metadata is a set of schema-free key-values associated with a field value. It can be used for metadata
     * about the value itself, for example to describe its source, creator or quality. Such information could
     * also be modelled within the Lily schema (for example using complex field types), but this could make the
     * schema very complex.</p>
     *
     * <p>If you set metadata for a field which does not exist, it will be ignored.</p>
     *
     * <p>Similar to record fields, metadata can be partially updated: you only need to specify the metadata keys you
     * want to change, the other metadata will be inherited from the current record state. Therefore, deleting
     * metadata needs to be done explicitly through {@link MetadataBuilder#delete(String)}</p>.
     *
     * <p>For versioned fields, changing only the metadata (without changing the field value itself) will also
     * cause a new version to be created.</p>
     *
     * <p>Metadata is currently not supported for versioned-mutable fields.</p>
     */
    void setMetadata(QName fieldName, Metadata metadata);

    /**
     * Returns the metadata for all fields.
     *
     * <p>The returned map can be manipulated, except if it is empty, because in that case a
     * Collections.emptyCollection() is returned.</p>
     */
    Map<QName, Metadata> getMetadataMap();
}
