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

import java.io.Closeable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.lilyproject.util.Pair;

// IMPORTANT:
//   See the note on the requirement TypeException described in the Repository.java file.

/**
 * TypeManager provides access to the repository schema. This is where {@link RecordType}s and {@link FieldType}s
 * are managed.
 *
 * <p>For an in-depth description of the repository model, please see the Lily documentation.
 */
public interface TypeManager extends Closeable {

    /**
     * Instantiates a new RecordType object.
     *
     * <p>This is only a factory method, nothing is created in the repository.
     */
    RecordType newRecordType(QName name) throws TypeException;

    /**
     * Instantiates a new RecordType object.
     *
     * <p>This is only a factory method, nothing is created in the repository.
     */
    RecordType newRecordType(SchemaId recordTypeId, QName name) throws TypeException;

    /**
     * Instantiates a new RecordType object.
     *
     * <p>This is only a factory method, nothing is created in the repository.
     */
    RecordType newRecordType(SchemaId recordTypeId, QName name, Long version) throws TypeException;

    /**
     * Instantiates a new RecordType object.
     *
     * <p>This is only a factory method, nothing is created in the repository.
     */
    RecordType newRecordType(QName name, Map<SchemaId, Long> mixins,
            Collection<FieldTypeEntry> fieldTypeEntries) throws TypeException;

    /**
     * Instantiates a new RecordType object.
     *
     * <p>This is only a factory method, nothing is created in the repository.
     */
    RecordType newRecordType(SchemaId recordTypeId, QName name, Long version, Map<SchemaId, Long> mixins,
            Collection<FieldTypeEntry> fieldTypeEntries) throws TypeException;

    /**
     * Creates a RecordType in the repository.
     *
     * @throws RecordTypeExistsException   when a recordType with the same id already exists on the repository
     * @throws RecordTypeNotFoundException when a mixin of the recordType refers to a non-existing {@link RecordType}
     * @throws TypeException               when the given recordType has no name specified
     * @throws FieldTypeNotFoundException
     * @throws RepositoryException         when an unexpected exception occurs on the repository
     */
    RecordType createRecordType(RecordType recordType) throws RepositoryException, InterruptedException;

    /**
     * Gets a RecordType from the repository.
     *
     * @param version the version of the record type to return, or null for the latest version.
     * @throws RecordTypeNotFoundException when the recordType does not exist
     * @throws RepositoryException         when an unexpected exception occurs on the repository
     */
    RecordType getRecordTypeById(SchemaId id, Long version) throws RepositoryException, InterruptedException;

    /**
     * Gets a RecordType from the repository.
     *
     * @param version the version of the record type to return, or null for the latest version.
     * @throws RecordTypeNotFoundException when the recordType does not exist
     * @throws RepositoryException         when an unexpected exception occurs on the repository
     */
    RecordType getRecordTypeByName(QName name, Long version) throws RepositoryException, InterruptedException;

    /**
     * Updates an existing record type.
     *
     * <p>You can provide any RecordType object as argument, either one retrieved from TypeManager, for example
     * using {@link #getRecordTypeByName(QName, Long)} or a newly instantiated one, using {@link
     * #newRecordType(QName)}.
     *
     * <p>The state of the record type will be updated to correspond to the given RecordType object. This also
     * concerns the list of fields: any fields that were previously in the record type but are not present in
     * the provided RecordType object will be removed. This is different from {@link Record}s, where field deletion
     * is explicit.
     *
     * <p>Upon each update, a new version of the RecordType is created. The number of the created version is available
     * from the returned RecordType object.
     *
     * @throws RecordTypeNotFoundException when the recordType to be updated does not exist
     * @throws FieldTypeNotFoundException
     * @throws RepositoryException         when an unexpected exception occurs on the repository
     */
    RecordType updateRecordType(RecordType recordType) throws RepositoryException, InterruptedException;

    /**
     * Either creates or updates the record type, depending on whether it exists in
     * the repository, and depending on the information supplied in the record type.
     *
     * <p>When the ID is supplied of the record type, this method will always do
     * an update. If the name is supplied, it is checked whether it already exists,
     * if so the record type is updated, and otherwise it is created.
     *
     * @return the created or updated record type
     */
    RecordType createOrUpdateRecordType(RecordType recordType) throws RepositoryException, InterruptedException;

    /**
     * Get the list of all record types that exist in the repository. This returns the latest version of
     * each record type.
     */
    Collection<RecordType> getRecordTypes() throws RepositoryException, InterruptedException;

    /**
     * Instantiates a new FieldTypeEntry object.
     *
     * <p>This is only a factory method, nothing is created in the repository.
     *
     * <p>FieldTypeEntries can be added to {@link RecordType}s.
     */
    FieldTypeEntry newFieldTypeEntry(SchemaId fieldTypeId, boolean mandatory);

    /**
     * Instantiates a new FieldType object.
     *
     * <p>This is only a factory method, nothing is created in the repository.
     */
    FieldType newFieldType(ValueType valueType, QName name, Scope scope);

    /**
     * Instantiates a new FieldType object.
     *
     * <p>This is only a factory method, nothing is created in the repository.
     */
    FieldType newFieldType(String valueType, QName name, Scope scope) throws RepositoryException, InterruptedException;

    /**
     * Instantiates a new FieldType object.
     *
     * <p>This is only a factory method, nothing is created in the repository.
     */
    FieldType newFieldType(SchemaId id, ValueType valueType, QName name, Scope scope);

    /**
     * Creates a FieldType in the repository.
     *
     * <p>The ID of a field type is assigned by the system. If there is an ID present in the provided FieldType
     * object, it will be ignored. The generated ID is available from the returned FieldType object.
     *
     * @return updated FieldType object
     * @throws RepositoryException      when an unexpected exception occurs on the repository
     * @throws FieldTypeExistsException
     */
    FieldType createFieldType(FieldType fieldType) throws RepositoryException, InterruptedException;

    /**
     * Creates a field type in the repository, using the given parameters.
     */
    FieldType createFieldType(ValueType valueType, QName name, Scope scope) throws RepositoryException,
            InterruptedException;

    /**
     * Creates a field type in the repository, using the given parameters.
     */
    FieldType createFieldType(String valueType, QName name, Scope scope) throws RepositoryException,
            InterruptedException;

    /**
     * Updates an existing FieldType.
     *
     * <p>You can provide any FieldType object as argument, either retrieved via {@link #getFieldTypeByName} or
     * newly instantiated via {@link #newFieldType}.
     *
     * <p>It is the ID of the field type which serves to identify the field type, so the ID must be present in the
     * FieldType object. The QName of the field type can be changed.
     *
     * @return updated FieldType object
     * @throws FieldTypeNotFoundException when no fieldType with id and version exists
     * @throws FieldTypeUpdateException   an exception occurred while updating the FieldType
     * @throws RepositoryException        when an unexpected exception occurs on the repository
     */
    FieldType updateFieldType(FieldType fieldType) throws RepositoryException, InterruptedException;

    /**
     * Creates or updates a field type, depending on whether it exists in the repository,
     * and depending on the information supplied in the field type.
     *
     * <p>The only case in which an update is performed is when both the ID
     * and name are supplied. This is because the name is the only mutable
     * property of a field type, and you can obviously only change it if
     * there is some other way to identify the field type, namely its ID.
     *
     * <p>If no create or update needs to be performed, it is still always
     * validated that the immutable properties in the supplied field type
     * object correspond to those in the repository, if not a
     * {@link FieldTypeUpdateException} is produced. These properties could
     * also be missing, the returned field type object will always contain
     * the full state as stored in the repository.
     */
    FieldType createOrUpdateFieldType(FieldType fieldType) throws RepositoryException, InterruptedException;

    /**
     * Gets a FieldType from the repository.
     *
     * @throws FieldTypeNotFoundException when no fieldType with the given ID exists
     * @throws RepositoryException        when an unexpected exception occurs on the repository
     */
    FieldType getFieldTypeById(SchemaId id) throws RepositoryException, InterruptedException;

    /**
     * Gets a FieldType from the repository.
     *
     * @throws FieldTypeNotFoundException when no fieldType with the given name exists
     * @throws RepositoryException        when an unexpected exception occurs on the repository
     */
    FieldType getFieldTypeByName(QName name) throws RepositoryException, InterruptedException;

    /**
     * Gets the list of all field types that exist in the repository.
     */
    Collection<FieldType> getFieldTypes() throws RepositoryException, InterruptedException;

    /**
     * Provides {@link ValueType} instances. These are used to set to value type of {@link FieldType}s.
     *
     * <p>The built-in available value types are listed in the following table.
     *
     * <table>
     * <tbody>
     * <tr><th>Name</th>     <th>Class</th></tr> <th>
     * <tr><td>STRING</td>   <td>java.lang.String</td></tr>
     * <tr><td>INTEGER</td>  <td>java.lang.Integer</td></tr>
     * <tr><td>LONG</td>     <td>java.lang.Long</td></tr>
     * <tr><td>DOUBLE</td>   <td>java.lang.Double</td></tr>
     * <tr><td>DECIMAL</td>  <td>java.math.BigDecimal</td></tr>
     * <tr><td>BOOLEAN</td>  <td>java.lang.Boolean</td></tr>
     * <tr><td>DATE</td>     <td>org.joda.time.LocalDate</td></tr>
     * <tr><td>DATETIME</td> <td>org.joda.time.DateTime</td></tr>
     * <tr><td>BLOB</td>     <td>org.lilyproject.repository.api.Blob</td></tr>
     * <tr><td>LINK</td>     <td>org.lilyproject.repository.api.Link</td></tr>
     * <tr><td>URI</td>      <td>java.net.URI</td></tr>
     * <tr><td>LIST</td>     <td>java.util.List</td></tr>
     * <tr><td>PATH</td>     <td>org.lilyproject.repository.api.HierarchyPath</td></tr>
     * <tr><td>RECORD</td>   <td>org.lilyproject.repository.api.Record</td></tr>
     * <tr><td>BYTEARRAY</td><td>org.lilyproject.bytes.api.ByteArray</td></tr>
     * </tbody>
     * </table>
     *
     * <p>Some value types accept extra parameters to define the exact value type.
     * <p>For List and Path these parameters define the value type of the included values.
     * It is mandatory to define this value type.
     * It should be specified by putting its name between brackets "&lt;&gt;"
     * and if that value type in its turn needs some extra parameters,
     * these should be appended again within brackets "&lt;&gt;".
     * <br>For example: <code>getValueType("LIST&lt;PATH&lt;STRING&gt;&gt;");</code>
     *
     * <p>For Record and Link valuetype it is possible to define the {@link RecordType} in the parameters.
     * This is not mandatory. It is done by specifying the name of the RecordType in the format
     * <code>{namespace}name</code> between brackets "&lt;&gt;".
     * <br>For example: <code>getValueType("RECORD<{myNamespace}recordType1>");</code>
     *
     * @param valueType the value type string representation. See table above.
     * @see ValueType
     */
    ValueType getValueType(String valueType) throws RepositoryException, InterruptedException;

    /**
     * Registers custom {@link ValueType}s.
     *
     * <p><b>TODO:</b> Maybe this should rather move to an SPI interface? Can this replace a built-in primitive
     * value type if the name corresponds? Does it make sense to allow registering at any time? Probably implies
     * registering on all Lily nodes? This needs more thought.
     */
    void registerValueType(String name, ValueTypeFactory valueTypeFactory);


    /**
     * Returns a record type builder, providing a fluent API to manipulate record types.
     */
    RecordTypeBuilder recordTypeBuilder() throws TypeException;

    /**
     * Returns a record type builder initialized with the values from an existing record type, providing a fluent API
     * to
     * manipulate this record type.
     */
    RecordTypeBuilder recordTypeBuilder(RecordType recordType) throws TypeException;

    /**
     * Returns a field type builder, providing a fluent API to manipulate field types.
     *
     * <p>Note that field types can also be created in a single statement using
     * {@link #createFieldType(String, QName, Scope)}, the field type builder just
     * offers a little more flexibility in how to specify name and scope and what
     * operations to do (update, createOrUpdate).
     */
    FieldTypeBuilder fieldTypeBuilder() throws TypeException;

    //
    // Schema cache
    //

    /**
     * Returns a snapshot of the FieldTypes cache.
     * <p>
     * To be used when a consistent snapshot is needed while performing a CRUD
     * operation.
     *
     * @return a snapshot of the FieldTypes cache
     * @throws InterruptedException
     */
    FieldTypes getFieldTypesSnapshot() throws InterruptedException;

    /**
     * Returns the list of field types known by the repository.
     * <p>
     * This method bypasses the cache of the type manager.
     */
    List<FieldType> getFieldTypesWithoutCache() throws RepositoryException, InterruptedException;

    /**
     * Returns the list of record types known by the repository.
     * <p>
     * This method bypasses the cache of the type manager.
     */
    List<RecordType> getRecordTypesWithoutCache() throws RepositoryException, InterruptedException;

    /**
     * Returns both the list of field types and record types known by the
     * repository.
     * <p>
     * This method bypasses the cache of the type manager.
     */
    Pair<List<FieldType>, List<RecordType>> getTypesWithoutCache()
            throws RepositoryException, InterruptedException;

    /**
     * Returns both the list of field types and record types known by the
     * repository for a given bucket
     * <p>
     * This method bypasses the cache of the type manager.
     *
     * @return a TypeBucket containing the list of field and record types of the
     *         bucket
     */
    TypeBucket getTypeBucketWithoutCache(String bucketId) throws RepositoryException, InterruptedException;

    /**
     * <b>EXPERT ONLY !</b> Enables the schema cache refreshing system.
     * <p>
     * When enabled the schema caches will get a trigger to update their data
     * whenever a schema update was performed.
     *
     * @throws RepositoryException when setting the flag to enabled failed
     * @see {@link #disableSchemaCacheRefresh()}
     */
    void enableSchemaCacheRefresh() throws RepositoryException, InterruptedException;

    /**
     * <b>EXPERT ONLY !</b> Disables the schema cache refreshing system.
     * <p>
     * When disabling the schema cache refreshing system, a client performing
     * schema updates should use a repository which it got through
     * <code>LilyClient.getPlainRepository()</code> in order to have all updates
     * performed on the same repository and related schema cache.<br/>
     * Otherwise updates could be directed to servers lacking needed type
     * information (due to the disabled cache refreshing).
     *
     * @throws RepositoryException when setting the flag to disabled failed
     */
    void disableSchemaCacheRefresh() throws RepositoryException, InterruptedException;

    /**
     * <b>EXPERT ONLY !</b> Triggers a forced schema cache refresh.
     * <p>
     * Even if the schema cache refreshing system is disabled, this call will
     * trigger the schema caches to refresh their data.
     *
     * @throws RepositoryException when setting the flag to refresh the caches failed
     * @see {@link #disableSchemaCacheRefresh()}
     */
    void triggerSchemaCacheRefresh() throws RepositoryException, InterruptedException;

    /**
     * <b>EXPERT ONLY !</b> Checks if the schema cache refreshing system is
     * enabled or disabled
     *
     * @return true when enabled
     * @see {@link #disableSchemaCacheRefresh()}
     */
    boolean isSchemaCacheRefreshEnabled() throws RepositoryException, InterruptedException;
}
