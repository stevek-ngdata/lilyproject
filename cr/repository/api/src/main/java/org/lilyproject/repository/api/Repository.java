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
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Set;

// IMPORTANT:
//   The Repository implementation might be wrapped to add automatic retrying of operations in case
//   of IO exceptions or when no Lily servers are available. In case this fails, a
//   RetriesExhausted(Record|Type|Blob)Exception is thrown. Therefore, all methods in this interface
//   should declare this exception. Also, the remote implementation can cause IO exceptions which are
//   dynamically wrapped in Record|Type|BlobException, thus this exception (which is a parent class
//   of the RetriesExhausted exceptions) should be in the throws clause of all methods.

/**
 * Repository is the primary access point for accessing the functionality of the Lily repository.
 *
 * <p>Via Repository, you can perform all {@link Record}-related CRUD operations.
 */
public interface Repository extends Closeable {
    /**
     * Instantiates a new Record object.
     *
     * <p>This is only a factory method, nothing is created in the repository.
     */
    Record newRecord() throws RecordException;

    /**
     * Instantiates a new Record object with the RecordId already filled in.
     *
     * <p>This is only a factory method, nothing is created in the repository.
     */
    Record newRecord(RecordId recordId) throws RecordException;

    /**
     * Creates a new record in the repository.
     *
     * <p>A Record object can be instantiated via {@link #newRecord}.
     *
     * <p>If a recordId is given in {@link Record}, that id is used. If not, a new id is generated and available
     * from the returned Record object.
     *
     * @throws RecordExistsException
     *             if a record with the given recordId already exists
     * @throws RecordNotFoundException
     *             if the master record for a variant record does not exist
     * @throws InvalidRecordException
     *             if an empty record is being created
     * @throws FieldTypeNotFoundException
     * @throws RecordTypeNotFoundException
     */
    Record create(Record record) throws RepositoryException, InterruptedException;

    /**
     * Shortcut for {@link #update(Record, boolean, boolean, java.util.List)
     * update(record, updateVersion, useLatestRecordType, null)}.
     */
    Record update(Record record, boolean updateVersion, boolean useLatestRecordType) throws RepositoryException, InterruptedException;

    /**
     * Shortcut for {@link #update(Record, boolean, boolean, java.util.List) update(record, false, true, null)}.
     */
    Record update(Record record) throws RepositoryException, InterruptedException;

    /**
     * Shortcut for {@link #update(Record, boolean, boolean, java.util.List) update(record, false, true, conditions)}.
     */
    Record update(Record record, List<MutationCondition> conditions) throws RepositoryException, InterruptedException;

    /**
     * Updates an existing record in the repository.
     *
     * <p>An update can either update the versioned and/or non-versioned fields (in this last case a new version
     * will be created), or it can update the versioned-mutable fields. This last one is an update of (manipulation
     * of) an existing version and cannot be combined with updating fields in the versioned and non-versioned scope.
     * So these are two distinct operations, you have to choose which one you want to do, this is done by setting
     * the updateVersion argument. Most of the time you will update versioned or non-versioned fields, for this you
     * set updateVersion to false.
     *
     * <p>The provided Record object can either be obtained by reading a record via {@link #read} or
     * it can also be instantiated from scratch via {@link #newRecord}.
     *
     * <p>The Record object can be limited to contain only those fields that you are interested in changing, so it
     * can be sparsely filled. If you want to be sure of the value of all fields, then specify them all, even if
     * they have not changed compared to what you read, since another update might have been performed concurrently.
     * Fields that are not present in the record will not be deleted, deleting fields
     * needs to be done explicitly by adding them to the list of fields to delete, see {@link Record#getFieldsToDelete}.
     *
     * <p>If the record contains any changed versioned fields, a new version will be created. The number of the created
     * version will be available on the returned Record object.
     *
     * <p>If no record type is specified in the record object, the record type that is currently stored (in the
     * non-versioned scope) will be used. Newly created versions always get the same record type as the current
     * record type of the non-versioned scope (= either the one specified in the Record, or if absent, from
     * what is currently stored). Usually you will want to move automatically to the latest version of
     * the record type. This can be done by setting the version to null in the record (if you also specify the name
     * in the record object), but more conveniently using the useLatestRecordType argument.
     *
     * <p><b>Updating an existing version: updating versioned-mutable fields</b>
     *
     * <p>The following applies to updating an existing version:
     *
     * <ul>
     *
     * <li>It is required to specify a version in the Record object.
     *
     * <li>If you do not specify a record type in the Record object, the record type of the versioned-mutable scope
     * of the version that is being modified will be set to the current one (= the stored one) of the non-versioned
     * scope, and possibly to its latest version depending on the argument useLatestRecordType.
     *
     * <li>If you do specify a record type in the Record object (using {@link Record#setRecordType(QName)}), the record
     * type of the versioned-mutable scope of the version that is being modified will be changed to it, but the record
     * type of the non-versioned scope will be left unmodified. This is in contrast to the record type of the versioned
     * scope, which is always brought to the record type of the non-versioned scope when a new version is created.
     * However, we found it should be possible to modify the versioned-mutable record type of an existing version
     * without influencing the current record state.
     *
     * </ul>
     *
     * <p><b>The returned record object</b>
     *
     * <p>The record object you supply as argument will not be modified, it is internally cloned an modified. Currently
     * these modifications are mostly limited to setting the resolved record type and version. The returned record
     * object will never contain any fields you did not specify in the Record object, so you might have to do a read
     * to see the full record situation (other fields might have been added by concurrent updates). This will be
     * addressed by issue <a href="http://dev.outerthought.org/trac/outerthought_lilyproject/ticket/93">93<a>.<p>
     *
     * <p><b>Conditionally updating a record: the conditions argument</b></p>
     *
     * <p>A conditional update allows to update a record only in case it satisfies certain conditions. This is
     * also known as "check and update (CAS - check and set)" or "optimistic concurrency control (OCC)".</p>
     *
     * <p>The condition can check on any of the record fields (of any scope), thus is not limited to the fields
     * provided in the record object. For versioned(-mutable) fields, the check is usually performed on the data
     * from the latest version, except in case updateVersion is true, then the data from version being updated
     * will be checked (both for the versioned and versioned-mutable fields).</p>
     *
     * <p>For more details on specifying the conditions, see {@link MutationCondition}. All the conditions should
     * be satisfied for the update to proceed, thus the conditions are AND-ed.</p>
     *
     * <p>In case one ore more conditions are not satisfied, NO exception is thrown, rather the responseStatus
     * field of the returned Record object is set to {@link ResponseStatus#CONFLICT}. So the caller who is interested
     * in knowing whether the update succeeded should check on this field. The returned record object will contain
     * the currently stored repository state, not the submitted record values.</p>
     *
     * <p>The conditions are checked before checking if the record actually needs updating, so you might get
     * a conflict response even if the stored record state corresponds to the supplied record state.</p>
     *
     * @param updateVersion if true, the version indicated in the record will be updated (i.e. only the mutable fields will be updated)
     *          otherwise, a new version of the record will be created (if it contains versioned fields)
     * @param useLatestRecordType if true, the RecordType version given in the Record will be ignored and the latest available RecordType will
     *        be used while updating the Record
     * @param conditions optional (can be null), set of conditions that should be satisfied for the update to proceed
     *
     * @throws RecordNotFoundException
     *             if the record does not exist
     * @throws InvalidRecordException
     *             if no update information is provided
     * @throws RepositoryException
     *             TBD
     * @throws FieldTypeNotFoundException
     * @throws RecordTypeNotFoundException
     * @throws WalProcessingException
     */
    Record update(Record record, boolean updateVersion, boolean useLatestRecordType, List<MutationCondition> conditions)
            throws RepositoryException, InterruptedException;

    /**
     * Creates or updates a record, depending on whether the record already exists.
     *
     * <p>See {@link #createOrUpdate(Record, boolean)} for more details.
     */
    Record createOrUpdate(Record record) throws RepositoryException, InterruptedException;

    /**
     * Creates or updates a record, depending on whether the record already exists.
     *
     * <p>This method has the advantage that you do not have to deal with {@link RecordExistsException}
     * (in case of create) or {@link RecordNotFoundException} (in case of update).
     *
     * <p>This method has the advantage over create that it can be safely retried in case of IO related problems,
     * without having to worry about whether the previous call did or did not go through, and thus avoiding
     * {@link RecordExistsException}'s or the creation of multiple records (in case the client did not
     * specify an ID).
     */
    Record createOrUpdate(Record record, boolean useLatestRecordType) throws RepositoryException, InterruptedException;

    /**
     * @deprecated in favor of using varargs for the fieldNames. Please use {@link #read(List, QName...)} instead.
     * 
     * Reads a record limited to a subset of the fields. Only the fields specified in the fieldNames list will be
     * included.
     *
     * <p>Versioned and versioned-mutable fields will be taken from the latest version.
     *
     * <p>It is not an error if the record would not have a particular field, though it is an error to specify
     * a non-existing field name.
     * 
     * @param recordId the id of the record to read, null is not allowed
     * @param fieldNames list of names of the fields to read or null to read all fields
     */
    @Deprecated 
    Record read(RecordId recordId, List<QName> fieldNames) throws RepositoryException, InterruptedException;
    
    /**
     * Reads a record.
     * If fieldNames are specified, the read is limited to include only this subset of fields.
     * Otherwise all fields of the record are read. 
     *
     * <p>Versioned and versioned-mutable fields will be taken from the latest version.
     *
     * <p>It is not an error if the record would not have a particular field, though it is an error to specify
     * a non-existing field name.
     * 
     * @param recordId the id of the record to read, null is not allowed
     * @param fieldNames names of the fields to read or null to read all fields
     */
    Record read(RecordId recordId, QName... fieldNames) throws RepositoryException, InterruptedException;
    
    /**
     * @deprecated in favor of using varargs for the fieldNames. Please use {@link #read(List<RecordId>, QName...)} instead.
     *
     * Reads a list of records limited to a subset of the fields. Only the fields specified in the fieldNames list will be
     * included.
     *
     * <p>Versioned and versioned-mutable fields will be taken from the latest version.
     *
     * <p>It is not an error if the records would not have a particular field, though it is an error to specify
     * a non-existing field name.
     * 
     * <p>No RecordNotFoundException is thrown when a record does not exist or has been deleted.
     * Instead, the returned list will not contain an entry for that requested id. 
     *
     * @param list or recordIds to read, null is not allowed
     * @param fieldNames list of names of the fields to read or null to read all fields
     * @return list of records that are read, can be smaller than the amount or requested ids when those are not found
     */
    @Deprecated
    List<Record> read(List<RecordId> recordIds, List<QName> fieldNames) throws RepositoryException, InterruptedException;
    
    /**
     * Reads a list of records.
     * If fieldNames are specified, the read is limited to include only this subset of fields.
     * Otherwise all fields are read. 
     *
     * <p>Versioned and versioned-mutable fields will be taken from the latest version.
     *
     * <p>It is not an error if the records would not have a particular field, though it is an error to specify
     * a non-existing field name.
     * 
     * <p>No RecordNotFoundException is thrown when a record does not exist or has been deleted.
     * Instead, the returned list will not contain an entry for that requested id. 
     *
     * @param list or recordIds to read, null is not allowed
     * @param fieldNames names of the fields to read or null to read all fields
     * @return list of records that are read, can be smaller than the amount or requested ids when those are not found
     */
    List<Record> read(List<RecordId> recordIds, QName... fieldNames) throws RepositoryException, InterruptedException;
    
    /**
     * @deprecated in favor of using varargs for the fieldNames. Please use {@link #read(RecordId, Long, QName...)} instead.
     * 
     * Reads a specific version of a record limited to a subset of the fields.
     * 
     * <p>If the given list of fields is empty, all fields will be read.
     */
    @Deprecated
    Record read(RecordId recordId, Long version, List<QName> fieldNames) throws RepositoryException, InterruptedException;
    
    /**
     * Reads a specific version of a record.
     * If fieldNames are specified, the read is limited to include only this subset of fields.
     * Otherwise all fields are read. 
     * 
     * <p>If the given list of fields is empty, all fields will be read.
     */
    Record read(RecordId recordId, Long version, QName... fieldNames) throws RepositoryException, InterruptedException;
    
    /**
     * @deprecated in favor of using varargs for the fieldNames. Please use {@link #read(RecordId, Long, Long, QName...)} instead.
     * 
     * Reads all versions of a record between fromVersion and toVersion (both included), limited to a subset of the fields.
     * 
     * <p>If the given list of fields is empty, all fields will be read.
     */
    @Deprecated
    List<Record> readVersions(RecordId recordId, Long fromVersion, Long toVersion, List<QName> fieldNames)
            throws RepositoryException, InterruptedException;
    
    
    /**
     * Reads all versions of a record between fromVersion and toVersion (both included).
     * If fieldNames are specified, the read is limited to include only this subset of fields.
     * Otherwise all fields are read. 
     * 
     * <p>If the given list of fields is empty, all fields will be read.
     */
    List<Record> readVersions(RecordId recordId, Long fromversion, Long toVersion, QName... fieldNames) throws RepositoryException, InterruptedException;

    /**
     * @deprecated in favor of using varargs for the fieldNames. Please use {@link #read(RecordId, List<Long>, QName...)} instead.
     * 
     * Reads all versions of a record listed the <code>versions</code>, limited to a subset of the fields.
     * 
     * @param recordId id of the record to read
     * @param versions the list of versions to read, should not contain null values
     * @param fieldNames list of fields to read, if null all fields will be read
     * @return a list of records. The list can be smaller than the number of requested versions if some requested versions
     * have a higher number than the highest existing version.
     */
    @Deprecated
    List<Record> readVersions(RecordId recordId, List<Long> versions, List<QName> fieldNames)
            throws RepositoryException, InterruptedException;
        
    /**
     * Reads all versions of a record listed the <code>versions</code>.
     * If fieldNames are specified, the read is limited to include only this subset of fields.
     * Otherwise all fields are read. 
     * 
     * @param recordId id of the record to read
     * @param versions the list of versions to read, should not contain null values
     * @param fieldNames names of fields to read, if null all fields will be read
     * @return a list of records. The list can be smaller than the number of requested versions if some requested versions
     * have a higher number than the highest existing version.
     */
    List<Record> readVersions(RecordId recordId, List<Long> version, QName... fieldNames) throws RepositoryException, InterruptedException;
    
    /**
     * Reads a Record and also returns the mapping from QNames to IDs.
     *
     * <p>See {@link IdRecord} for more information.
     *
     * @param version version to load. Optional, can be null.
     * @param fieldIds load only the fields with these ids. optional, can be null.
     */
    IdRecord readWithIds(RecordId recordId, Long version, List<SchemaId> fieldIds) throws RepositoryException, InterruptedException;

    /**
     * Delete a {@link Record} from the repository.
     *
     * @param recordId
     *            id of the record to delete
     */
    void delete(RecordId recordId) throws RepositoryException, InterruptedException;

    /**
     * Conditionally delete a record from the repository.
     *
     * <p>The delete will only succeed if all the supplied conditions are satisfied. The conditions can check
     * on fields from all scopes, it are the values from the latest version which are used.</p>
     *
     * <p>In case the conditions are satisfied, this method returns null.</p>
     *
     * <p>In case the conditions are not satisfied, this method returns a Record object with its responseStatus
     * field set to {@link ResponseStatus#CONFLICT}. The fields contained in the record object will be those
     * referred to in the conditions, as far as they exist.</p>
     */
    Record delete(RecordId recordId, List<MutationCondition> conditions) throws RepositoryException, InterruptedException;

    /**
     * Returns the IdGenerator service.
     */
    IdGenerator getIdGenerator();

    /**
     * Returns the TypeManager.
     */
    TypeManager getTypeManager();

    /**
     * A {@link BlobStoreAccess} must be registered with the repository before
     * it can be used. Any BlobStoreAccess that has ever been used to store
     * binary data of a blob must be registered before that data can be
     * retrieved again.
     *
     */
    void registerBlobStoreAccess(BlobStoreAccess blobStoreAccess);

    /**
     * Returns an {@link OutputStream} for a blob. The binary data of a blob
     * must be written to this outputStream and the stream must be closed before
     * the blob may be stored in a {@link Record}. The method
     * {@link Blob#setValue(byte[])} will be called internally to update the
     * blob with information that will make it possible to retrieve that data
     * again through {@link #getInputStream(Blob)}.
     *
     * <p>
     * The {@link BlobStoreAccessFactory} will decide to which underlying
     * blobstore the data will be written.
     *
     * @param blob
     *            the blob for which to open an OutputStream
     * @return an OutputStream
     * @throws RepositoryException when an unexpected exception occurs
     */
    OutputStream getOutputStream(Blob blob) throws RepositoryException, InterruptedException;

    /**
     * Returns a {@link BlobAccess} object which provides access to the blob metadata and the input stream to
     * read the blob's data.
     *
     * <p>A blob is retrieved by specifying the {record id, version, field name} coordinates.
     * And in case of ListValueType or PathValueType fields an array of indexes.
     *
     * @param recordId the id of the record containing the blob
     * @param version optionally a version of the record, if null the latest record version is used
     * @param fieldName the QName of the field containing the blob
     * @param indexes optionally, the position of a blob in a List or Path field,
     *        where each index gives the position at a deeper level

     * @throws BlobNotFoundException thrown when no blob can be found at the given location
     * @throws BlobException thrown when opening an InputStream on the blob fails
     */
    BlobAccess getBlob(RecordId recordId, Long version, QName fieldName, Integer...indexes) throws RepositoryException, InterruptedException;
    
    /**
     * Shortcut getBlob method where version and indexes are set to null.
     */
    BlobAccess getBlob(RecordId recordId, QName fieldName) throws RepositoryException, InterruptedException;

    /**
     * Returns an {@link InputStream} from which the binary data of a blob can be read.
     * 
     * <p>A blob is retrieved by specifying the {record id, version, field name} coordinates.
     * And in case of ListValueType or PathValueType fields an array of indexes.
     * 
     * @param recordId the id of the record containing the blob
     * @param version optionally a version of the record, if null the latest record version is used
     * @param fieldName the QName of the field containing the blob
     * @param indexes optionally, the position of a blob in a List or Path field,
     *        where each index gives the position at a deeper level

     * @throws BlobNotFoundException thrown when no blob can be found at the given location
     * @throws BlobException thrown when opening an InputStream on the blob fails
     */
    InputStream getInputStream(RecordId recordId, Long version, QName fieldName, Integer...indexes) throws RepositoryException, InterruptedException;

    /**
     * Shortcut getInputStream method where version, and indexes are set to null.
     */
    InputStream getInputStream(RecordId recordId, QName fieldName) throws RepositoryException, InterruptedException;

    /**
     * getInputStream method where the record containing the blob is given instead of its recordId.
     * This avoids an extra call on the repository to read the record.
     * This is especially usefull for inline blobs. 
     */
    InputStream getInputStream(Record record, QName fieldName, Integer...indexes) throws RepositoryException, InterruptedException;
    
    /**
     * Get all the variants that exist for the given recordId.
     *
     * @param recordId typically a master record id, if you specify a variant record id, its master will automatically
     *                 be used
     * @return the set of variants, including the master record id. Returns an empty list if the record would not
     *         exist.
     */
    Set<RecordId> getVariants(RecordId recordId) throws RepositoryException, InterruptedException;

    /**
     * Returns a record builder object which can be used to compose a record object and create or update it on the repository.
     */
    RecordBuilder recordBuilder() throws RecordException, InterruptedException;
}
