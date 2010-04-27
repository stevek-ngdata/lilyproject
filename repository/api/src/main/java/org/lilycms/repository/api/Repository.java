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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Set;

import org.lilycms.repository.api.exception.BlobNotFoundException;
import org.lilycms.repository.api.exception.FieldTypeNotFoundException;
import org.lilycms.repository.api.exception.InvalidRecordException;
import org.lilycms.repository.api.exception.RecordExistsException;
import org.lilycms.repository.api.exception.RecordNotFoundException;
import org.lilycms.repository.api.exception.RecordTypeNotFoundException;
import org.lilycms.repository.api.exception.RepositoryException;

/**
 * Repository is the API for all CRUD operations on Records.
 */
public interface Repository {
    /**
     * Creates a new {@link Record} object.
     */
    Record newRecord();

    /**
     * Creates a new {@link Record} object with the {@link RecordId} already
     * filled in.
     */
    Record newRecord(RecordId recordId);

    /**
     * Creates a new record on the repository.
     *
     * <p>
     * If a recordId is given in {@link Record}, that id is used. If not, a new
     * recordId is generated and placed in {@link Record}.
     *
     * @param record
     *            contains the data of the record or variant record to be
     *            created
     * @throws RecordExistsException
     *             if a record with the given recordId already exists
     * @throws RecordNotFoundException
     *             if the master record for a variant record does not exist
     * @throws InvalidRecordException
     *             if an empty record is being created
     * @throws FieldTypeNotFoundException
     * @throws FieldGroupNotFoundException
     * @throws RecordTypeNotFoundException
     * @throws RepostioryException
     *             TBD
     */
    Record create(Record record) throws RecordExistsException,
            RecordNotFoundException, InvalidRecordException,
            RecordTypeNotFoundException, FieldTypeNotFoundException,
            RepositoryException;

    /**
     * Updates an existing record on the repository.
     *
     * <p>
     * The {@link Record} is updated with the new version number.
     * <p>
     * Only {@link Field}s that are mentioned in the {@link Record} will be
     * updated.
     * <p>
     * Fields to be deleted should be separately and explicitly listed in the
     * {@link Record}.
     *
     * @param record
     *            contains the data of the record or variant record to be
     *            updated
     * @throws RecordNotFoundException
     *             if the record does not exist
     * @throws InvalidRecordException
     *             if no update information is provided
     * @throws RepositoryException
     *             TBD
     * @throws FieldTypeNotFoundException
     * @throws FieldGroupNotFoundException
     * @throws RecordTypeNotFoundException
     */
    Record update(Record record) throws RecordNotFoundException,
            InvalidRecordException, RecordTypeNotFoundException,
            FieldTypeNotFoundException, RepositoryException;

    Record updateMutableFields(Record record) throws InvalidRecordException,
            RecordNotFoundException, RecordTypeNotFoundException,
            FieldTypeNotFoundException, RepositoryException;

    Record read(RecordId recordId) throws RecordNotFoundException,
            RecordTypeNotFoundException, FieldTypeNotFoundException,
            RepositoryException;

    Record read(RecordId recordId, List<QName> fieldNames)
            throws RecordNotFoundException, RecordTypeNotFoundException,
            FieldTypeNotFoundException, RepositoryException;

    Record read(RecordId recordId, Long version)
            throws RecordNotFoundException, RecordTypeNotFoundException,
            FieldTypeNotFoundException, RepositoryException;

    Record read(RecordId recordId, Long version, List<QName> fieldNames)
            throws RecordNotFoundException, RecordTypeNotFoundException,
            FieldTypeNotFoundException, RepositoryException;

    /**
     * Delete a {@link Record} from the repository.
     *
     * @param recordId
     *            id of the record to delete
     * @throws RepositoryException
     *             TBD
     */
    void delete(RecordId recordId) throws RepositoryException;

    /**
     * @return the IdGenerator service
     */
    IdGenerator getIdGenerator();

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
    OutputStream getOutputStream(Blob blob) throws RepositoryException;

    /**
     * Returns an {@link InputStream} from which the binary data of a blob can
     * be read. The value of blob is used to identify the underlying blobstore
     * and actual data to return through this InputStream, see {@link #getOutputStream(Blob)}.
     *
     * @param blob the blob for which to open an InputStream
     * @return an InputStream
     * @throws BlobNotFoundException when the blob does not contain a valid key in its value
     * @throws RepositoryException when an unexpected exception occurs
     */
    InputStream getInputStream(Blob blob) throws BlobNotFoundException,
            RepositoryException;

    /**
     * Deletes the data identified by a blob from the underlying blobstore. See {@link #getOutputStream(Blob)} and {@link #getInputStream(Blob)}.
     * @param blob the blob to delete
     * @throws BlobNotFoundException when the blob does not contain a valid key in its value
     * @throws RepositoryException when an unexpected exception occurs
     */
    void delete(Blob blob) throws BlobNotFoundException, RepositoryException;

    /**
     * Get all the variants that exist for the given recordId.
     *
     * @param recordId typically a master record id, if you specify a variant record id, its master will automatically
     *                 be used
     * @return the set of variants, including the master record id.
     */
    Set<RecordId> getVariants(RecordId recordId) throws RepositoryException;    

}
