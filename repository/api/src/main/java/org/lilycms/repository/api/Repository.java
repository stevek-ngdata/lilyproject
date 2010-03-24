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
 * Repository is the API for all CRUD operations on Records.
 */
public interface Repository {
    /**
     * Creates a new {@link Record} object.
     */
    Record newRecord() throws RepositoryException;

    /**
     * Creates a new {@link Record} object with the {@link RecordId} already
     * filled in.
     */
    Record newRecord(RecordId recordId) throws RepositoryException;

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
     * @throws RepostioryException
     *             TBD
     */
    void create(Record record) throws RecordExistsException, RecordNotFoundException, InvalidRecordException,
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
     */
    void update(Record record) throws RecordNotFoundException, InvalidRecordException, RepositoryException;

    /**
     * Read the latest version of a {@link Record} from the repository.
     * 
     * @param recordId
     *            the id of the {@link Record} to read
     * @param fieldIds
     *            the fields to read from the {@link Record}, an empty list
     *            results in reading all fields
     * @throws RecordNotFoundException
     *             if the {@link Record} does not exist
     * @throws RepositoryException
     *             TBD
     */
    Record read(RecordId recordId, String... fieldIds) throws RecordNotFoundException, RepositoryException;

    /**
     * Read a specific version of a {@link Record}
     * 
     * @param version
     *            the versionNumber to read
     * 
     */
    Record read(RecordId recordId, Long version, String... fieldIds) throws RecordNotFoundException,
                    RepositoryException;

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
}
