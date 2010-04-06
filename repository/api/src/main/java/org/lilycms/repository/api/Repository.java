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

import java.util.List;


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
     * @throws FieldDescriptorNotFoundException 
     * @throws FieldGroupNotFoundException 
     * @throws RecordTypeNotFoundException 
     * @throws RepostioryException
     *             TBD
     */
    Record create(Record record) throws RecordExistsException, RecordNotFoundException, InvalidRecordException,
                    RecordTypeNotFoundException, FieldGroupNotFoundException, FieldDescriptorNotFoundException, RepositoryException;

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
     * @throws FieldDescriptorNotFoundException 
     * @throws FieldGroupNotFoundException 
     * @throws RecordTypeNotFoundException 
     */
    Record update(Record record) throws RecordNotFoundException, InvalidRecordException, RecordTypeNotFoundException, FieldGroupNotFoundException, FieldDescriptorNotFoundException, RepositoryException;

    Record read(RecordId recordId) throws RecordNotFoundException, RecordTypeNotFoundException, FieldGroupNotFoundException, FieldDescriptorNotFoundException, RepositoryException;

    Record read(RecordId recordId, List<String> nonVersionableFieldIds, List<String> versionableFieldIds, List<String> versionableMutableFieldIds) throws RecordNotFoundException, RecordTypeNotFoundException, FieldGroupNotFoundException, FieldDescriptorNotFoundException, RepositoryException;

    Record read(RecordId recordId, Long version) throws RecordNotFoundException, RecordTypeNotFoundException, FieldGroupNotFoundException, FieldDescriptorNotFoundException, RepositoryException;

    Record read(RecordId recordId, Long version, List<String> nonVersionableFieldIds, List<String> versionableFieldIds, List<String> versionableMutableFieldIds) throws RecordNotFoundException, RecordTypeNotFoundException, FieldGroupNotFoundException, FieldDescriptorNotFoundException, RepositoryException;

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
