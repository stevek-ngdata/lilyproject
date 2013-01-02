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
package org.lilyproject.repository.impl;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.FieldTypeNotFoundException;
import org.lilyproject.repository.api.FieldTypes;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.TypeException;

public interface SchemaCache {

    /**
     * Starts and initializes the schema cache.
     * <p/>
     * This should be called after the repositories it relates to are started,
     * but before those are made available externally.
     */
    void start() throws InterruptedException, KeeperException, RepositoryException;

    /**
     * Stops the schema cache.
     */
    void close() throws IOException;

    /**
     * Updates a field type on the cache.
     * <p/>
     * When on the server side, this will also trigger a cache invalidation.
     * After which all caches will refresh themselves.
     */
    void updateFieldType(FieldType fieldType) throws TypeException, InterruptedException;

    /**
     * Updates a record type on the cache.
     * <p/>
     * When on the server side, this will also trigger a cache invalidation.
     * After which all caches will refresh themselves.
     */
    void updateRecordType(RecordType recordType) throws TypeException, InterruptedException;

    /**
     * Returns a {@link FieldTypes} snapshot object of the field types cache.
     *
     * @throws InterruptedException
     */
    FieldTypes getFieldTypesSnapshot() throws InterruptedException;

    /**
     * Returns a list of field types currently in the cache.
     *
     * @throws InterruptedException
     * @throws TypeException
     */
    List<FieldType> getFieldTypes() throws TypeException, InterruptedException;

    /**
     * Returns a collection of record types currently in the cache.
     *
     * @throws InterruptedException
     */
    Collection<RecordType> getRecordTypes() throws InterruptedException;

    /**
     * Returns the record type with the given name from the cache.
     *
     * @return the RecordType or null if not found
     * @throws InterruptedException
     */
    RecordType getRecordType(QName name) throws InterruptedException;

    /**
     * Returns the record type with the given id from the cache.
     *
     * @return the RecordType or null if not found
     */
    RecordType getRecordType(SchemaId id);

    /**
     * Returns the field type with the given name from the cache.
     *
     * @return the FieldType
     * @throws FieldTypeNotFoundException when the field type is not known in the cache.
     * @throws InterruptedException
     * @throws TypeException
     */
    FieldType getFieldType(QName name) throws FieldTypeNotFoundException, InterruptedException, TypeException;

    /**
     * Returns the field type with the given id from the cache.
     *
     * @return the FieldType
     * @throws FieldTypeNotFoundException when the field type is not known in the cache.
     * @throws InterruptedException
     * @throws TypeException
     */
    FieldType getFieldType(SchemaId id) throws FieldTypeNotFoundException, TypeException, InterruptedException;

    boolean fieldTypeExists(QName name) throws InterruptedException;

    FieldType getFieldTypeByNameReturnNull(QName name) throws InterruptedException;
}
