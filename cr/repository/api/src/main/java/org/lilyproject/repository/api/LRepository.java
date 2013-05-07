/*
 * Copyright 2013 NGDATA nv
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

// IMPORTANT:
//   The Repository implementation might be wrapped to add automatic retrying of operations in case
//   of IO exceptions or when no Lily servers are available. In case this fails, a
//   RetriesExhausted(Record|Type|Blob)Exception is thrown. Therefore, all methods in this interface
//   should declare this exception. Also, the remote implementation can cause IO exceptions which are
//   dynamically wrapped in Record|Type|BlobException, thus this exception (which is a parent class
//   of the RetriesExhausted exceptions) should be in the throws clause of all methods.

/**
 * A repository is a set of tables, tables contain records.
 *
 * <p>An LRepository instance can be obtained from {@link RepositoryManager}.</p>
 */
public interface LRepository {
    /**
     * Get a table object through which you can do CRUD operations.
     *
     * <p>The table should have been previously created using the {@link TableManager} retrieved from
     * {@link Repository#getTableManager()}.</p>
     *
     * <p>For backwards compatibility, you can cast the returned LTable to {@link Repository}.</p>
     */
    LTable getTable(String tableName) throws InterruptedException, RepositoryException;

    /**
     * Get a table object for the default "record" table. This is equivalent to calling
     * getTable("record").
     */
    LTable getDefaultTable() throws InterruptedException, RepositoryException;


    TableManager getTableManager();

    /**
     * @return the IdGenerator service
     */
    IdGenerator getIdGenerator();

    /**
     * @return the TypeManager service
     */
    TypeManager getTypeManager();

    /**
     * @return the factory for creating records
     */
    RecordFactory getRecordFactory();

    /**
     * The name of this repository.
     */
    String getRepositoryName();
}
