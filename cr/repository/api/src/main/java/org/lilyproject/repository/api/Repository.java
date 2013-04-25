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
import java.io.IOException;

// IMPORTANT:
//   The Repository implementation might be wrapped to add automatic retrying of operations in case
//   of IO exceptions or when no Lily servers are available. In case this fails, a
//   RetriesExhausted(Record|Type|Blob)Exception is thrown. Therefore, all methods in this interface
//   should declare this exception. Also, the remote implementation can cause IO exceptions which are
//   dynamically wrapped in Record|Type|BlobException, thus this exception (which is a parent class
//   of the RetriesExhausted exceptions) should be in the throws clause of all methods.

/**
 * A Repository is a set of tables, tables contain records.
 *
 * <p>A Repository is tenant-specific and obtained from {@link RepositoryManager}.</p>
 *
 * <p>For backwards compatibility, Repository extends from Table. The methods of Table will in this
 * case be executed against the default table, that is the table called "records".</p>
 */
public interface Repository extends LTable, Closeable {
    /**
     * TODO multitenancy
     *
     * <p>For backwards compatibility, you can cast the returned LTable to Repository. New code should
     * be written against the LTable interface.</p>
     */
    LTable getTable(String tableName) throws IOException, InterruptedException;

    LTable getDefaultTable() throws IOException, InterruptedException;

    // TODO multitenancy
    //RepositoryTableManager getTableManager();

    /**
     * @return the IdGenerator service
     */
    IdGenerator getIdGenerator();

    /**
     * @return the TypeManager service
     */
    TypeManager getTypeManager();

    /**
     * Returns the RepositoryManager that is responsible for this Repository.
     * @return owning RepositoryManager
     */
    RepositoryManager getRepositoryManager();

    /**
     * A {@link BlobStoreAccess} must be registered with the repository before
     * it can be used. Any BlobStoreAccess that has ever been used to store
     * binary data of a blob must be registered before that data can be
     * retrieved again.
     */
    void registerBlobStoreAccess(BlobStoreAccess blobStoreAccess);

}
