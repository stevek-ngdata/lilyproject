/*
 * Copyright 2012 NGDATA nv
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

/**
 * Handles storage and retrieval of {@link Repository} objects for each HTable, as well as general repository services
 * such as a {@link TypeManager} and {@link IdGenerator}.
 */
public interface RepositoryManager extends Closeable {

    /**
     * @return the factory for creating records
     */
    RecordFactory getRecordFactory();

    /**
     * @return the IdGenerator service
     */
    IdGenerator getIdGenerator();

    /**
     * @return the TypeManager service
     */
    TypeManager getTypeManager();

    /**
     * Get the default repository.
     * 
     * @return default repository
     */
    Repository getDefaultRepository();

    /**
     * Get the {@code Repository} for a specific HBase table.
     * 
     * @param tableName Name of the table for which the repository is to be fetched
     * @return Either a new Repository or a cached instance
     */
    Repository getRepository(String tableName) throws IOException, InterruptedException;
}
