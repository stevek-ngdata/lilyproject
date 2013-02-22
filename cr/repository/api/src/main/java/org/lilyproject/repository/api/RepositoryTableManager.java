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

import java.io.IOException;
import java.util.List;

/**
 * Handles the life-cycle of Lily repository tables.
 */
public interface RepositoryTableManager {

    /**
     * Create a new record table. An exception will be thrown if the table already exists.
     * 
     * @param tableName name of the table to create
     */
    void createTable(String tableName) throws InterruptedException, IOException;

    /**
     * Create a new record table with predefined region splits. An exception will be thrown if the table already exists.
     * 
     * @param tableName name of the table to create
     * @param splitKeys byte prefixes upon which regions are defined in the table
     */
    void createTable(String tableName, byte[][] splitKeys) throws InterruptedException, IOException;

    /**
     * Delete an existing record table. An exception will be thrown if the table doesn't exist.
     * 
     * @param tableName name of the table to be deleted
     */
    void dropTable(String tableName) throws InterruptedException, IOException;

    /**
     * Get all currently-existing tables.
     * 
     * @return All existing tables
     */
    List<String> getTableNames() throws InterruptedException, IOException;

    /**
     * Check if a {@link RepositoryTable} exists.
     * 
     * @param tableName name of the table to check for
     * @return true if the table exists, otherwise false
     */
    boolean tableExists(String tableName) throws InterruptedException, IOException;

}
