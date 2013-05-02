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

/**
 * Handles storage and retrieval of {@link Repository} objects for each HTable, as well as general repository services
 * such as a {@link TypeManager} and {@link IdGenerator}.
 */
public interface RepositoryManager extends Closeable {

    /**
     * @return the IdGenerator service
     */
    IdGenerator getIdGenerator();

    /**
     * Get the {@code Repository} for the public tenant. This is the same as calling
     * getRepository("public").
     *
     * @return the public repository
     */
    LRepository getPublicRepository() throws InterruptedException, RepositoryException;

    /**
     * Get the {@code Repository} for a specific tenant.
     *
     * @param tenantName name of the tenant for which the repository is to be fetched
     * @return Either a new Repository or a cached instance
     */
    LRepository getRepository(String tenantName) throws InterruptedException, RepositoryException;

    /**
     * Get the specified table for the public tenant.
     *
     * <p>This is a shortcut for calling getPublicTenant().getTable(tableName).</p>
     *
     * <p><b>It is strongly recommended to get a {@link LRepository} instance for a specific tenant and then
     * work from there, so that your code can easily be applied to different tenants.</b></p>
     */
    LTable getTable(String tableName) throws InterruptedException, RepositoryException;

    /**
     * Get the default table ("record") from the public tenant.
     *
     * <p><b>It is strongly recommended to get a {@link LRepository} instance for a specific tenant and then
     * work from there, so that your code can easily be applied to different tenants.</b></p>
     */
    LTable getDefaultTable() throws InterruptedException, RepositoryException;
}
