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
 * Gives access to different repositories.
 *
 * <p>You can have multiple repositories ({@link LRepository}), each repository contains tables ({@link LTable}),
 * tables contain records ({@link Record}).</p>
 *
 * <p>Multiple repositories can be useful for multitenancy use-cases, e.g. to set up different spaces for
 * different divisions in an organisation, while sharing the same cluster (same Hadoop, HBase, Lily, ...).
 * Since each repository has its own tables (which maps to HBase tables), this feature is suited to have
 * a few big repositories rather than many small repositories.</p>
 *
 * <p>Repositories need to be defined up front through the {@code RepositoryModel}.</p>
 */
public interface RepositoryManager extends Closeable {

    /**
     * Get the default {@code Repository}. This is the same as calling getRepository("default").
     *
     * @return the default repository
     */
    LRepository getDefaultRepository() throws InterruptedException, RepositoryException;

    /**
     * Get a named {@code Repository}.
     *
     * @param repositoryName name of the repository to be retrieved
     * @return Either a new Repository or a cached instance
     */
    LRepository getRepository(String repositoryName) throws InterruptedException, RepositoryException;

    /**
     * Get the specified table from the default repository.
     *
     * <p>This is a shortcut for calling getDefaultRepository().getTable(tableName).</p>
     *
     * <p><b>It is strongly recommended to get a specific {@link LRepository} instance and then
     * work from there, so that your code can easily be applied to different repositories.</b></p>
     */
    LTable getTable(String tableName) throws InterruptedException, RepositoryException;

    /**
     * Get the default table ("record") from the default repository.
     *
     * <p><b>It is strongly recommended to get a specific {@link LRepository} instance and then
     * work from there, so that your code can easily be applied to different repositories.</b></p>
     */
    LTable getDefaultTable() throws InterruptedException, RepositoryException;
}
