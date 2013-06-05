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
package org.lilyproject.repository.model.api;

import java.util.Set;

/**
 * Created, update and delete of {@link RepositoryDefinition}s.
 */
public interface RepositoryModel {
    /**
     * Creates a new repository. The repository will initially be in the lifecycle state CREATE_REQUESTED.
     *
     * <p>Use {@link #waitUntilRepositoryInState} if you need to wait until a repository is active.</p>
     */
    void create(String repositoryName) throws RepositoryExistsException, InterruptedException, RepositoryModelException;

    /**
     * Soft-delete a repository.
     *
     * <p>This doesn't delete the repository but puts it in the lifecycle state DELETE_REQUESTED.
     * This allows cleanup operations to happen, once these are done the repository will be
     * fully deleted.</p>
     */
    void delete(String repositoryName) throws InterruptedException, RepositoryModelException, RepositoryNotFoundException;

    /**
     * Deletes a repository.
     *
     * <p>Normally you shouldn't use this method but rather {@link #delete(String)}.
     * The use of this method is reserved for the RepositoryMaster.</p>
     */
    void deleteDirect(String repositoryName) throws InterruptedException, RepositoryModelException, RepositoryNotFoundException;

    /**
     * Updates a repository.
     */
    void updateRepository(RepositoryDefinition repositoryDefinition) throws InterruptedException, RepositoryModelException,
            RepositoryNotFoundException;

    /**
     * Gets the list of repositories.
     *
     * <p>The repositories are retrieved from the persistent storage, so this method is relatively expensive.</p>
     */
    Set<RepositoryDefinition> getRepositories() throws InterruptedException, RepositoryModelException;

    /**
     * Gets a repository definition.
     *
     * <p>The repository is retrieved from the persistent storage, so this method is relatively expensive.</p>
     */
    RepositoryDefinition getRepository(String repositoryName) throws InterruptedException, RepositoryModelException, RepositoryNotFoundException;

    /**
     * This method checks if a repository exists and is active.
     *
     * <p>The implementation of this method should be fast and suited for very-frequent calling,
     * i.e. it should not perform any IO.</p>
     */
    boolean repositoryExistsAndActive(String repositoryName);

    /**
     * This method checks if a repository is active, it throws an exception if the repository does not exist.
     *
     * <p>The implementation of this method should be fast and suited for very-frequent calling,
     * i.e. it should not perform any IO.</p>
     */
    boolean repositoryActive(String repositoryName) throws RepositoryNotFoundException;

    /**
     * Waits until a repository is in the given state. If the repository would not yet be known, this method will wait
     * for that as well.
     *
     * <p><b>Important:</b> you need to check the return code to know if the repository really arrived in the
     * desired state (= when true is returned).</p>
     */
    boolean waitUntilRepositoryInState(String repositoryName, RepositoryDefinition.RepositoryLifecycleState state, long timeout)
            throws InterruptedException;

    /**
     * Get the list of repositories and at the same time register a listener for future repository changes.
     *
     * <p>This method assures you will get change notifications for any change compared to the
     * repositories returned by this method.</p>
     */
    Set<RepositoryDefinition> getRepositories(RepositoryModelListener listener);

    void registerListener(RepositoryModelListener listener);

    void unregisterListener(RepositoryModelListener listener);

}
