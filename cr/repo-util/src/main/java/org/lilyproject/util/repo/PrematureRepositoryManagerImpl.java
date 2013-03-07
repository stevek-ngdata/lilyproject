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
package org.lilyproject.util.repo;

import java.io.IOException;

import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.RecordFactory;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryManager;
import org.lilyproject.repository.api.TypeManager;

/**
 * See {@link PrematureRepository}.
 */
public class PrematureRepositoryManagerImpl implements PrematureRepositoryManager {
    private volatile RepositoryManager delegate;
    private final Object delegateAvailable = new Object();

    private void waitOnRepoManager() {
        while (delegate == null) {
            synchronized (delegateAvailable) {
                try {
                    delegateAvailable.wait();
                } catch (InterruptedException e) {
                    throw new RuntimeException("Interrupted while waiting for repository to become available.", e);
                }
            }
        }
    }

    @Override
    public void setRepositoryManager(RepositoryManager repositoryManager) {
        this.delegate = repositoryManager;
        synchronized (delegateAvailable) {
            delegateAvailable.notifyAll();
        }
    }

    @Override
    public Repository getDefaultRepository() throws IOException, InterruptedException {
        waitOnRepoManager();
        return delegate.getDefaultRepository();
    }

    @Override
    public Repository getRepository(String tableName) throws IOException, InterruptedException {
        waitOnRepoManager();
        return delegate.getRepository(tableName);
    }

    @Override
    public void close() throws IOException {
        synchronized (delegateAvailable) {
            if (delegate != null) {
                delegate.close();
            }
        }
    }

    @Override
    public RecordFactory getRecordFactory() {
        waitOnRepoManager();
        return delegate.getRecordFactory();
    }

    @Override
    public IdGenerator getIdGenerator() {
        waitOnRepoManager();
        return delegate.getIdGenerator();
    }

    @Override
    public TypeManager getTypeManager() {
        waitOnRepoManager();
        return delegate.getTypeManager();
    }

}
