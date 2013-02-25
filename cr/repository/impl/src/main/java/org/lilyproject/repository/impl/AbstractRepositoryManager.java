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
package org.lilyproject.repository.impl;

import java.io.IOException;
import java.util.Map;

import com.google.common.collect.Maps;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.RecordFactory;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryManager;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.util.io.Closer;

/**
 * Handles thread-safe creation and caching of Repository objects.
 */
public abstract class AbstractRepositoryManager implements RepositoryManager {
    
    private final Map<String,Repository> repositoryCache = Maps.newHashMap();
    private final TypeManager typeManager;
    private final IdGenerator idGenerator;
    private final RecordFactory recordFactory;
    
    
    public AbstractRepositoryManager(TypeManager typeManager, IdGenerator idGenerator, RecordFactory recordFactory) {
        this.typeManager = typeManager;
        this.idGenerator = idGenerator;
        this.recordFactory = recordFactory;
    }
    
    @Override
    public TypeManager getTypeManager() {
        return typeManager;
    }
    
    @Override
    public IdGenerator getIdGenerator() {
        return idGenerator;
    }
    
    @Override
    public RecordFactory getRecordFactory() {
        return recordFactory;
    }
    
    /**
     * Create a new Repository object for the repository cache.
     * @param tableName Name of the backing HTable for the repository
     * @return newly-created repository
     */
    protected abstract Repository createRepository(String tableName) throws IOException, InterruptedException;
    
    @Override
    public Repository getRepository(String tableName) throws IOException, InterruptedException {
        if (!repositoryCache.containsKey(tableName)) {
            synchronized (repositoryCache) {
                if (!repositoryCache.containsKey(tableName)) {
                    repositoryCache.put(tableName, createRepository(tableName));
                }
            }
        }
        return repositoryCache.get(tableName);
    }
    
    @Override
    public synchronized void close() throws IOException {
        for (Repository repository : repositoryCache.values()) {
            Closer.close(repository);
        }
        repositoryCache.clear();
        Closer.close(typeManager);
    }

}
