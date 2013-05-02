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
import org.lilyproject.repository.api.LRepository;
import org.lilyproject.repository.api.LTable;
import org.lilyproject.repository.api.RecordFactory;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.RepositoryManager;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.tenant.model.api.TenantModel;
import org.lilyproject.util.hbase.LilyHBaseSchema.Table;
import org.lilyproject.util.io.Closer;

/**
 * Handles thread-safe creation and caching of Repository objects.
 */
public abstract class AbstractRepositoryManager implements RepositoryManager {

    private final Map<TenantTableKey, Repository> repositoryCache = Maps.newHashMap();
    private final TypeManager typeManager;
    private final IdGenerator idGenerator;
    private final RecordFactory recordFactory;
    private final TenantModel tenantModel;


    public AbstractRepositoryManager(TypeManager typeManager, IdGenerator idGenerator, RecordFactory recordFactory,
            TenantModel tenantModel) {
        this.typeManager = typeManager;
        this.idGenerator = idGenerator;
        this.recordFactory = recordFactory;
        this.tenantModel = tenantModel;
    }

    protected TypeManager getTypeManager() {
        return typeManager;
    }

    @Override
    public IdGenerator getIdGenerator() {
        return idGenerator;
    }

    protected RecordFactory getRecordFactory() {
        return recordFactory;
    }

    /**
     * Create a new Repository object for the repository cache.
     */
    protected abstract Repository createRepository(TenantTableKey key) throws InterruptedException, RepositoryException;

    @Override
    public LRepository getPublicRepository() throws InterruptedException, RepositoryException {
        return getRepository("public");
    }

    @Override
    public LRepository getRepository(String tenantName) throws InterruptedException, RepositoryException {
        return getRepository(tenantName, Table.RECORD.name);
    }

    public Repository getRepository(String tenantName, String tableName)
            throws InterruptedException, RepositoryException {
        if (!tenantModel.tenantExistsAndActive(tenantName)) {
            throw new RepositoryException("Tenant does not exist or is not active: " + tenantName);
        }
        TenantTableKey key = new TenantTableKey(tenantName, tableName);
        if (!repositoryCache.containsKey(key)) {
            synchronized (repositoryCache) {
                if (!repositoryCache.containsKey(key)) {
                    repositoryCache.put(key, createRepository(key));
                }
            }
        }
        return repositoryCache.get(key);
    }

    @Override
    public LTable getTable(String tableName) throws InterruptedException, RepositoryException {
        return getPublicRepository().getTable(tableName);
    }

    @Override
    public LTable getDefaultTable() throws InterruptedException, RepositoryException {
        return getPublicRepository().getDefaultTable();
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
