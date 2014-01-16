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
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.ngdata.lily.security.hbase.client.AuthEnabledHTable;
import com.ngdata.lily.security.hbase.client.AuthorizationContextProvider;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.LRepository;
import org.lilyproject.repository.api.RecordFactory;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.RepositoryManager;
import org.lilyproject.repository.api.RepositoryUnavailableException;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.model.api.RepositoryModel;
import org.lilyproject.util.hbase.LilyHBaseSchema;
import org.lilyproject.util.hbase.LilyHBaseSchema.Table;
import org.lilyproject.util.hbase.RepoAndTableUtil;
import org.lilyproject.util.io.Closer;

/**
 * Handles thread-safe creation and caching of Repository objects.
 */
public abstract class AbstractRepositoryManager implements RepositoryManager {

    private final Map<RepoTableKey, Repository> repositoryCache = Maps.newHashMap();
    private final TypeManager typeManager;
    private final IdGenerator idGenerator;
    private final RecordFactory recordFactory;
    private final RepositoryModel repositoryModel;
    private final AuthorizationContextProvider authzCtxProvider = new DRAuthorizationContextProvider();

    /**
     * For NGDATA's hbase authorization layer: unique name for the application, in order to
     * identify the relevant set of permissions.
     */
    private static final String PERMISSION_APP_NAME = "hbase-dr";

    /**
     * For NGDATA's hbase authorization layer: default permissions that ensure the system columns are
     * always visible by the Lily Data Repository.
     */
    private static final Set<String> DEFAULT_PERMISSIONS = ImmutableSet.of(
            PERMISSION_APP_NAME + ":rw#column:qualifier:bin_starts_with:"
                    + Bytes.toStringBinary(new byte[]{LilyHBaseSchema.RecordColumn.SYSTEM_PREFIX}));

    /**
     * For NGDATA's hbase authorization layer: the types of permissions for which access needs to be granted.
     */
    private static final Set<String> ROW_PERMISSION_TYPES = ImmutableSet.of("row_labels", "row_recordtype");

    public AbstractRepositoryManager(TypeManager typeManager, IdGenerator idGenerator, RecordFactory recordFactory,
            RepositoryModel repositoryModel) {
        this.typeManager = typeManager;
        this.idGenerator = idGenerator;
        this.recordFactory = recordFactory;
        this.repositoryModel = repositoryModel;
    }

    protected HTableInterface wrapWithAuthorization(HTableInterface htable) {
        return new AuthEnabledHTable(authzCtxProvider, false, PERMISSION_APP_NAME, DEFAULT_PERMISSIONS, htable);
    }

    protected TypeManager getTypeManager() {
        return typeManager;
    }

    protected IdGenerator getIdGenerator() {
        return idGenerator;
    }

    protected RecordFactory getRecordFactory() {
        return recordFactory;
    }

    /**
     * Create a new Repository object for the repository cache.
     */
    protected abstract Repository createRepository(RepoTableKey key) throws InterruptedException, RepositoryException;

    @Override
    public LRepository getDefaultRepository() throws InterruptedException, RepositoryException {
        return getRepository(RepoAndTableUtil.DEFAULT_REPOSITORY);
    }

    @Override
    public LRepository getRepository(String repositoryName) throws InterruptedException, RepositoryException {
        return getRepository(repositoryName, Table.RECORD.name);
    }

    public Repository getRepository(String repositoryName, String tableName)
            throws InterruptedException, RepositoryException {
        if (!repositoryModel.repositoryExistsAndActive(repositoryName)) {
            throw new RepositoryUnavailableException("Repository does not exist or is not active: " + repositoryName);
        }
        RepoTableKey key = new RepoTableKey(repositoryName, tableName);
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
    public synchronized void close() throws IOException {
        if (shouldCloseRepositories()) {
            for (Repository repository : repositoryCache.values()) {
                Closer.close(repository);
            }
        }
        repositoryCache.clear();
        Closer.close(typeManager);
    }

    protected boolean shouldCloseRepositories() {
        return true;
    }

}
