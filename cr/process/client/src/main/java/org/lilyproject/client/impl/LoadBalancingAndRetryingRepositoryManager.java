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
package org.lilyproject.client.impl;

import com.ngdata.lily.security.hbase.client.AuthorizationContextProvider;
import org.lilyproject.client.RetryConf;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.LTable;
import org.lilyproject.repository.api.RecordFactory;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.impl.AbstractRepositoryManager;
import org.lilyproject.repository.impl.RepoTableKey;
import org.lilyproject.repository.spi.BaseRepositoryDecorator;
import org.lilyproject.repository.model.api.RepositoryModel;
import org.lilyproject.util.hbase.LilyHBaseSchema;

/**
 * An implementation of RepositoryManager whose LRepository, LTable and TypeManager will balance operations
 * over different instances of these objects. This balancing happens transparently for each method call, i.e.
 * it is not necessary to retrieve new repository or table objects. Also, methods calls are wrapped to add
 * automatic retrying behavior for certain kinds of IO errors (if the operation is idempotent), so that
 * clients are unaware of things like a lily server process that is being restarted.
 */
public class LoadBalancingAndRetryingRepositoryManager extends AbstractRepositoryManager {
    private final RetryConf retryConf;
    private final LoadBalancingUtil.LBInstanceProvider<Repository> repositoryProvider;
    private final LoadBalancingUtil.LBInstanceProvider<TypeManager> typeManagerProvider;

    public LoadBalancingAndRetryingRepositoryManager(LoadBalancingUtil.LBInstanceProvider<Repository> repositoryProvider,
            LoadBalancingUtil.LBInstanceProvider<TypeManager> typeManagerProvider, RetryConf retryConf, IdGenerator idGenerator,
            RecordFactory recordFactory, RepositoryModel repositoryModel, AuthorizationContextProvider authzCtxProvider) {
        super(createTypeManager(typeManagerProvider, retryConf), idGenerator, recordFactory, repositoryModel,
                authzCtxProvider);
        this.repositoryProvider = repositoryProvider;
        this.typeManagerProvider = typeManagerProvider;
        this.retryConf = retryConf;
    }

    private static TypeManager createTypeManager(LoadBalancingUtil.LBInstanceProvider<TypeManager> typeManagerProvider, RetryConf retryConf) {
        return RetryUtil.getRetryingInstance(LoadBalancingUtil.getLoadBalancedInstance(typeManagerProvider,
                TypeManager.class, null, null), TypeManager.class, retryConf);
    }

    @Override
    protected Repository createRepository(RepoTableKey key) throws InterruptedException, RepositoryException {
        // Note that the parent caches these instances
        return new LBAwareRepository(RetryUtil.getRetryingInstance(
                LoadBalancingUtil.getLoadBalancedInstance(repositoryProvider, Repository.class, key.getRepositoryName(),
                        key.getTableName()), Repository.class, retryConf), this, key.getRepositoryName());
    }

    /**
     * Repository also serves as a factory for tables and a registry for other services (such as typemanager),
     * and for some of these it is important to also return the load-balancing-and-retrying instances (if they would
     * just call the delegate, they would return the unwrapped instances). This wrapper takes care of that.
     */
    public class LBAwareRepository extends BaseRepositoryDecorator {
        private TypeManager typeManager;
        private AbstractRepositoryManager repositoryManager;
        private String repositoryName;

        /**
         *
         * @param repositoryManager the one which gives load-balancing-and-retrying repositories
         */
        public LBAwareRepository(Repository delegate, AbstractRepositoryManager repositoryManager, String repositoryName) {
            super(delegate);
            this.repositoryManager = repositoryManager;
            this.repositoryName = repositoryName;
        }

        @Override
        public LTable getTable(String tableName) throws InterruptedException, RepositoryException {
            return repositoryManager.getRepository(repositoryName, tableName);
        }

        @Override
        public LTable getDefaultTable() throws InterruptedException, RepositoryException {
            return repositoryManager.getRepository(repositoryName, LilyHBaseSchema.Table.RECORD.name);
        }

        @Override
        public TypeManager getTypeManager() {
            if (typeManager == null) {
                typeManager = RetryUtil.getRetryingInstance(LoadBalancingUtil.getLoadBalancedInstance(typeManagerProvider,
                        TypeManager.class, null, null), TypeManager.class, retryConf);
            }
            return typeManager;
        }
    }
}
