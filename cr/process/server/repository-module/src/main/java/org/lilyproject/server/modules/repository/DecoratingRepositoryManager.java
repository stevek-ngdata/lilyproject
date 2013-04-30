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
package org.lilyproject.server.modules.repository;

import java.io.IOException;

import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.impl.AbstractRepositoryManager;
import org.lilyproject.repository.impl.HBaseRepository;
import org.lilyproject.repository.impl.HBaseRepositoryManager;
import org.lilyproject.repository.impl.TenantTableKey;
import org.lilyproject.tenant.model.api.TenantModel;

public class DecoratingRepositoryManager extends AbstractRepositoryManager {

    private HBaseRepositoryManager wrappedRepositoryManager;
    private RecordUpdateHookActivator recordUpdateHookActivator;
    private RepositoryDecoratorActivator repositoryDecoratorActivator;

    public DecoratingRepositoryManager(HBaseRepositoryManager repositoryManager,
            RecordUpdateHookActivator recordUpdateHookActivator,
            RepositoryDecoratorActivator repositoryDecoratorActivator,
            TenantModel tenantModel) {
        super(repositoryManager.getTypeManager(), repositoryManager.getIdGenerator(),
                repositoryManager.getRecordFactory(), tenantModel);
        this.wrappedRepositoryManager = repositoryManager;
        this.recordUpdateHookActivator = recordUpdateHookActivator;
        this.repositoryDecoratorActivator = repositoryDecoratorActivator;
    }

    @Override
    protected Repository createRepository(TenantTableKey key)
            throws IOException, InterruptedException, RepositoryException {
        HBaseRepository repository = (HBaseRepository)wrappedRepositoryManager.getRepository(
                key.getTenantName()).getTable(key.getTableName());
        recordUpdateHookActivator.activateUpdateHooks(repository);
        return new DecoratingRepository(repositoryDecoratorActivator.getDecoratedRepository(repository),
                key.getTenantName(), this);
    }

    @Override
    protected Repository getRepository(String tenantName, String tableName) throws IOException, InterruptedException, RepositoryException {
        return super.getRepository(tenantName, tableName);
    }
}
