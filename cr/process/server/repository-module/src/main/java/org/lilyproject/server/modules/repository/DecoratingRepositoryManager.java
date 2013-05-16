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

import com.google.common.collect.Maps;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.RecordFactory;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.impl.AbstractRepositoryManager;
import org.lilyproject.repository.impl.HBaseRepository;
import org.lilyproject.repository.impl.HBaseRepositoryManager;
import org.lilyproject.repository.impl.RepoTableKey;
import org.lilyproject.repository.model.api.RepositoryModel;
import org.lilyproject.util.io.Closer;

import java.io.IOException;
import java.util.Map;

public class DecoratingRepositoryManager extends AbstractRepositoryManager {

    private HBaseRepositoryManager wrappedRepositoryManager;
    private RecordUpdateHookActivator recordUpdateHookActivator;
    private RepositoryDecoratorActivator repositoryDecoratorActivator;
    private final Map<RepoTableKey, RepositoryDecoratorChain> decoratoredRepositoryCache = Maps.newHashMap();

    public DecoratingRepositoryManager(HBaseRepositoryManager repositoryManager,
            RecordUpdateHookActivator recordUpdateHookActivator,
            RepositoryDecoratorActivator repositoryDecoratorActivator,
            RepositoryModel repositoryModel, RecordFactory recordFactory, TypeManager typeManager,
            IdGenerator idGenerator) {
        super(typeManager, idGenerator, recordFactory, repositoryModel);
        this.wrappedRepositoryManager = repositoryManager;
        this.recordUpdateHookActivator = recordUpdateHookActivator;
        this.repositoryDecoratorActivator = repositoryDecoratorActivator;
    }

    @Override
    protected Repository createRepository(RepoTableKey key)
            throws InterruptedException, RepositoryException {

        if (decoratoredRepositoryCache.containsKey(key)) {
            throw new RuntimeException("Unexpected: createRepository is called twice for " + key);
        }

        HBaseRepository repository = (HBaseRepository)wrappedRepositoryManager.getRepository(
                key.getRepositoryName()).getTable(key.getTableName());

        recordUpdateHookActivator.activateUpdateHooks(repository);

        RepositoryDecoratorChain chain = repositoryDecoratorActivator.getDecoratedRepository(repository);
        decoratoredRepositoryCache.put(key, chain);

        return new DecoratingRepository(chain.getFullyDecoratoredRepository(), key.getRepositoryName(), this);
    }

    /**
     * @return null if it doesn't exist
     */
    public RepositoryDecoratorChain getRepositoryDecoratorChain(String repositoryName, String tableName) {
        return decoratoredRepositoryCache.get(new RepoTableKey(repositoryName, tableName));
    }

    @Override
    public synchronized void close() throws IOException {
        // We close each of the repository-decorators in the chain individually, since each of them might
        // have state/resources that they need to clean up, and we don't want to rely on the decorators
        // themselves forwarding the close call.
        for (RepositoryDecoratorChain chain : decoratoredRepositoryCache.values()) {
            for (RepositoryDecoratorChain.Entry entry : chain.getEntries()) {
                Closer.close(entry.repository);
            }
        }
        decoratoredRepositoryCache.clear();
    }

    @Override
    protected boolean shouldCloseRepositories() {
        // we close the repositories ourselves in our close()
        return false;
    }
}
