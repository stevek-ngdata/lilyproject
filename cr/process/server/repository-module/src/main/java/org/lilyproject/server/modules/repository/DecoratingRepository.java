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
package org.lilyproject.server.modules.repository;

import org.lilyproject.repository.api.LTable;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.spi.BaseRepositoryDecorator;
import org.lilyproject.util.hbase.LilyHBaseSchema;

import org.lilyproject.util.repo.RepoAndTableUtil;

/**
 * A Repository which calls back to the DecoratingRepositoryManager when it needs repository objects,
 * so that these are also decorated, and managed from the same place (in the same cache).
 */
public class DecoratingRepository extends BaseRepositoryDecorator {
    private DecoratingRepositoryManager decoratingRepositoryManager;
    private String repositoryName;

    public DecoratingRepository(Repository delegate, String repositoryName, DecoratingRepositoryManager decoratingRepositoryManager) {
        setDelegate(delegate);
        this.decoratingRepositoryManager = decoratingRepositoryManager;
        this.repositoryName = repositoryName;
    }

    @Override
    public LTable getTable(String tableName) throws InterruptedException, RepositoryException {
        return decoratingRepositoryManager.getRepository(repositoryName, tableName);
    }

    @Override
    public LTable getDefaultTable() throws InterruptedException, RepositoryException {
        return decoratingRepositoryManager.getRepository(RepoAndTableUtil.DEFAULT_REPOSITORY, LilyHBaseSchema.Table.RECORD.name);
    }
}
