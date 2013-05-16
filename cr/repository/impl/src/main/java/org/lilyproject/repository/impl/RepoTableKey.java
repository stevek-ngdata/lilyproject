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
package org.lilyproject.repository.impl;

import org.lilyproject.util.hbase.RepoAndTableUtil;

import com.google.common.base.Preconditions;

/**
 * An object that can be used anywhere the combination of repository name and table name is needed.
 *
 */
public class RepoTableKey {
    private final String repositoryName;
    private final String tableName;

    public RepoTableKey(String repositoryName, String tableName) {
        Preconditions.checkNotNull(repositoryName, "repositoryName");
        Preconditions.checkNotNull(tableName, "tableName");
        this.repositoryName = repositoryName;
        this.tableName = tableName;
    }

    public String getRepositoryName() {
        return repositoryName;
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RepoTableKey that = (RepoTableKey)o;

        if (!tableName.equals(that.tableName)) return false;
        if (!repositoryName.equals(that.repositoryName)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = repositoryName.hashCode();
        result = 31 * result + tableName.hashCode();
        return result;
    }
}
