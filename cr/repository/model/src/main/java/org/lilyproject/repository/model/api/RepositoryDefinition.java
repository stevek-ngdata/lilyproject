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

import com.google.common.base.Preconditions;

/**
 * Definition of a tenant in the context of multi-tenancy.
 *
 * <p>Lily supports multi-tenancy, whereby each tenant has its own Repository containing its own tables.</p>
 *
 * <p>Tenants are managed through {@link RepositoryModel}.</p>
 */
public class RepositoryDefinition {
    private String name;
    private RepositoryLifecycleState lifecycleState;

    public RepositoryDefinition(String name, RepositoryLifecycleState lifecycleState) {
        Preconditions.checkNotNull(name, "name");
        Preconditions.checkNotNull(lifecycleState, "lifecycleState");

        this.name = name;
        this.lifecycleState = lifecycleState;
    }

    public String getName() {
        return name;
    }

    public RepositoryLifecycleState getLifecycleState() {
        return lifecycleState;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RepositoryDefinition repoDef = (RepositoryDefinition)o;

        if (lifecycleState != repoDef.lifecycleState) return false;
        if (!name.equals(repoDef.name)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + lifecycleState.hashCode();
        return result;
    }

    public static enum RepositoryLifecycleState {
        ACTIVE, CREATE_REQUESTED, DELETE_REQUESTED
    }
}
