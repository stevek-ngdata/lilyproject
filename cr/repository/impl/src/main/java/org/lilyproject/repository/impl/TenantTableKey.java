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

import com.google.common.base.Preconditions;
import org.lilyproject.tenant.model.impl.TenantTableUtil;

/**
 * An object that can be used anywhere the combination of tenant and table is needed.
 *
 */
public class TenantTableKey {
    private final String tenantId;
    private final String tableName;

    public TenantTableKey(String tenantId, String tableName) {
        Preconditions.checkNotNull(tenantId, "tenantId");
        Preconditions.checkNotNull(tableName, "tableName");
        this.tenantId = tenantId;
        this.tableName = tableName;
    }

    public String getTenantId() {
        return tenantId;
    }

    public String getTableName() {
        return tableName;
    }

    public String toHBaseTableName() {
        return TenantTableUtil.getHBaseTableName(tenantId, tableName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TenantTableKey that = (TenantTableKey)o;

        if (!tableName.equals(that.tableName)) return false;
        if (!tenantId.equals(that.tenantId)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = tenantId.hashCode();
        result = 31 * result + tableName.hashCode();
        return result;
    }
}
