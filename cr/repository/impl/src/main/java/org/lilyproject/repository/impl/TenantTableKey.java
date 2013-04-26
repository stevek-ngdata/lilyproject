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
    private final String tenantName;
    private final String tableName;

    public TenantTableKey(String tenantName, String tableName) {
        Preconditions.checkNotNull(tenantName, "tenantName");
        Preconditions.checkNotNull(tableName, "tableName");
        this.tenantName = tenantName;
        this.tableName = tableName;
    }

    public String getTenantName() {
        return tenantName;
    }

    public String getTableName() {
        return tableName;
    }

    public String toHBaseTableName() {
        return TenantTableUtil.getHBaseTableName(tenantName, tableName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TenantTableKey that = (TenantTableKey)o;

        if (!tableName.equals(that.tableName)) return false;
        if (!tenantName.equals(that.tenantName)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = tenantName.hashCode();
        result = 31 * result + tableName.hashCode();
        return result;
    }
}
