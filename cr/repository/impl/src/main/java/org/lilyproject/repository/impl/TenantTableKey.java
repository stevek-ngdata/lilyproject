package org.lilyproject.repository.impl;

import com.google.common.base.Preconditions;

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
        return RepoUtil.getHBaseTableName(tenantId, tableName);
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
