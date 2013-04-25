package org.lilyproject.repository.impl;

public class RepoUtil {
    public static final String PUBLIC_TENANT = "public";

    public static final String TENANT_TABLE_SEPARATOR = "__";

    /**
     * Checks if a table is owned by a certain tenant. Assumes the passed table name is a record table name,
     * i.e. this does not filter out any other tables that might exist in HBase.
     */
    public static final boolean belongsToTenant(String hbaseTableName, String tenantId) {
        if (tenantId.equals(PUBLIC_TENANT)) {
            return !hbaseTableName.contains("__");
        } else {
            return hbaseTableName.startsWith(tenantId + TENANT_TABLE_SEPARATOR);
        }
    }

    public static String getHBaseTableName(String tenantId, String tableName) {
        if (tenantId.equals(PUBLIC_TENANT)) {
            return tableName;
        } else {
            return tenantId + "__" + tableName;
        }
    }
}
