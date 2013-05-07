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
package org.lilyproject.util.repo;

import java.util.regex.Pattern;

public class TenantTableUtil {
    public static final String PUBLIC_TENANT = "public";

    public static final String TENANT_TABLE_SEPARATOR = "__";

    private static final String VALID_NAME_PATTERN = "[a-zA-Z_0-9-.]+";
    private static final Pattern VALID_NAME_CHARS = Pattern.compile(VALID_NAME_PATTERN);
    public static final String VALID_NAME_EXPLANATION = "A valid name should follow the regex " + VALID_NAME_PATTERN
            + " and not contain " + TENANT_TABLE_SEPARATOR + ".";

    /**
     * Checks if a table is owned by a certain tenant. Assumes the passed table name is a record table name,
     * i.e. this does not filter out any other tables that might exist in HBase.
     */
    public static final boolean belongsToTenant(String hbaseTableName, String tenantName) {
        if (tenantName.equals(PUBLIC_TENANT)) {
            return !hbaseTableName.contains(TENANT_TABLE_SEPARATOR);
        } else {
            return hbaseTableName.startsWith(tenantName + TENANT_TABLE_SEPARATOR);
        }
    }

    public static String getHBaseTableName(String tenantName, String tableName) {
        if (tenantName.equals(PUBLIC_TENANT)) {
            // Tables within the public tenant are not prefixed with the tenant name, because of backwards
            // compatibility with the pre-tenant situation.
            return tableName;
        } else {
            return tenantName + TENANT_TABLE_SEPARATOR + tableName;
        }
    }

    public static boolean isValidTableName(String name) {
        return VALID_NAME_CHARS.matcher(name).matches() && !name.contains(TENANT_TABLE_SEPARATOR);
    }

    private static boolean isPublicTenant(String name) {
        return PUBLIC_TENANT.equals(name);
    }

    public static String extractLilyTableName(String tenantName, String hbaseTableName) {
        if (isPublicTenant(tenantName)) {
            return hbaseTableName;
        } else {
            String prefix = tenantName + TENANT_TABLE_SEPARATOR;
            if (!hbaseTableName.startsWith(prefix)) {
                throw new IllegalArgumentException(String.format("HBase table '%s' does not belong to tenant '%s'",
                        hbaseTableName, tenantName));
            }
            return hbaseTableName.substring(prefix.length());
        }
    }

    /**
     * Splits an HBase table name into Lily tenant and table names. Assumes the provided HBase table name is a valid
     * Lily record table (should be check on beforehand).
     *
     * @return an array of size 2: first element is tenant name, second is table name
     */
    public static String[] getTenantAndTable(String hbaseTableName) {
        int pos = hbaseTableName.indexOf(TENANT_TABLE_SEPARATOR);
        if (pos == -1) {
            return new String[] { PUBLIC_TENANT, hbaseTableName };
        } else {
            String tenant = hbaseTableName.substring(0, pos);
            String table = hbaseTableName.substring(pos + TENANT_TABLE_SEPARATOR.length());
            return new String[] { tenant, table };
        }
    }
}
