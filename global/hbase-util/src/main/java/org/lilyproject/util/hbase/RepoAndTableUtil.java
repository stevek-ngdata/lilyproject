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
package org.lilyproject.util.hbase;

import java.util.regex.Pattern;

import org.apache.hadoop.hbase.HTableDescriptor;

/**
 * Utility that handles the mapping between repository/table names and HBase storage table names.
 */
public class RepoAndTableUtil {
    public static final String DEFAULT_REPOSITORY = "default";

    /**
     * Separator between repository name and table name in the HBase table name.
     */
    public static final String REPOSITORY_TABLE_SEPARATOR = "__";

    private static final String VALID_NAME_PATTERN = "[a-zA-Z_0-9-.]+";
    private static final Pattern VALID_NAME_CHARS = Pattern.compile(VALID_NAME_PATTERN);
    public static final String VALID_NAME_EXPLANATION = "A valid name should follow the regex " + VALID_NAME_PATTERN
            + " and not contain " + REPOSITORY_TABLE_SEPARATOR + ".";

    static final String OWNING_REPOSITORY_KEY = "lilyOwningRepository";

    /**
     * Checks if a table is owned by a certain repository. Assumes the passed table name is a record table name,
     * i.e. this does not filter out any other tables that might exist in HBase.
     */
    public static final boolean belongsToRepository(String hbaseTableName, String repositoryName) {
        if (repositoryName.equals(DEFAULT_REPOSITORY)) {
            return !hbaseTableName.contains(REPOSITORY_TABLE_SEPARATOR);
        } else {
            return hbaseTableName.startsWith(repositoryName + REPOSITORY_TABLE_SEPARATOR);
        }
    }

    public static String getHBaseTableName(String repositoryName, String tableName) {
        if (repositoryName.equals(DEFAULT_REPOSITORY)) {
            // Tables within the default repository are not prefixed with the repository name, because of backwards
            // compatibility with the pre-multiple-repositories situation.
            return tableName;
        } else {
            return repositoryName + REPOSITORY_TABLE_SEPARATOR + tableName;
        }
    }

    public static boolean isValidTableName(String name) {
        return VALID_NAME_CHARS.matcher(name).matches() && !name.contains(REPOSITORY_TABLE_SEPARATOR);
    }

    private static boolean isDefaultRepository(String name) {
        return DEFAULT_REPOSITORY.equals(name);
    }

    public static String extractLilyTableName(String repositoryName, String hbaseTableName) {
        if (isDefaultRepository(repositoryName)) {
            return hbaseTableName;
        } else {
            String prefix = repositoryName + REPOSITORY_TABLE_SEPARATOR;
            if (!hbaseTableName.startsWith(prefix)) {
                throw new IllegalArgumentException(String.format("HBase table '%s' does not belong to repository '%s'",
                        hbaseTableName, repositoryName));
            }
            return hbaseTableName.substring(prefix.length());
        }
    }

    /**
     * Splits an HBase table name into Lily repository and table names. Assumes the provided HBase table name is a valid
     * Lily record table (should be check on beforehand).
     *
     * @return an array of size 2: first element is repository name, second is table name
     */
    public static String[] getRepositoryAndTable(String hbaseTableName) {
        int pos = hbaseTableName.indexOf(REPOSITORY_TABLE_SEPARATOR);
        if (pos == -1) {
            return new String[] {DEFAULT_REPOSITORY, hbaseTableName };
        } else {
            String repository = hbaseTableName.substring(0, pos);
            String table = hbaseTableName.substring(pos + REPOSITORY_TABLE_SEPARATOR.length());
            return new String[] { repository, table };
        }
    }
    
    /**
     * Mark a table descriptor as being owned by a repository. This method is intended to be used before the
     * table defined by the descriptor is created.
     * 
     * @param tableDescriptor descriptor of a table to be created
     * @param repositoryName name of the repository to which the table will belong
     */
    public static void setRepositoryOwnership(HTableDescriptor tableDescriptor, String repositoryName) {
        String existingValue = tableDescriptor.getValue(OWNING_REPOSITORY_KEY);
        if (existingValue != null && !existingValue.equals(repositoryName)) {
            throw new IllegalStateException("Table descriptor already belongs to repository '"
                                            + existingValue + "', can't set owning repository to " + repositoryName);
        }
        tableDescriptor.setValue(OWNING_REPOSITORY_KEY, repositoryName);
    }
    
    /**
     * Get the name of the owning repository for a table descriptor.
     * 
     * @param tableDescriptor descriptor for which the owning repository is to be retrieved
     * @return the owning repository
     */
    public static String getOwningRepository(HTableDescriptor tableDescriptor) {
        return tableDescriptor.getValue(OWNING_REPOSITORY_KEY);
    }
}
