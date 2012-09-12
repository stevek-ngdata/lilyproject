package org.lilyproject.util.hbase;

import java.util.regex.Pattern;

public class TableConfigEntry {
    private Pattern tableNamePattern;
    private TableConfig tableConfig;

    public TableConfigEntry(Pattern tableNamePattern, TableConfig tableConfig) {
        this.tableNamePattern = tableNamePattern;
        this.tableConfig = tableConfig;
    }

    public boolean matches(String tableName) {
        return tableNamePattern.matcher(tableName).matches();
    }

    public Pattern getTableNamePattern() {
        return tableNamePattern;
    }

    public TableConfig getTableConfig() {
        return tableConfig;
    }
}
