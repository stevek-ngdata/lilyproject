package org.lilyproject.repository.api;

import java.util.HashMap;
import java.util.Map;

public class TableNotFoundException extends RepositoryException {
    private String tenantName;
    private String tableName;

    public TableNotFoundException(String tenantName, String tableName) {
        this.tenantName = tenantName;
        this.tableName = tableName;
    }

    public TableNotFoundException(String message, Map<String, String> state) {
        this.tenantName = state.get("tenantName");
        this.tableName = state.get("tableName");
    }

    @Override
    public Map<String, String> getState() {
        Map<String, String> state = new HashMap<String, String>();
        state.put("tenantName", tenantName);
        state.put("tableName", tableName);
        return state;
    }

    @Override
    public String getMessage() {
        return String.format("Table '%s' for tenant '%s'.", tableName, tenantName);
    }
}
