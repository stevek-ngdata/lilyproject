package org.lilyproject.repository.api;

import java.util.HashMap;
import java.util.Map;

public class TableNotFoundException extends RepositoryException {
    private String repositoryName;
    private String tableName;

    public TableNotFoundException(String repositoryName, String tableName) {
        this.repositoryName = repositoryName;
        this.tableName = tableName;
    }

    public TableNotFoundException(String message, Map<String, String> state) {
        this.repositoryName = state.get("repositoryName");
        this.tableName = state.get("tableName");
    }

    @Override
    public Map<String, String> getState() {
        Map<String, String> state = new HashMap<String, String>();
        state.put("repositoryName", repositoryName);
        state.put("tableName", tableName);
        return state;
    }

    @Override
    public String getMessage() {
        return String.format("Table '%s' for repository '%s'.", tableName, repositoryName);
    }
}
