package org.lilycms.repository.api;

import java.util.Map;
import java.util.Map.Entry;

public class RecordExistsException extends Exception {
    private final String recordId;
    private final Map<String, String> variantProperties;

    public RecordExistsException(String recordId, Map<String, String> variantProperties) {
        this.recordId = recordId;
        this.variantProperties = variantProperties;
    }

    public String getRecordId() {
        return recordId;
    }
    
    public Map<String, String> getVariantProperties() {
        return variantProperties;
    }
    
    @Override
    public String getMessage() {
        StringBuffer message = new StringBuffer();
        message.append("Record <");
        message.append(recordId);
        message.append("> ");
        if (variantProperties != null && !variantProperties.isEmpty()) {
            for (Entry<String, String> variantEntry : variantProperties.entrySet()) {
                message.append("<");
                message.append(variantEntry.getKey());
                message.append(":");
                message.append(variantEntry.getValue());
                message.append("> ");
            }
        }
        message.append("already exists");
        return message.toString();
    }
}
