package org.lilycms.repository.api;

import java.util.Map;
import java.util.Map.Entry;

public class RecordNotFoundException extends Exception {

    private final String recordId;
    private final Long version;
    private final Map<String, String> variantProperties;


    public RecordNotFoundException(String recordId, Long version, Map<String, String> variantProperties) {
        this.recordId = recordId;
        this.version = version;
        this.variantProperties = variantProperties;
    }
    
    public String getRecordId() {
        return recordId;
    }
    
    public Map<String, String> getVariantProperties() {
        return variantProperties;
    }
    
    public Long getVersion() {
        return version;
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
        if (version != null) {
            message.append("<version:");
            message.append(version);
            message.append(">");
        }
        message.append("not found");
        return message.toString();
    }
}
