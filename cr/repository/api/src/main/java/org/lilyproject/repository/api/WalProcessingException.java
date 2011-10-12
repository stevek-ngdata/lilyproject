package org.lilyproject.repository.api;

import java.util.HashMap;
import java.util.Map;


public class WalProcessingException extends RepositoryException {
    private String recordId;
    private String info;

    public WalProcessingException(String message, Map<String, String> state) {
        this.recordId = state.get("recordId");
        this.info = state.get("info");
    }
    
    @Override
    public Map<String, String> getState() {
        Map<String, String> state = new HashMap<String, String>();
        state.put("recordId", recordId);
        state.put("info", info);
        return state;
    }
    
    public WalProcessingException(RecordId recordId, String info) {
        this.info = info;
        this.recordId = recordId != null ? recordId.toString() : null;
    }

    public WalProcessingException(RecordId recordId, Throwable cause) {
        super(cause);
        this.recordId = recordId != null ? recordId.toString() : null;
    }

    @Override
    public String getMessage() {
        return "Wal failed to process messages for record '" + recordId + "', " + info;
    }
}