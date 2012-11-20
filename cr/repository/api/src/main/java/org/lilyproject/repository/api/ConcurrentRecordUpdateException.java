package org.lilyproject.repository.api;

import java.util.HashMap;
import java.util.Map;

public class ConcurrentRecordUpdateException  extends RecordException {
    private String recordId;

    public ConcurrentRecordUpdateException(String message, Map<String, String> state) {
        this.recordId = state.get("recordId");
    }

    @Override
    public Map<String, String> getState() {
        Map<String, String> state = new HashMap<String, String>();
        state.put("recordId", recordId);
        return state;
    }

    public ConcurrentRecordUpdateException(RecordId recordId) {
        this.recordId = recordId != null ? recordId.toString() : null;
    }

    @Override
    public String getMessage() {
        return "Concurrent update on record " + recordId;
    }
}

