package org.lilyproject.repository.api.filter;

import org.lilyproject.repository.api.RecordId;

public class RecordIdPrefixFilter implements RecordFilter {
    private RecordId recordId;
    
    public RecordIdPrefixFilter(RecordId recordId) {
        this.recordId = recordId;
    }

    public RecordId getRecordId() {
        return recordId;
    }

    public void setRecordId(RecordId recordId) {
        this.recordId = recordId;
    }
}
