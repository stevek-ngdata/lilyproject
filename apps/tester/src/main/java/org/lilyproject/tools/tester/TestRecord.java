package org.lilyproject.tools.tester;

import org.lilyproject.repository.api.RecordId;

public class TestRecord {
    private RecordId recordId;
    private boolean deleted;
    private TestRecordType recordType;

    public TestRecord(RecordId recordId, TestRecordType recordType) {
        this.recordId = recordId;
        this.recordType = recordType;
    }
    
    public boolean isDeleted() {
        return deleted;
    }
    
    public RecordId getRecordId() {
        return recordId;
    }
    
    public TestRecordType getRecordType() {
        return recordType;
    }

    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((recordId == null) ? 0 : recordId.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TestRecord other = (TestRecord) obj;
        if (recordId == null) {
            if (other.recordId != null)
                return false;
        } else if (!recordId.equals(other.recordId))
            return false;
        return true;
    }
}
