package org.lilyproject.repository.api.filter;

import org.lilyproject.repository.api.QName;

public class RecordTypeFilter implements RecordFilter {
    private QName recordType;
    private Long version;

    public RecordTypeFilter() {
    }
    
    public RecordTypeFilter(QName recordType) {
        this.recordType = recordType;
    }

    public RecordTypeFilter(QName recordType, Long version) {
        this.recordType = recordType;
        this.version = version;
    }

    public QName getRecordType() {
        return recordType;
    }

    public void setRecordType(QName recordType) {
        this.recordType = recordType;
    }

    public Long getVersion() {
        return version;
    }

    public void setVersion(Long version) {
        this.version = version;
    }
}
