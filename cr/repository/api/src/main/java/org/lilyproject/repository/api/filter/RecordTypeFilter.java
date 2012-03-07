package org.lilyproject.repository.api.filter;

import org.lilyproject.repository.api.QName;

/**
 * Filters on the record type of records.
 *
 * <p>It is the record type from the non-versioned scope based on which the filtering is performed.</p>
 */
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

    /**
     * Get the version of the record type. This is optional, thus can be null.
     */
    public Long getVersion() {
        return version;
    }

    /**
     * Set the version of the record type. This is optional, thus can be null.
     */
    public void setVersion(Long version) {
        this.version = version;
    }
}
