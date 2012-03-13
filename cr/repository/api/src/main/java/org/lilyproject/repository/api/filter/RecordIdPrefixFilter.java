package org.lilyproject.repository.api.filter;

import org.lilyproject.repository.api.RecordId;

/**
 * A filter which lets through records whose ID starts with the given RecordId.
 *
 * <p>It does not make sense to use this with UUID-based record IDs.</p>
 * 
 * <p>When using this filter, you should set
 * {@link org.lilyproject.repository.api.RecordScan#setStartRecordId(org.lilyproject.repository.api.RecordId)}
 * to the same RecordId as set here, since any earlier records will be dropped
 * anyway by this filter.</p>
 *
 * <p>The scan will stop as soon as a record ID is encountered which is larger than
 * what can be matched by the prefix. So the scan will not needlessly run to the end
 * of the table.</p>
 */
public class RecordIdPrefixFilter implements RecordFilter {
    private RecordId recordId;

    public RecordIdPrefixFilter() {
    }
    
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
