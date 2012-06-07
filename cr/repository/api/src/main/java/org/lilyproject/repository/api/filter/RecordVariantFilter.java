package org.lilyproject.repository.api.filter;

import org.lilyproject.repository.api.RecordId;

/**
 * Filters based on variant properties. It returns only records which have the same variant properties (they keys, not
 * the values) as the given record id.
 *
 * @author Jan Van Besien
 */
public class RecordVariantFilter implements RecordFilter {
    private RecordId recordId;

    public RecordVariantFilter(RecordId recordId) {
        this.recordId = recordId;
    }

    public RecordId getRecordId() {
        return recordId;
    }

    public void setRecordId(RecordId recordId) {
        this.recordId = recordId;
    }
}
