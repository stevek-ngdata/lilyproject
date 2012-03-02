package org.lilyproject.repository.api.filter;

import org.lilyproject.repository.api.CompareOp;
import org.lilyproject.repository.api.QName;

public class FieldValueFilter implements RecordFilter {
    private QName field;
    private Object fieldValue;
    private CompareOp compareOp = CompareOp.EQUAL;
    private boolean filterIfMissing = true;

    public FieldValueFilter() {
    }
    
    public FieldValueFilter(QName field, Object fieldValue) {
        this.field = field;
        this.fieldValue = fieldValue;
    }

    public FieldValueFilter(QName field, CompareOp compareOp, Object fieldValue) {
        this.field = field;
        this.compareOp = compareOp;
        this.fieldValue = fieldValue;
    }

    public QName getField() {
        return field;
    }

    public void setField(QName field) {
        this.field = field;
    }

    public Object getFieldValue() {
        return fieldValue;
    }

    public void setFieldValue(Object fieldValue) {
        this.fieldValue = fieldValue;
    }

    public CompareOp getCompareOp() {
        return compareOp;
    }

    /**
     * Only {@link CompareOp#EQUAL} and {@link CompareOp#NOT_EQUAL} are supported.
     */
    public void setCompareOp(CompareOp compareOp) {
        this.compareOp = compareOp;
    }

    /**
     * Set whether entire record should be filtered if the field is not found.
     * 
     * <p>If true, the entire record will be skipped if the field is not found. This is the default.
     * 
     * <p>If false, the record will pass if the field is not found.
     */
    public void setFilterIfMissing(boolean filterIfMissing) {
        this.filterIfMissing = filterIfMissing;
    }

    /**
     * Get whether entire record should be filtered if the field is not found.
     * 
     * @return true if record should be skipped if field not found, false if record
     *         should be let through anyways
     */
    public boolean getFilterIfMissing() {
        return filterIfMissing;
    }
}
