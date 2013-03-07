/*
 * Copyright 2012 NGDATA nv
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lilyproject.repository.api.filter;

import org.lilyproject.repository.api.CompareOp;
import org.lilyproject.repository.api.QName;

/**
 * Filters based on the value of a record field.
 *
 * <p>Only equals and not-equals comparisons are possible. This is because the comparison happens inside
 * the HBase region servers, on the bytes-encoded field values.</p>
 *
 * <p>For versioned fields, the filtering always happens based on the last version of the field values.</p>
 */
public class FieldValueFilter implements RecordFilter {
    private QName field;
    private Object fieldValue;
    private CompareOp compareOp = CompareOp.EQUAL;
    private boolean filterIfMissing = true;

    public FieldValueFilter() {
    }

    /**
     * Constructs a filter which checks that the specified field is equal
     * to the specified value.
     */
    public FieldValueFilter(QName field, Object fieldValue) {
        this.field = field;
        this.fieldValue = fieldValue;
    }

    /**
     * Constructs a filter comparing the specified field with the specified value,
     * using the specified comparison operator. Only {@link CompareOp#EQUAL} and
     * {@link CompareOp#NOT_EQUAL} are supported.
     */
    public FieldValueFilter(QName field, CompareOp compareOp, Object fieldValue) {
        this.field = field;
        this.compareOp = compareOp;
        this.fieldValue = fieldValue;
    }

    /**
     * Gets the field whose value is to be compared.
     */
    public QName getField() {
        return field;
    }

    /**
     * Sets the field whose value is to be compared.
     */
    public void setField(QName field) {
        this.field = field;
    }

    /**
     * @see #setFieldValue(Object)
     */
    public Object getFieldValue() {
        return fieldValue;
    }

    /**
     * Sets the value of the field.
     *
     * <p>The provided object should be of the correct type, corresponding
     * to the value type of the field ({@link #setField(QName)}). The expected
     * Java classes for each Lily field type can be found at
     * {@link org.lilyproject.repository.api.TypeManager#getValueType(String)}
     */
    public void setFieldValue(Object fieldValue) {
        this.fieldValue = fieldValue;
    }

    /**
     * @see #setCompareOp(CompareOp)
     */
    public CompareOp getCompareOp() {
        return compareOp;
    }

    /**
     * Sets the comparison operator, only {@link CompareOp#EQUAL} and
     * {@link CompareOp#NOT_EQUAL} are supported.
     */
    public void setCompareOp(CompareOp compareOp) {
        this.compareOp = compareOp;
    }

    /**
     * Set whether the record should be filtered if the record does not have the field.
     *
     * <p>If true, the record will be skipped if the field is not found. This is the default.
     *
     * <p>If false, the record will pass if the field is not found.
     */
    public void setFilterIfMissing(boolean filterIfMissing) {
        this.filterIfMissing = filterIfMissing;
    }

    /**
     * Get whether the record should be filtered if the record does not have the field.
     *
     * @return true if record should be skipped if field not found, false if record
     *         should be let through anyways
     */
    public boolean getFilterIfMissing() {
        return filterIfMissing;
    }
}
