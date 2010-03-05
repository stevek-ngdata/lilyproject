package org.lilycms.repository.impl;

import java.util.Arrays;

import org.apache.hadoop.hbase.util.Bytes;
import org.lilycms.repository.api.Field;

public class FieldImpl implements Field {
    private String fieldId;
    private byte[] value;

    public FieldImpl(String fieldId, byte[] value) {
        this.fieldId = fieldId;
        this.value = value;
    }

    public String getFieldId() {
        return fieldId;
    }
    
    public void setFieldId(String fieldId) {
        this.fieldId = fieldId;
    }

    public byte[] getValue() {
        return value;
    }
    
    public void setValue(byte[] value) {
        this.value = value;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((fieldId == null) ? 0 : fieldId.hashCode());
        result = prime * result + Arrays.hashCode(value);
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
        FieldImpl other = (FieldImpl) obj;
        if (fieldId == null) {
            if (other.fieldId != null)
                return false;
        } else if (!fieldId.equals(other.fieldId))
            return false;
        if (!Arrays.equals(value, other.value))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "["+fieldId+","+ Bytes.toString(value)+"]";
    }
}
