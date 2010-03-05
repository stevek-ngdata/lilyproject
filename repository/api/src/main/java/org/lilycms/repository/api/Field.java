package org.lilycms.repository.api;

public interface Field {
    String getFieldId();
    void setFieldId(String fieldId);
    byte[] getValue();
    void setValue(byte[] value);
}

