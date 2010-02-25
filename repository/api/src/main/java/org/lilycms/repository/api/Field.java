package org.lilycms.repository.api;

public interface Field {
    String getName();
    void setName(String name);
    byte[] getValue();
    void setValue(byte[] value);
}

