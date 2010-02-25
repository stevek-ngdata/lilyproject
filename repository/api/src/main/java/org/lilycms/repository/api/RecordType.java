package org.lilycms.repository.api;

public interface RecordType {
    String getName();
    long getVersion();
    FieldDescriptor getFieldDescriptor(String name);
}
