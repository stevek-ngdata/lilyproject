package org.lilycms.repository.api;

public interface FieldDescriptor {
    String getName();
    long getVersion();
    String getFieldType();
    boolean isMandatory();
    boolean isVersionable();
}
