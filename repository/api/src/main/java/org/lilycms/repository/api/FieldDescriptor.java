package org.lilycms.repository.api;

public interface FieldDescriptor {
    String getFieldDescriptorId();
    long getVersion();
    String getFieldType();
    boolean isMandatory();
    boolean isVersionable();
}
