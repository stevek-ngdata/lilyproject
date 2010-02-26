package org.lilycms.repository.api;

import java.util.Collection;

public interface RecordType {
    String getRecordTypeId();
    long getVersion();
    void addFieldDescriptor(FieldDescriptor fieldDescriptor);
    void removeFieldDescriptor(String removeFieldDescriptorId);
    FieldDescriptor getFieldDescriptor(String fieldDescriptorId);
    Collection<FieldDescriptor> getFieldDescriptors();
}
