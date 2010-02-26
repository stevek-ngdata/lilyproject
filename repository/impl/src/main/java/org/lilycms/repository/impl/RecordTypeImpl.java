package org.lilycms.repository.impl;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.lilycms.repository.api.FieldDescriptor;
import org.lilycms.repository.api.RecordType;

public class RecordTypeImpl implements RecordType {
    
    private final String recordTypeId;
    private Map<String, FieldDescriptor> fieldDescriptors = new HashMap<String, FieldDescriptor>();
    private long version;

    public RecordTypeImpl(String recordTypeId) {
        this.recordTypeId = recordTypeId;
        
    }
    
    public void addFieldDescriptor(FieldDescriptor fieldDescriptor) {
        fieldDescriptors.put(fieldDescriptor.getFieldDescriptorId(), fieldDescriptor);
    }
    
    public void removeFieldDescriptor(String fieldDescriptorId) {
        fieldDescriptors.remove(fieldDescriptorId);
    }

    public FieldDescriptor getFieldDescriptor(String fieldDescriptorId) {
        return fieldDescriptors.get(fieldDescriptorId);
    }
    
    public Collection<FieldDescriptor> getFieldDescriptors() {
        return fieldDescriptors.values();
    }

    public String getRecordTypeId() {
        return recordTypeId;
    }

    public long getVersion() {
        return version;
    }
    
    public void setVersion(long version){
        this.version = version;
    }
}
