package org.lilycms.repository.impl;

import org.lilycms.repository.api.FieldDescriptor;

public class FieldDescriptorImpl implements FieldDescriptor {

    private final String fieldDescriptorId;
    private long version;
    private final boolean mandatory;
    private final boolean versionable;
    private final String fieldType;

    public FieldDescriptorImpl(String fieldDescriptorId, String fieldType, boolean mandatory, boolean versionable) {
        this(fieldDescriptorId, 0, fieldType, mandatory, versionable);
    }
    
    public FieldDescriptorImpl(String fieldDescriptorId, long version, String fieldType, boolean mandatory, boolean versionable) {
        this.fieldDescriptorId = fieldDescriptorId;
        this.version = version;
        this.fieldType = fieldType;
        this.mandatory = mandatory;
        this.versionable = versionable;
    }

    public String getFieldType() {
        return fieldType;
    }

    public String getFieldDescriptorId() {
        return fieldDescriptorId;
    }

    public long getVersion() {
        return version;
    }
    
    public void setVersion(long version) {
        this.version = version;
    }

    public boolean isMandatory() {
        return mandatory;
    }

    public boolean isVersionable() {
        return versionable;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((fieldDescriptorId == null) ? 0 : fieldDescriptorId.hashCode());
        result = prime * result + ((fieldType == null) ? 0 : fieldType.hashCode());
        result = prime * result + (mandatory ? 1231 : 1237);
        result = prime * result + (int) (version ^ (version >>> 32));
        result = prime * result + (versionable ? 1231 : 1237);
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
        FieldDescriptorImpl other = (FieldDescriptorImpl) obj;
        if (fieldDescriptorId == null) {
            if (other.fieldDescriptorId != null)
                return false;
        } else if (!fieldDescriptorId.equals(other.fieldDescriptorId))
            return false;
        if (fieldType == null) {
            if (other.fieldType != null)
                return false;
        } else if (!fieldType.equals(other.fieldType))
            return false;
        if (mandatory != other.mandatory)
            return false;
        if (version != other.version)
            return false;
        if (versionable != other.versionable)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "FieldDescriptorImpl [fieldDescriptorId=" + fieldDescriptorId + ", version=" + version + ", fieldType="
                        + fieldType + ", mandatory=" + mandatory + ", versionable=" + versionable + "]";
    }

    
}
