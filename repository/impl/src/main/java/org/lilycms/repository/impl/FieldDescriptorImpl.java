package org.lilycms.repository.impl;

import org.lilycms.repository.api.FieldDescriptor;

public class FieldDescriptorImpl implements FieldDescriptor {

    private final String name;
    private final long version;
    private final boolean mandatory;
    private final boolean versionable;
    private final String fieldType;

    public FieldDescriptorImpl(String name, long version, String fieldType, boolean mandatory, boolean versionable) {
        this.name = name;
        this.version = version;
        this.fieldType = fieldType;
        this.mandatory = mandatory;
        this.versionable = versionable;
    }

    public String getFieldType() {
        return fieldType;
    }

    public String getName() {
        return name;
    }

    public long getVersion() {
        return version;
    }

    public boolean isMandatory() {
        return mandatory;
    }

    public boolean isVersionable() {
        return versionable;
    }
}
