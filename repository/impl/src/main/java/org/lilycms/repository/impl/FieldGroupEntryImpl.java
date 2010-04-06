/*
 * Copyright 2010 Outerthought bvba
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lilycms.repository.impl;

import org.lilycms.repository.api.FieldDescriptor;
import org.lilycms.repository.api.FieldGroupEntry;

/**
 *
 */
public class FieldGroupEntryImpl implements FieldGroupEntry {

    private String fieldDescriptorId;
    private Long fieldDescriptorVersion;
    private boolean mandatory;
    private String alias;

    public FieldGroupEntryImpl(String fieldDescriptorId, Long fieldDescriptorVersion, boolean mandatory, String alias) {
        this.fieldDescriptorId = fieldDescriptorId;
        this.fieldDescriptorVersion = fieldDescriptorVersion;
        this.mandatory = mandatory;
        this.alias = alias;
    }

    public String getFieldDescriptorId() {
        return fieldDescriptorId;
    }
    
    public Long getFieldDescriptorVersion() {
        return fieldDescriptorVersion;
    }
    
    public String getAlias() {
        return alias;
    }

    public boolean isMandatory() {
        return mandatory;
    }

    public void setFieldDescriptorId(String id) {
        this.fieldDescriptorId = id;
    }
    
    public void setFieldDescriptorVersion(Long version) {
        this.fieldDescriptorVersion = version;
    }
    
    public void setAlias(String alias) {
        this.alias = alias;
    }
    
    public void setMandatory(boolean mandatory) {
        this.mandatory = mandatory;
    }
    
    public FieldGroupEntry clone() {
        return new FieldGroupEntryImpl(fieldDescriptorId, fieldDescriptorVersion, mandatory, alias);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((alias == null) ? 0 : alias.hashCode());
        result = prime * result + ((fieldDescriptorId == null) ? 0 : fieldDescriptorId.hashCode());
        result = prime * result + ((fieldDescriptorVersion == null) ? 0 : fieldDescriptorVersion.hashCode());
        result = prime * result + (mandatory ? 1231 : 1237);
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
        FieldGroupEntryImpl other = (FieldGroupEntryImpl) obj;
        if (alias == null) {
            if (other.alias != null)
                return false;
        } else if (!alias.equals(other.alias))
            return false;
        if (fieldDescriptorId == null) {
            if (other.fieldDescriptorId != null)
                return false;
        } else if (!fieldDescriptorId.equals(other.fieldDescriptorId))
            return false;
        if (fieldDescriptorVersion == null) {
            if (other.fieldDescriptorVersion != null)
                return false;
        } else if (!fieldDescriptorVersion.equals(other.fieldDescriptorVersion))
            return false;
        if (mandatory != other.mandatory)
            return false;
        return true;
    }
}
