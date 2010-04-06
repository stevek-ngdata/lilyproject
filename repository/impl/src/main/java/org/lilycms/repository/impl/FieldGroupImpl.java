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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.lilycms.repository.api.FieldGroup;
import org.lilycms.repository.api.FieldGroupEntry;

/**
 *
 */
public class FieldGroupImpl implements FieldGroup {

    private final String id;
    private Long version;
    private Map<String, FieldGroupEntry> fieldGroupEntries;

    public FieldGroupImpl(String id) {
        this.id = id;
        fieldGroupEntries = new HashMap<String, FieldGroupEntry>();
    }
    
    public void setVersion(Long version) {
        this.version = version;
    }
    
    public Long getVersion() {
        return this.version;
    }
    
    public String getId() {
        return id;
    }

    public Collection<FieldGroupEntry> getFieldGroupEntries() {
        return fieldGroupEntries.values();
    }

    public FieldGroupEntry getFieldGroupEntry(String fieldDescriptorId) {
        return fieldGroupEntries.get(fieldDescriptorId);
    }

    public void setFieldGroupEntry(FieldGroupEntry fieldGroupEntry) {
        fieldGroupEntries.put(fieldGroupEntry.getFieldDescriptorId(), fieldGroupEntry);
    }

    public void removeFieldGroupEntry(String fieldDescriptorId) {
        fieldGroupEntries.remove(fieldDescriptorId);
    }
    
    public FieldGroup clone() {
        FieldGroup fieldGroup = new FieldGroupImpl(this.id);
        for (FieldGroupEntry fieldGroupEntry : getFieldGroupEntries()) {
            fieldGroup.setFieldGroupEntry(fieldGroupEntry);
        }
        return fieldGroup;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((fieldGroupEntries == null) ? 0 : fieldGroupEntries.hashCode());
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((version == null) ? 0 : version.hashCode());
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
        FieldGroupImpl other = (FieldGroupImpl) obj;
        if (fieldGroupEntries == null) {
            if (other.fieldGroupEntries != null)
                return false;
        } else if (!fieldGroupEntries.equals(other.fieldGroupEntries))
            return false;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        if (version == null) {
            if (other.version != null)
                return false;
        } else if (!version.equals(other.version))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "FieldGroupImpl [id=" + id + ", version=" + version + ", fieldGroupEntries=" + fieldGroupEntries + "]";
    }
}
