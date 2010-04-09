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

import java.util.HashMap;
import java.util.Map;

import org.lilycms.repository.api.RecordType;
import org.lilycms.repository.api.TypeManager;
import org.lilycms.repository.api.Record.Scope;

public class RecordTypeImpl implements RecordType {
    
    private final String id;
    private Long version;
    private Map<Scope, String> fieldGroupIds;
    private Map<Scope, Long> fieldGroupVersions;
    private Map<String, Long> mixins;

    /**
     * This constructor should not be called directly.
     * @use {@link TypeManager#newRecordType} instead
     */
    public RecordTypeImpl(String id) {
        this.id = id;
        fieldGroupIds = new HashMap<Scope, String>();
        fieldGroupIds.put(Scope.NON_VERSIONABLE, null);
        fieldGroupIds.put(Scope.VERSIONABLE, null);
        fieldGroupIds.put(Scope.VERSIONABLE_MUTABLE, null);
        fieldGroupVersions = new HashMap<Scope, Long>();
        fieldGroupVersions.put(Scope.NON_VERSIONABLE, null);
        fieldGroupVersions.put(Scope.VERSIONABLE, null);
        fieldGroupVersions.put(Scope.VERSIONABLE_MUTABLE, null);
        mixins = new HashMap<String, Long>();
    }
    
    public String getId() {
        return id;
    }

    public Long getVersion() {
        return version;
    }
    
    public void setVersion(Long version){
        this.version = version;
    }

    public String getFieldGroupId(Scope scope) {
        return fieldGroupIds.get(scope);
    }

    public Long getFieldGroupVersion(Scope scope) {
        return fieldGroupVersions.get(scope);
    }

    public void setFieldGroupId(Scope scope, String id) {
        fieldGroupIds.put(scope, id);
    }

    public void setFieldGroupVersion(Scope scope, Long version) {
        fieldGroupVersions.put(scope, version);
    }
    
    public void addMixin(String recordTypeId, Long recordTypeVersion) {
        mixins.put(recordTypeId, recordTypeVersion);
    }
    
    public void removeMixin(String recordTypeId) {
        mixins.remove(recordTypeId);
    }
    
    public Map<String, Long> getMixins() {
        return mixins;
    }

    public RecordType clone() {
        RecordTypeImpl clone = new RecordTypeImpl(this.id);
        clone.version = this.version;
        clone.fieldGroupIds.putAll(fieldGroupIds);
        clone.fieldGroupVersions.putAll(fieldGroupVersions);
        clone.mixins.putAll(mixins);
        return clone;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((fieldGroupIds == null) ? 0 : fieldGroupIds.hashCode());
        result = prime * result + ((fieldGroupVersions == null) ? 0 : fieldGroupVersions.hashCode());
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((mixins == null) ? 0 : mixins.hashCode());
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
        RecordTypeImpl other = (RecordTypeImpl) obj;
        if (fieldGroupIds == null) {
            if (other.fieldGroupIds != null)
                return false;
        } else if (!fieldGroupIds.equals(other.fieldGroupIds))
            return false;
        if (fieldGroupVersions == null) {
            if (other.fieldGroupVersions != null)
                return false;
        } else if (!fieldGroupVersions.equals(other.fieldGroupVersions))
            return false;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        if (mixins == null) {
            if (other.mixins != null)
                return false;
        } else if (!mixins.equals(other.mixins))
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
        return "RecordTypeImpl [id=" + id + ", version=" + version + ", fieldGroupIds=" + fieldGroupIds
                        + ", fieldGroupVersions=" + fieldGroupVersions + ", mixins=" + mixins + "]";
    }

}
