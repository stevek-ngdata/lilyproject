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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.lilycms.repository.api.FieldNotFoundException;
import org.lilycms.repository.api.Record;
import org.lilycms.repository.api.RecordId;
import org.lilycms.repository.api.Repository;

public class RecordImpl implements Record {
    private RecordId recordId;
    private Map<String, Object> fields = new HashMap<String, Object>();
    private Set<String> deleteFields = new HashSet<String>();
    private String recordTypeId;
    private long recordTypeVersion;
    private Map<String, String> variantProperties = new HashMap<String, String>();
    private Long version;

    /**
     * This constructor should not be called directly.
     * @use {@link Repository#newRecord} instead
     */
    public RecordImpl() {
    }
    
    /**
     * This constructor should not be called directly.
     * @use {@link Repository#newRecord} instead
     */
    public RecordImpl(RecordId recordId) {
        this.recordId = recordId;
    }
    
    public void setId(RecordId recordId) {
        this.recordId = recordId;
    }
    
    public RecordId getId() {
        return recordId;
    }
    
    public void setVersion(Long version) {
        this.version = version;
    }
    
    public Long getVersion() {
        return version;
    }

    public void setRecordType(String recordTypeId, long recordTypeVersion) {
        this.recordTypeId = recordTypeId;
        this.recordTypeVersion = recordTypeVersion;
    }

    public String getRecordTypeId() {
        return recordTypeId;
    }

    public long getRecordTypeVersion() {
        return recordTypeVersion;
    }

    public void setField(String fieldId, Object value) {
        fields.put(fieldId, value);
    }

    public Object getField(String fieldId) throws FieldNotFoundException {
        Object field = fields.get(fieldId);
        if (field == null) {
            throw new FieldNotFoundException(fieldId);
        }
        return field;
    }

    public Map<String, Object> getFields() {
        return fields;
    }

    public void addVariantProperty(String dimension, String dimensionValue) {
        variantProperties.put(dimension, dimensionValue);
    }

    public void addVariantProperties(Map<String, String> variantProperties) {
        if (variantProperties != null) {
            this.variantProperties.putAll(variantProperties);
        }
    }

    public Map<String, String> getVariantProperties() {
        return variantProperties;
    }

    public void deleteField(String fieldId) {
        deleteFields.add(fieldId);
    }

    public Set<String> getDeleteFields() {
        return deleteFields;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((deleteFields == null) ? 0 : deleteFields.hashCode());
        result = prime * result + ((fields == null) ? 0 : fields.hashCode());
        result = prime * result + ((recordId == null) ? 0 : recordId.hashCode());
        result = prime * result + ((recordTypeId == null) ? 0 : recordTypeId.hashCode());
        result = prime * result + (int) (recordTypeVersion ^ (recordTypeVersion >>> 32));
        result = prime * result + ((variantProperties == null) ? 0 : variantProperties.hashCode());
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
        RecordImpl other = (RecordImpl) obj;
        if (deleteFields == null) {
            if (other.deleteFields != null)
                return false;
        } else if (!deleteFields.equals(other.deleteFields))
            return false;
        if (fields == null) {
            if (other.fields != null)
                return false;
        } else if (!fields.equals(other.fields))
            return false;
        if (recordId == null) {
            if (other.recordId != null)
                return false;
        } else if (!recordId.equals(other.recordId))
            return false;
        if (recordTypeId == null) {
            if (other.recordTypeId != null)
                return false;
        } else if (!recordTypeId.equals(other.recordTypeId))
            return false;
        if (recordTypeVersion != other.recordTypeVersion)
            return false;
        if (variantProperties == null) {
            if (other.variantProperties != null)
                return false;
        } else if (!variantProperties.equals(other.variantProperties))
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
        return "RecordImpl [recordId=" + recordId + ", version=" + version + ", recordTypeId=" + recordTypeId
                        + ", recordTypeVersion=" + recordTypeVersion + ", variantProperties=" + variantProperties
                        + ", fields=" + fields + ", deleteFields=" + deleteFields + "]";
    }

    

}