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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.lilycms.repository.api.FieldNotFoundException;
import org.lilycms.repository.api.Record;
import org.lilycms.repository.api.RecordId;
import org.lilycms.repository.api.Repository;

public class RecordImpl implements Record {
    private RecordId id;
    private Map<Scope, Map<String, Object>> fields = new HashMap<Scope, Map<String, Object>>();
    private String recordTypeId;
    private Long recordTypeVersion;
    private Map<Scope, String> recordTypeIds = new HashMap<Scope, String>();
    private Map<Scope, Long> recordTypeVersions = new HashMap<Scope, Long>();
    private Long version;
    private Map<Scope, List<String>> fieldsToDelete = new HashMap<Scope, List<String>>();

    /**
     * This constructor should not be called directly.
     * @use {@link Repository#newRecord} instead
     */
    public RecordImpl() {
        initialize();
    }

    /**
     * This constructor should not be called directly.
     * @use {@link Repository#newRecord} instead
     */
    public RecordImpl(RecordId id) {
        this.id = id;
        initialize();
    }

    private void initialize() {
        for (Scope scope : Scope.values()) {
            fields.put(scope, new HashMap<String, Object>());
            fieldsToDelete.put(scope, new ArrayList<String>());
        }
    }

    public void setId(RecordId id) {
        this.id = id;
    }
    
    public RecordId getId() {
        return id;
    }
    
    public void setVersion(Long version) {
        this.version = version;
    }
    
    public Long getVersion() {
        return version;
    }

    public void setRecordType(String recordTypeId, Long recordTypeVersion) {
        this.recordTypeId = recordTypeId;
        this.recordTypeVersion = recordTypeVersion;
    }

    public String getRecordTypeId() {
        return recordTypeId;
    }

    public Long getRecordTypeVersion() {
        return recordTypeVersion;
    }
    
    public void setRecordType(Scope scope, String id, Long version) {
        recordTypeIds.put(scope, id);
        recordTypeVersions.put(scope, version);
    }
    
    public String getRecordTypeId(Scope scope) {
        return recordTypeIds.get(scope);
    }
    
    public Long getRecordTypeVersion(Scope scope) {
        return recordTypeVersions.get(scope);
    }
    
    public void setField(Scope scope, String name, Object value) {
        fields.get(scope).put(name, value);
    }
    
    public Object getField(Scope scope, String name) throws FieldNotFoundException {
        Object field = fields.get(scope).get(name);
        if (field == null) {
            throw new FieldNotFoundException(name);
        }
        return field;
    }

    public Map<String, Object> getFields(Scope scope) {
        return fields.get(scope);
    }

    public List<String> getFieldsToDelete(Scope scope) {
        return fieldsToDelete.get(scope);
    }

    public void addFieldsToDelete(Scope scope, List<String> names) {
        fieldsToDelete.get(scope).addAll(names);
    }

    public void removeFieldsToDelete(Scope scope, List<String> names) {
        fieldsToDelete.get(scope).removeAll(names);
    }

    public Record clone() {
        RecordImpl record = new RecordImpl();
        record.id = id;
        record.version = version;
        record.recordTypeId = recordTypeId;
        record.recordTypeVersion = recordTypeVersion;
        record.recordTypeIds.putAll(recordTypeIds);
        record.recordTypeVersions.putAll(recordTypeVersions);
        for (Scope scope : Scope.values()) {
            record.fields.get(scope).putAll(fields.get(scope));
            record.fieldsToDelete.get(scope).addAll(fieldsToDelete.get(scope));
        }
        return record;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((fields == null) ? 0 : fields.hashCode());
        result = prime * result + ((fieldsToDelete == null) ? 0 : fieldsToDelete.hashCode());
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((recordTypeId == null) ? 0 : recordTypeId.hashCode());
        result = prime * result + ((recordTypeIds == null) ? 0 : recordTypeIds.hashCode());
        result = prime * result + ((recordTypeVersion == null) ? 0 : recordTypeVersion.hashCode());
        result = prime * result + ((recordTypeVersions == null) ? 0 : recordTypeVersions.hashCode());
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
        if (fields == null) {
            if (other.fields != null)
                return false;
        } else if (!fields.equals(other.fields))
            return false;
        if (fieldsToDelete == null) {
            if (other.fieldsToDelete != null)
                return false;
        } else if (!fieldsToDelete.equals(other.fieldsToDelete))
            return false;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        if (recordTypeId == null) {
            if (other.recordTypeId != null)
                return false;
        } else if (!recordTypeId.equals(other.recordTypeId))
            return false;
        if (recordTypeIds == null) {
            if (other.recordTypeIds != null)
                return false;
        } else if (!recordTypeIds.equals(other.recordTypeIds))
            return false;
        if (recordTypeVersion == null) {
            if (other.recordTypeVersion != null)
                return false;
        } else if (!recordTypeVersion.equals(other.recordTypeVersion))
            return false;
        if (recordTypeVersions == null) {
            if (other.recordTypeVersions != null)
                return false;
        } else if (!recordTypeVersions.equals(other.recordTypeVersions))
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
        return "RecordImpl [id=" + id + ", version=" + version + ", recordTypeId=" + recordTypeId
                        + ", recordTypeVersion=" + recordTypeVersion + ", recordTypeIds=" + recordTypeIds
                        + ", recordTypeVersions=" + recordTypeVersions + ", fields=" + fields + ", fieldsToDelete="
                        + fieldsToDelete + "]";
    }
}