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
    private Map<String, Object> nonVersionableFields = new HashMap<String, Object>();
    private Map<String, Object> versionableFields = new HashMap<String, Object>();
    private Map<String, Object> versionableMutableFields = new HashMap<String, Object>();
    private String recordTypeId;
    private Long recordTypeVersion;
    private String nonVersionableRecordTypeId;
    private Long nonVersionableRecordTypeVersion;
    private String versionableRecordTypeId;
    private Long versionableRecordTypeVersion;
    private String versionableMutableRecordTypeId;
    private Long versionableMutableRecordTypeVersion;
    private Long version;
    private List<String> nonVersionableFieldsToDelete = new ArrayList<String>();
    private List<String> versionableFieldsToDelete = new ArrayList<String>();
    private List<String> versionableMutableFieldsToDelete = new ArrayList<String>();

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
    public RecordImpl(RecordId id) {
        this.id = id;
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
    
    public void setNonVersionableRecordType(String id, Long version) {
        this.nonVersionableRecordTypeId = id;
        this.nonVersionableRecordTypeVersion = version;
    }
    
    public String getNonVersionableRecordTypeId() {
        return nonVersionableRecordTypeId;
    }
    
    public Long getNonVersionableRecordTypeVersion() {
        return nonVersionableRecordTypeVersion;
    }
    
    public void setVersionableRecordType(String id, Long version) {
        this.versionableRecordTypeId = id;
        this.versionableRecordTypeVersion = version;
    }
    
    public String getVersionableRecordTypeId() {
        return versionableRecordTypeId;
    }
    
    public Long getVersionableRecordTypeVersion() {
        return versionableRecordTypeVersion;
    }
    
    public void setVersionableMutableRecordType(String id, Long version) {
        this.versionableMutableRecordTypeId = id;
        this.versionableMutableRecordTypeVersion = version;
    }
    
    public String getVersionableMutableRecordTypeId() {
        return versionableMutableRecordTypeId;
    }

    public Long getVersionableMutableRecordTypeVersion() {
        return versionableMutableRecordTypeVersion;
    }
    
    public void setNonVersionableField(String fieldId, Object value) {
        this.nonVersionableFields.put(fieldId, value);
    }
    
    public void setVersionableField(String fieldId, Object value) {
        this.versionableFields.put(fieldId, value);
    }
    
    public void setVersionableMutableField(String fieldId, Object value) {
        this.versionableMutableFields.put(fieldId, value);
    }

    public Object getNonVersionableField(String fieldId) throws FieldNotFoundException {
        Object field = nonVersionableFields.get(fieldId);
        if (field == null) {
            throw new FieldNotFoundException(fieldId);
        }
        return field;
    }

    public Object getVersionableField(String fieldId) throws FieldNotFoundException {
        Object field = versionableFields.get(fieldId);
        if (field == null) {
            throw new FieldNotFoundException(fieldId);
        }
        return field;
    }

    public Object getVersionableMutableField(String fieldId) throws FieldNotFoundException {
        Object field = versionableMutableFields.get(fieldId);
        if (field == null) {
            throw new FieldNotFoundException(fieldId);
        }
        return field;
    }

    public Map<String, Object> getNonVersionableFields() {
        return nonVersionableFields;
    }

    public Map<String, Object> getVersionableFields() {
        return versionableFields;
    }

    public Map<String, Object> getVersionableMutableFields() {
        return versionableMutableFields;
    }
    
    public List<String> getNonVersionableFieldsToDelete() {
        return nonVersionableFieldsToDelete;
    }

    public List<String> getVersionableFieldsToDelete() {
        return versionableFieldsToDelete;
    }

    public List<String> getVersionableMutableFieldsToDelete() {
        return versionableMutableFieldsToDelete;
    }

    public void addNonVersionableFieldsToDelete(List<String> fieldIds) {
        this.nonVersionableFieldsToDelete .addAll(fieldIds);
    }

    public void addVersionableFieldsToDelete(List<String> fieldIds) {
        this.versionableFieldsToDelete.addAll(fieldIds);
    }

    public void addVersionableMutableFieldsToDelete(List<String> fieldIds) {
        this.versionableMutableFieldsToDelete.addAll(fieldIds);
    }
    
    public void removeNonVersionableFieldsToDelete(List<String> fieldIds) {
        this.nonVersionableFieldsToDelete .removeAll(fieldIds);
    }

    public void removeVersionableFieldsToDelete(List<String> fieldIds) {
        this.versionableFieldsToDelete.removeAll(fieldIds);
    }

    public void removeVersionableMutableFieldsToDelete(List<String> fieldIds) {
        this.versionableMutableFieldsToDelete.removeAll(fieldIds);
    }

    public Record clone() {
        RecordImpl record = new RecordImpl();
        record.id = id;
        record.version = version;
        record.recordTypeId = recordTypeId;
        record.recordTypeVersion = recordTypeVersion;
        record.nonVersionableRecordTypeId = nonVersionableRecordTypeId;
        record.nonVersionableRecordTypeVersion = nonVersionableRecordTypeVersion;
        record.versionableRecordTypeId = versionableRecordTypeId;
        record.versionableRecordTypeVersion = versionableRecordTypeVersion;
        record.versionableMutableRecordTypeId = versionableMutableRecordTypeId;
        record.versionableMutableRecordTypeVersion = versionableMutableRecordTypeVersion;
        record.nonVersionableFields.putAll(nonVersionableFields);
        record.versionableFields.putAll(versionableFields);
        record.versionableMutableFields.putAll(versionableMutableFields);
        record.nonVersionableFieldsToDelete.addAll(nonVersionableFieldsToDelete);
        record.versionableFieldsToDelete.addAll(versionableFieldsToDelete);
        record.versionableMutableFieldsToDelete.addAll(versionableMutableFieldsToDelete);
        return record;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((nonVersionableFields == null) ? 0 : nonVersionableFields.hashCode());
        result = prime * result
                        + ((nonVersionableFieldsToDelete == null) ? 0 : nonVersionableFieldsToDelete.hashCode());
        result = prime * result + ((nonVersionableRecordTypeId == null) ? 0 : nonVersionableRecordTypeId.hashCode());
        result = prime * result
                        + ((nonVersionableRecordTypeVersion == null) ? 0 : nonVersionableRecordTypeVersion.hashCode());
        result = prime * result + ((recordTypeId == null) ? 0 : recordTypeId.hashCode());
        result = prime * result + ((recordTypeVersion == null) ? 0 : recordTypeVersion.hashCode());
        result = prime * result + ((version == null) ? 0 : version.hashCode());
        result = prime * result + ((versionableFields == null) ? 0 : versionableFields.hashCode());
        result = prime * result + ((versionableFieldsToDelete == null) ? 0 : versionableFieldsToDelete.hashCode());
        result = prime * result + ((versionableMutableFields == null) ? 0 : versionableMutableFields.hashCode());
        result = prime
                        * result
                        + ((versionableMutableFieldsToDelete == null) ? 0 : versionableMutableFieldsToDelete.hashCode());
        result = prime * result
                        + ((versionableMutableRecordTypeId == null) ? 0 : versionableMutableRecordTypeId.hashCode());
        result = prime
                        * result
                        + ((versionableMutableRecordTypeVersion == null) ? 0 : versionableMutableRecordTypeVersion
                                        .hashCode());
        result = prime * result + ((versionableRecordTypeId == null) ? 0 : versionableRecordTypeId.hashCode());
        result = prime * result
                        + ((versionableRecordTypeVersion == null) ? 0 : versionableRecordTypeVersion.hashCode());
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
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        if (nonVersionableFields == null) {
            if (other.nonVersionableFields != null)
                return false;
        } else if (!nonVersionableFields.equals(other.nonVersionableFields))
            return false;
        if (nonVersionableFieldsToDelete == null) {
            if (other.nonVersionableFieldsToDelete != null)
                return false;
        } else if (!nonVersionableFieldsToDelete.equals(other.nonVersionableFieldsToDelete))
            return false;
        if (nonVersionableRecordTypeId == null) {
            if (other.nonVersionableRecordTypeId != null)
                return false;
        } else if (!nonVersionableRecordTypeId.equals(other.nonVersionableRecordTypeId))
            return false;
        if (nonVersionableRecordTypeVersion == null) {
            if (other.nonVersionableRecordTypeVersion != null)
                return false;
        } else if (!nonVersionableRecordTypeVersion.equals(other.nonVersionableRecordTypeVersion))
            return false;
        if (recordTypeId == null) {
            if (other.recordTypeId != null)
                return false;
        } else if (!recordTypeId.equals(other.recordTypeId))
            return false;
        if (recordTypeVersion == null) {
            if (other.recordTypeVersion != null)
                return false;
        } else if (!recordTypeVersion.equals(other.recordTypeVersion))
            return false;
        if (version == null) {
            if (other.version != null)
                return false;
        } else if (!version.equals(other.version))
            return false;
        if (versionableFields == null) {
            if (other.versionableFields != null)
                return false;
        } else if (!versionableFields.equals(other.versionableFields))
            return false;
        if (versionableFieldsToDelete == null) {
            if (other.versionableFieldsToDelete != null)
                return false;
        } else if (!versionableFieldsToDelete.equals(other.versionableFieldsToDelete))
            return false;
        if (versionableMutableFields == null) {
            if (other.versionableMutableFields != null)
                return false;
        } else if (!versionableMutableFields.equals(other.versionableMutableFields))
            return false;
        if (versionableMutableFieldsToDelete == null) {
            if (other.versionableMutableFieldsToDelete != null)
                return false;
        } else if (!versionableMutableFieldsToDelete.equals(other.versionableMutableFieldsToDelete))
            return false;
        if (versionableMutableRecordTypeId == null) {
            if (other.versionableMutableRecordTypeId != null)
                return false;
        } else if (!versionableMutableRecordTypeId.equals(other.versionableMutableRecordTypeId))
            return false;
        if (versionableMutableRecordTypeVersion == null) {
            if (other.versionableMutableRecordTypeVersion != null)
                return false;
        } else if (!versionableMutableRecordTypeVersion.equals(other.versionableMutableRecordTypeVersion))
            return false;
        if (versionableRecordTypeId == null) {
            if (other.versionableRecordTypeId != null)
                return false;
        } else if (!versionableRecordTypeId.equals(other.versionableRecordTypeId))
            return false;
        if (versionableRecordTypeVersion == null) {
            if (other.versionableRecordTypeVersion != null)
                return false;
        } else if (!versionableRecordTypeVersion.equals(other.versionableRecordTypeVersion))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "RecordImpl [id=" + id + ", version=" + version + ", recordTypeId=" + recordTypeId
                        + ", recordTypeVersion=" + recordTypeVersion + ", nonVersionableRecordTypeId="
                        + nonVersionableRecordTypeId + ", nonVersionableRecordTypeVersion="
                        + nonVersionableRecordTypeVersion + ", versionableMutableRecordTypeId="
                        + versionableMutableRecordTypeId + ", versionableMutableRecordTypeVersion="
                        + versionableMutableRecordTypeVersion + ", versionableRecordTypeId=" + versionableRecordTypeId
                        + ", versionableRecordTypeVersion=" + versionableRecordTypeVersion + ", nonVersionableFields="
                        + nonVersionableFields + ", nonVersionableFieldsToDelete=" + nonVersionableFieldsToDelete
                        + ", versionableFields=" + versionableFields + ", versionableFieldsToDelete="
                        + versionableFieldsToDelete + ", versionableMutableFields=" + versionableMutableFields
                        + ", versionableMutableFieldsToDelete=" + versionableMutableFieldsToDelete + "]";
    }
}