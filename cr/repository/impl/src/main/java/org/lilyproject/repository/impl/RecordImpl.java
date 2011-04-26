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
package org.lilyproject.repository.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.lilyproject.repository.api.*;
import org.lilyproject.util.ObjectUtils;

public class RecordImpl implements Record {
    private RecordId id;
    private Map<QName, Object> fields = new HashMap<QName, Object>();
    private List<QName> fieldsToDelete = new ArrayList<QName>();
    private Map<Scope, RecordTypeRef> recordTypes = new HashMap<Scope, RecordTypeRef>();
    private Long version;
    private ResponseStatus responseStatus;
    

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

    public void setRecordType(QName name, Long version) {
        setRecordType(Scope.NON_VERSIONED, name, version);
    }
    
    public void setRecordType(QName name) {
        setRecordType(name, null);
    }
    
    public QName getRecordTypeName() {
        return getRecordTypeName(Scope.NON_VERSIONED);
    }

    public Long getRecordTypeVersion() {
        return getRecordTypeVersion(Scope.NON_VERSIONED);
    }
    
    public void setRecordType(Scope scope, QName name, Long version) {
        recordTypes.put(scope, new RecordTypeRef(name, version));
    }
    
    public QName getRecordTypeName(Scope scope) {
        RecordTypeRef ref = recordTypes.get(scope);
        return ref != null ? ref.name : null;
    }
    
    public Long getRecordTypeVersion(Scope scope) {
        RecordTypeRef ref = recordTypes.get(scope);
        return ref != null ? ref.version : null;
    }
    
    public void setField(QName name, Object value) {
        fields.put(name, value);
        fieldsToDelete.remove(name);
    }
    
    public Object getField(QName name) throws FieldNotFoundException {
        Object field = fields.get(name);
        if (field == null) {
            throw new FieldNotFoundException(name);
        }
        return field;
    }

    public boolean hasField(QName fieldName) {
        return fields.containsKey(fieldName);
    }

    public Map<QName, Object> getFields() {
        return fields;
    }

    public void delete(QName fieldName, boolean addToFieldsToDelete) {
        fields.remove(fieldName);

        if (addToFieldsToDelete) {
            getFieldsToDelete().add(fieldName);
        }
    }

    public List<QName> getFieldsToDelete() {
        return fieldsToDelete;
    }

    public void addFieldsToDelete(List<QName> names) {
        fieldsToDelete.addAll(names);
    }

    public void removeFieldsToDelete(List<QName> names) {
        fieldsToDelete.removeAll(names);
    }

    public ResponseStatus getResponseStatus() {
        return responseStatus;
    }

    public void setResponseStatus(ResponseStatus status) {
        this.responseStatus = status;
    }

    public Record clone() {
        RecordImpl record = new RecordImpl();
        record.id = id;
        record.version = version;
        record.recordTypes.putAll(recordTypes);
        record.fields.putAll(fields);
        record.fieldsToDelete.addAll(fieldsToDelete);
        // the ResponseStatus is not cloned, on purpose
        return record;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((fields == null) ? 0 : fields.hashCode());
        result = prime * result + ((fieldsToDelete == null) ? 0 : fieldsToDelete.hashCode());
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((recordTypes == null) ? 0 : recordTypes.hashCode());
        result = prime * result + ((version == null) ? 0 : version.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (!softEquals(obj))
            return false;

        RecordImpl other = (RecordImpl) obj;

        if (recordTypes == null) {
            if (other.recordTypes != null)
                return false;
        } else if (!recordTypes.equals(other.recordTypes)) {
            return false;
        }

        if (version == null) {
            if (other.version != null)
                return false;
        } else if (!version.equals(other.version)) {
            return false;
        }

        return true;
    }

    public boolean softEquals(Object obj) {
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
        } else if (!fields.equals(other.fields)) {
            return false;
        }

        if (fieldsToDelete == null) {
            if (other.fieldsToDelete != null)
                return false;
        } else if (!fieldsToDelete.equals(other.fieldsToDelete)) {
            return false;
        }

        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id)) {
            return false;
        }

        QName nonVersionedRT1 = getRecordTypeName(Scope.NON_VERSIONED);
        QName nonVersionedRT2 = other.getRecordTypeName(Scope.NON_VERSIONED);

        if (nonVersionedRT1 != null && nonVersionedRT2 != null && !nonVersionedRT1.equals(nonVersionedRT2)) {
            return false;
        }

        return true;
    }

    @Override
    public String toString() {
        return "RecordImpl [id=" + id + ", version=" + version + ", recordTypes=" + recordTypes
                        + ", fields=" + fields + ", fieldsToDelete="
                        + fieldsToDelete + "]";
    }

    private static final class RecordTypeRef {
        // This object is immutable on purpose (see record clone)
        final QName name;
        final Long version;

        public RecordTypeRef(QName name, Long version) {
            this.name = name;
            this.version = version;
        }

        @Override
        public String toString() {
            return name + ":" + version;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((name == null) ? 0 : name.hashCode());
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
            RecordTypeRef other = (RecordTypeRef) obj;
            return ObjectUtils.safeEquals(name, other.name) && ObjectUtils.safeEquals(version, other.version);
        }
    }
}