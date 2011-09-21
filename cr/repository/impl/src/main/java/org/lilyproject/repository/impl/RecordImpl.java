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

import java.util.*;
import java.util.Map.Entry;

import org.lilyproject.repository.api.*;
import org.lilyproject.util.ObjectUtils;

public class RecordImpl implements Record {
    private RecordId id;
    private Map<QName, Object> fields = new HashMap<QName, Object>();
    private List<QName> fieldsToDelete = new ArrayList<QName>(0); // default size zero because this is used relatively
                                                                  // rarely compared to fields added/updated.
    private Map<Scope, RecordTypeRef> recordTypes = new EnumMap<Scope, RecordTypeRef>(Scope.class);
    private Long version;
    private ResponseStatus responseStatus;
    
    private String defaultNamespace = null;
    
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
        if (name == null && version == null) {
            recordTypes.remove(scope);
        } else {
            recordTypes.put(scope, new RecordTypeRef(name, version));
        }
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
    
    public <T> T getField(QName name) throws FieldNotFoundException {
        Object field = fields.get(name);
        if (field == null) {
            throw new FieldNotFoundException(name);
        }
        return (T)field;
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
        if (!names.isEmpty()) {
            fieldsToDelete.addAll(names);
        }
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
        for (Entry<QName, Object> entry : fields.entrySet()) {
            record.fields.put(entry.getKey(), cloneValue(entry.getValue(), record)); // Deep clone of the values
        }
        if (fieldsToDelete.size() > 0) { // addAll seems expensive even when list is empty
            record.fieldsToDelete.addAll(fieldsToDelete);
        }
        // the ResponseStatus is not cloned, on purpose
        return record;
    }
    
    private Object cloneValue(Object value, Record clone) {
        if (value instanceof List) {
            List<Object> newList = new ArrayList<Object>();
            List<Object> values = (List<Object>)value;
            for (Object object : values) {
                newList.add(cloneValue(object, clone));
            }
            return newList;
        }
        if (value instanceof HierarchyPath) {
            Object[] elements = ((HierarchyPath)value).getElements();
            Object[] newElements = new Object[elements.length];
            for (int i = 0; i < newElements.length; i++) {
                newElements[i] = cloneValue(elements[i], clone);
            }
            return new HierarchyPath(newElements);
        }
        if (value instanceof Blob) {
            return ((Blob)value).clone();
        }
        if (value instanceof Record) {
            Record record = (Record)value;
            if (record == this)
                return clone; // Avoid recursion
            return (record).clone();
        }
        return value; // All other values are immutable
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

        if (obj instanceof RecordRvtImpl) {
            return equals(((RecordRvtImpl)obj).getRecord());
        }

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
        if (obj instanceof RecordRvtImpl) {
            return softEquals(((RecordRvtImpl)obj).getRecord());
        }
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
    
    public void setDefaultNamespace(String namespace) {
        this.defaultNamespace = namespace;
    }

    private QName resolveNamespace(String name) throws RecordException {
        if (defaultNamespace != null)
            return new QName(defaultNamespace, name);
        QName recordTypeName = getRecordTypeName();
        if (recordTypeName != null)
            return new QName(recordTypeName.getNamespace(), name);
        else throw new RecordException("Namespace could not be resolved for name '" + name + "'since no default namespace was given and no record type is set");
    }
    
    public void setRecordType(String recordTypeName) throws RecordException {
        setRecordType(resolveNamespace(recordTypeName));
    }
    
    public void setRecordType(String recordTypeName, Long version) throws RecordException {
        setRecordType(resolveNamespace(recordTypeName), version);
    }
    
    public void setRecordType(Scope scope, String recordTypeName, Long version) throws RecordException {
        setRecordType(scope, resolveNamespace(recordTypeName), version);
    }
    
    public <T> T getField(String fieldName) throws FieldNotFoundException, RecordException {
        // The cast to (T) is only needed for a bug in JDK's < 1.6u24
        return (T)getField(resolveNamespace(fieldName));
    }
    
    public void setField(String fieldName, Object value) throws RecordException {
        setField(resolveNamespace(fieldName), value);
    }
    
    public void delete(String fieldName, boolean addFieldsToDelete) throws RecordException {
        delete(resolveNamespace(fieldName), addFieldsToDelete);
    }
    
    public boolean hasField(String fieldName) throws RecordException {
        return hasField(resolveNamespace(fieldName));
    }
}