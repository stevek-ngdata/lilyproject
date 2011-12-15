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

    @Override
    public void setId(RecordId id) {
        this.id = id;
    }
    
    @Override
    public RecordId getId() {
        return id;
    }
    
    @Override
    public void setVersion(Long version) {
        this.version = version;
    }
    
    @Override
    public Long getVersion() {
        return version;
    }

    @Override
    public void setRecordType(QName name, Long version) {
        setRecordType(Scope.NON_VERSIONED, name, version);
    }
    
    @Override
    public void setRecordType(QName name) {
        setRecordType(name, null);
    }
    
    @Override
    public QName getRecordTypeName() {
        return getRecordTypeName(Scope.NON_VERSIONED);
    }

    @Override
    public Long getRecordTypeVersion() {
        return getRecordTypeVersion(Scope.NON_VERSIONED);
    }
    
    @Override
    public void setRecordType(Scope scope, QName name, Long version) {
        if (name == null && version == null) {
            recordTypes.remove(scope);
        } else {
            recordTypes.put(scope, new RecordTypeRef(name, version));
        }
    }
    
    @Override
    public QName getRecordTypeName(Scope scope) {
        RecordTypeRef ref = recordTypes.get(scope);
        return ref != null ? ref.name : null;
    }
    
    @Override
    public Long getRecordTypeVersion(Scope scope) {
        RecordTypeRef ref = recordTypes.get(scope);
        return ref != null ? ref.version : null;
    }
    
    @Override
    public void setField(QName name, Object value) {
        fields.put(name, value);
        fieldsToDelete.remove(name);
    }
    
    @Override
    public <T> T getField(QName name) throws FieldNotFoundException {
        Object field = fields.get(name);
        if (field == null) {
            throw new FieldNotFoundException(name);
        }
        return (T)field;
    }

    @Override
    public boolean hasField(QName fieldName) {
        return fields.containsKey(fieldName);
    }

    @Override
    public Map<QName, Object> getFields() {
        return fields;
    }

    @Override
    public void delete(QName fieldName, boolean addToFieldsToDelete) {
        fields.remove(fieldName);

        if (addToFieldsToDelete) {
            getFieldsToDelete().add(fieldName);
        }
    }

    @Override
    public List<QName> getFieldsToDelete() {
        return fieldsToDelete;
    }

    @Override
    public void addFieldsToDelete(List<QName> names) {
        if (!names.isEmpty()) {
            fieldsToDelete.addAll(names);
        }
    }

    @Override
    public void removeFieldsToDelete(List<QName> names) {
        fieldsToDelete.removeAll(names);
    }

    @Override
    public ResponseStatus getResponseStatus() {
        return responseStatus;
    }

    @Override
    public void setResponseStatus(ResponseStatus status) {
        this.responseStatus = status;
    }

    @Override
    public Record clone() throws RuntimeException {
        try {
            return cloneRecord(new IdentityRecordStack());
        } catch (RecordException e) {
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public Record cloneRecord() throws RecordException {
        return cloneRecord(new IdentityRecordStack());
    }

    @Override
    public Record cloneRecord(IdentityRecordStack parentRecords) throws RecordException {
        if (parentRecords.contains(this))
            throw new RecordException("A record may not be nested in itself: " + id);

        RecordImpl record = new RecordImpl();
        record.id = id;
        record.version = version;
        record.recordTypes.putAll(recordTypes);
        parentRecords.push(this);
        for (Entry<QName, Object> entry : fields.entrySet()) {
            record.fields.put(entry.getKey(), cloneValue(entry.getValue(), record, parentRecords));
        }
        parentRecords.pop();
        if (fieldsToDelete.size() > 0) { // addAll seems expensive even when list is empty
            record.fieldsToDelete.addAll(fieldsToDelete);
        }
        // the ResponseStatus is not cloned, on purpose
        return record;
    }
    
    private boolean detectRecordRecursion(List<Record> parentRecords) {
        for (Entry<QName, Object> entry : fields.entrySet()) {
            if (detectRecordRecursion(entry.getValue(), parentRecords))
                return true;
        }
        return false;
    }

    private boolean detectRecordRecursion(Object value, List<Record> parentRecords) {
        if (value instanceof HierarchyPath) {
            Object[] elements = ((HierarchyPath) value).getElements();
            for (Object object : elements) {
                if (detectRecordRecursion(object, parentRecords))
                    return true;
            }
        }
        if (value instanceof List) {
            List<Object> values = (List<Object>) value;
            for (Object object : values) {
                if (detectRecordRecursion(object, parentRecords))
                    return true;
            }
        }
        if (value instanceof Record) {
            if (parentRecords.contains(value))
                return true;
            Record record = (Record) value;
            parentRecords.add(record);
            Map<QName, Object> fields = record.getFields();
            for (Entry<QName, Object> entry : fields.entrySet()) {
                if (detectRecordRecursion(entry.getValue(), parentRecords))
                    return true;
            }
            parentRecords.remove(record);
        }
        return false; // Skip all other values
    }

    private Object cloneValue(Object value, Record clone, IdentityRecordStack parentRecords)
            throws RecordException {
        if (value instanceof HierarchyPath) {
            Object[] elements = ((HierarchyPath)value).getElements();
            Object[] newElements = new Object[elements.length];
            for (int i = 0; i < newElements.length; i++) {
                newElements[i] = cloneValue(elements[i], clone, parentRecords);
            }
            return new HierarchyPath(newElements);
        }
        if (value instanceof List) {
            List<Object> newList = new ArrayList<Object>();
            List<Object> values = (List<Object>)value;
            for (Object object : values) {
                newList.add(cloneValue(object, clone, parentRecords));
            }
            return newList;
        }
        if (value instanceof Blob) {
            return ((Blob)value).clone();
        }
        if (value instanceof Record) {
            Record record = (Record) value;
            return (record).cloneRecord(parentRecords);
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
        if (obj instanceof IdRecordImpl) {
            return softEquals(((IdRecordImpl)obj).getRecord());
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

    @Override
    public boolean softEquals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (obj instanceof RecordRvtImpl) {
            return softEquals(((RecordRvtImpl)obj).getRecord());
        }
        if (obj instanceof IdRecordImpl) {
            return softEquals(((IdRecordImpl)obj).getRecord());
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
    
    @Override
    public void setDefaultNamespace(String namespace) {
        this.defaultNamespace = namespace;
    }

    private QName resolveNamespace(String name) throws RecordException {
        if (defaultNamespace != null)
            return new QName(defaultNamespace, name);

        QName recordTypeName = getRecordTypeName();
        if (recordTypeName != null)
            return new QName(recordTypeName.getNamespace(), name);

        throw new RecordException("Namespace could not be resolved for name '" + name +
            "' since no default namespace was given and no record type is set.");
    }
    
    @Override
    public void setRecordType(String recordTypeName) throws RecordException {
        setRecordType(resolveNamespace(recordTypeName));
    }
    
    @Override
    public void setRecordType(String recordTypeName, Long version) throws RecordException {
        setRecordType(resolveNamespace(recordTypeName), version);
    }
    
    @Override
    public void setRecordType(Scope scope, String recordTypeName, Long version) throws RecordException {
        setRecordType(scope, resolveNamespace(recordTypeName), version);
    }
    
    @Override
    public <T> T getField(String fieldName) throws FieldNotFoundException, RecordException {
        // The cast to (T) is only needed for a bug in JDK's < 1.6u24
        return (T)getField(resolveNamespace(fieldName));
    }
    
    @Override
    public void setField(String fieldName, Object value) throws RecordException {
        setField(resolveNamespace(fieldName), value);
    }
    
    @Override
    public void delete(String fieldName, boolean addFieldsToDelete) throws RecordException {
        delete(resolveNamespace(fieldName), addFieldsToDelete);
    }
    
    @Override
    public boolean hasField(String fieldName) throws RecordException {
        return hasField(resolveNamespace(fieldName));
    }
}