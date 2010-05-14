package org.lilycms.repository.impl;

import org.lilycms.repository.api.*;
import org.lilycms.repository.api.FieldNotFoundException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IdRecordImpl implements IdRecord {
    private Record record;
    private Map<String, QName> mapping;

    public IdRecordImpl(Record record, Map<String, QName> idToQNameMapping) {
        this.record = record;
        this.mapping = idToQNameMapping;
    }

    public Object getField(String fieldId) throws FieldNotFoundException {
        QName qname = mapping.get(fieldId);
        if (qname == null) {
            throw new FieldNotFoundException(fieldId);
        }
        return record.getField(qname);
    }

    public boolean hasField(String fieldId) {
        QName qname = mapping.get(fieldId);
        if (qname == null) {
            return false;
        }
        // Normally, we will only have a mapping for fields which are actually in the record,
        // but just to be sure:
        return record.hasField(qname);
    }

    public Map<String, Object> getFieldsById() {
        Map<QName, Object> fields = record.getFields();
        Map<String, Object> fieldsById = new HashMap<String, Object>(fields.size());

        for (Map.Entry<String, QName> entry : mapping.entrySet()) {
            Object value = fields.get(entry.getValue());
            if (value != null) {
                fieldsById.put(entry.getKey(), value);
            }
        }

        return fieldsById;
    }

    public Record getRecord() {
        return record;
    }

    public void setId(RecordId recordId) {
        record.setId(recordId);
    }

    public RecordId getId() {
        return record.getId();
    }

    public void setVersion(Long version) {
        record.setVersion(version);
    }

    public Long getVersion() {
        return record.getVersion();
    }

    public void setRecordType(String id, Long version) {
        record.setRecordType(id, version);
    }

    public String getRecordTypeId() {
        return record.getRecordTypeId();
    }

    public Long getRecordTypeVersion() {
        return record.getRecordTypeVersion();
    }

    public void setRecordType(Scope scope, String id, Long version) {
        record.setRecordType(scope, id, version);
    }

    public String getRecordTypeId(Scope scope) {
        return record.getRecordTypeId(scope);
    }

    public Long getRecordTypeVersion(Scope scope) {
        return record.getRecordTypeVersion(scope);
    }

    public void setField(QName fieldName, Object value) {
        record.setField(fieldName, value);
    }

    public Object getField(QName fieldName) throws FieldNotFoundException {
        return record.getField(fieldName);
    }

    public boolean hasField(QName fieldName) {
        return record.hasField(fieldName);
    }

    public Map<QName, Object> getFields() {
        return record.getFields();
    }

    public void addFieldsToDelete(List<QName> fieldNames) {
        record.addFieldsToDelete(fieldNames);
    }

    public void removeFieldsToDelete(List<QName> fieldNames) {
        record.removeFieldsToDelete(fieldNames);
    }

    public List<QName> getFieldsToDelete() {
        return record.getFieldsToDelete();
    }

    public void delete(QName fieldName, boolean addToFieldsToDelete) {
        record.delete(fieldName, addToFieldsToDelete);
    }

    public Record clone() {
        throw new UnsupportedOperationException("IdRecordImpl does not support cloning.");
    }

    @Override
    public boolean equals(Object obj) {
        throw new UnsupportedOperationException("IdRecordImpl does not support equals.");
    }
}
