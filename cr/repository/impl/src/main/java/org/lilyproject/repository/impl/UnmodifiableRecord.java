package org.lilyproject.repository.impl;

import org.lilyproject.repository.api.*;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Wrapper around Record that disallows updates.
 */
public class UnmodifiableRecord implements Record {
    private Record delegate;

    private static final String MSG = "Modification of this record object was not expected.";

    public UnmodifiableRecord(Record delegate) {
        this.delegate = delegate;
    }

    @Override
    public void setId(RecordId recordId) {
        throw new RuntimeException(MSG);
    }

    @Override
    public RecordId getId() {
        return delegate.getId();
    }

    @Override
    public void setVersion(Long version) {
        throw new RuntimeException(MSG);
    }

    @Override
    public Long getVersion() {
        return delegate.getVersion();
    }

    @Override
    public void setRecordType(QName name, Long version) {
        throw new RuntimeException(MSG);
    }

    @Override
    public void setRecordType(QName name) {
        throw new RuntimeException(MSG);
    }

    @Override
    public QName getRecordTypeName() {
        return delegate.getRecordTypeName();
    }

    @Override
    public Long getRecordTypeVersion() {
        return delegate.getRecordTypeVersion();
    }

    @Override
    public void setRecordType(Scope scope, QName name, Long version) {
        throw new RuntimeException(MSG);
    }

    @Override
    public QName getRecordTypeName(Scope scope) {
        return delegate.getRecordTypeName(scope);
    }

    @Override
    public Long getRecordTypeVersion(Scope scope) {
        return delegate.getRecordTypeVersion(scope);
    }

    @Override
    public void setField(QName fieldName, Object value) {
        throw new RuntimeException(MSG);
    }

    @Override
    public void delete(QName fieldName, boolean addToFieldsToDelete) {
        throw new RuntimeException(MSG);
    }

    @Override
    public Object getField(QName fieldName) throws FieldNotFoundException {
        return delegate.getField(fieldName);
    }

    @Override
    public boolean hasField(QName fieldName) {
        return delegate.hasField(fieldName);
    }

    @Override
    public Map<QName, Object> getFields() {
        return Collections.unmodifiableMap(delegate.getFields());
    }

    @Override
    public void addFieldsToDelete(List<QName> fieldNames) {
        throw new RuntimeException(MSG);
    }

    @Override
    public void removeFieldsToDelete(List<QName> fieldNames) {
        throw new RuntimeException(MSG);
    }

    @Override
    public List<QName> getFieldsToDelete() {
        return Collections.unmodifiableList(delegate.getFieldsToDelete());
    }

    @Override
    public ResponseStatus getResponseStatus() {
        return delegate.getResponseStatus();
    }

    @Override
    public void setResponseStatus(ResponseStatus status) {
        throw new RuntimeException(MSG);
    }

    @Override
    public Record clone() {
        return delegate.clone();
    }

    @Override
    public boolean equals(Object obj) {
        return delegate.equals(obj);
    }

    @Override
    public boolean softEquals(Object obj) {
        return delegate.softEquals(obj);
    }
}
