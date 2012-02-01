package org.lilyproject.repository.impl;

import java.util.*;

import org.lilyproject.repository.api.*;

/**
 * Wrapper around Record that disallows updates.
 */
public class UnmodifiableRecord implements Record, Cloneable {
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
    public <T> T getField(QName fieldName) throws FieldNotFoundException {
        // The cast to (T) is only needed for a bug in JDK's < 1.6u24
        return (T)delegate.getField(fieldName);
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
    public Record cloneRecord(IdentityRecordStack parentRecords) throws RecordException {
        return delegate.cloneRecord(parentRecords);
    }

    @Override
    public Record cloneRecord() throws RecordException {
        return delegate.cloneRecord();
    }

    @Override
    public boolean equals(Object obj) {
        return delegate.equals(obj);
    }

    @Override
    public int hashCode() {
        return delegate != null ? delegate.hashCode() : 0;
    }

    @Override
    public boolean softEquals(Object obj) {
        return delegate.softEquals(obj);
    }
    
    @Override
    public void setDefaultNamespace(String namespace) {
        throw new RuntimeException(MSG);
    }
    
    @Override
    public void setRecordType(String recordTypeName) {
        throw new RuntimeException(MSG);
    }

    @Override
    public void setRecordType(String recordTypeName, Long version) {
        throw new RuntimeException(MSG);
    }

    @Override
    public void setRecordType(Scope scope, String recordTypeName, Long version) {
        throw new RuntimeException(MSG);
    }
    
    @Override
    public void setField(String fieldName, Object value) {
        throw new RuntimeException(MSG);
    }

    @Override
    public <T> T getField(String fieldName) throws FieldNotFoundException, RecordException {
        // The cast to (T) is only needed for a bug in JDK's < 1.6u24
        return (T)delegate.getField(fieldName);
    }
    
    @Override
    public void delete(String fieldName, boolean addToFieldsToDelete) {
        throw new RuntimeException(MSG);
    }
    
    @Override
    public boolean hasField(String fieldName) throws RecordException {
        return delegate.hasField(fieldName);
    }
}
