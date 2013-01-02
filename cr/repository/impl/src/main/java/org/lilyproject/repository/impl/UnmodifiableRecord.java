/*
 * Copyright 2012 NGDATA nv
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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.lilyproject.repository.api.FieldNotFoundException;
import org.lilyproject.repository.api.IdentityRecordStack;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordException;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.ResponseStatus;
import org.lilyproject.repository.api.Scope;

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

    @Override
    public Map<String, String> getAttributes() {
        return Collections.unmodifiableMap(delegate.getAttributes());
    }

    @Override
    public boolean hasAttributes() {
        return delegate.hasAttributes();
    }

    @Override
    public void setAttributes(Map<String, String> attributes) {
        throw new RuntimeException(MSG);
    }


}
