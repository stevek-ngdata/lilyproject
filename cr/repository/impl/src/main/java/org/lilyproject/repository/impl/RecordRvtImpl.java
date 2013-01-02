/*
 * Copyright 2011 Outerthought bvba
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

import java.util.List;
import java.util.Map;

import org.lilyproject.bytes.impl.DataInputImpl;
import org.lilyproject.repository.api.FieldNotFoundException;
import org.lilyproject.repository.api.IdRecord;
import org.lilyproject.repository.api.IdentityRecordStack;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordException;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.ResponseStatus;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.impl.valuetype.RecordValueType;

public class RecordRvtImpl implements IdRecord, Cloneable {

    private IdRecord delegate;
    private byte[] bytes;
    private RecordValueType recordValueType;

    public RecordRvtImpl(byte[] bytes, RecordValueType recordValueType) {
        this.bytes = bytes;
        this.recordValueType = recordValueType;
    }

    /**
     * @param clearBytes should be false for read operations, true for write operations.
     *                   The idea is that as long as the record is not modified, the
     *                   existing bytes can be reused.
     */
    private synchronized void decode(boolean clearBytes) {
        if (delegate == null) {
            try {
                delegate = (IdRecord)recordValueType.read(new DataInputImpl(bytes));
            } catch (RepositoryException e) {
                throw new RuntimeException("Failed to decode record ");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (clearBytes) {
            bytes = null;
        }
    }

    public byte[] getBytes() {
        return bytes;
    }

    @Override
    public void addFieldsToDelete(List<QName> fieldNames) {
        decode(true);
        delegate.addFieldsToDelete(fieldNames);
    }

    @Override
    public void delete(QName fieldName, boolean addToFieldsToDelete) {
        decode(true);
        delegate.delete(fieldName, addToFieldsToDelete);
    }

    @Override
    public void delete(String fieldName, boolean addToFieldsToDelete) throws RecordException {
        decode(true);
        delegate.delete(fieldName, addToFieldsToDelete);
    }

    @Override
    public <T> T getField(QName fieldName) throws FieldNotFoundException {
        decode(false);
        return delegate.getField(fieldName);
    }

    @Override
    public <T> T getField(String fieldName) throws FieldNotFoundException, RecordException {
        decode(false);
        return delegate.getField(fieldName);
    }

    @Override
    public Map<QName, Object> getFields() {
        decode(false);
        return delegate.getFields();
    }

    @Override
    public List<QName> getFieldsToDelete() {
        decode(false);
        return delegate.getFieldsToDelete();
    }

    @Override
    public RecordId getId() {
        decode(false);
        return delegate.getId();
    }

    @Override
    public QName getRecordTypeName() {
        decode(false);
        return delegate.getRecordTypeName();
    }

    @Override
    public QName getRecordTypeName(Scope scope) {
        decode(false);
        return delegate.getRecordTypeName(scope);
    }

    @Override
    public Long getRecordTypeVersion() {
        decode(false);
        return delegate.getRecordTypeVersion();
    }

    @Override
    public Long getRecordTypeVersion(Scope scope) {
        decode(false);
        return delegate.getRecordTypeVersion(scope);
    }

    @Override
    public ResponseStatus getResponseStatus() {
        decode(false);
        return delegate.getResponseStatus();
    }

    @Override
    public Long getVersion() {
        decode(false);
        return delegate.getVersion();
    }

    @Override
    public boolean hasField(QName fieldName) {
        decode(false);
        return delegate.hasField(fieldName);
    }

    @Override
    public boolean hasField(String fieldName) throws RecordException {
        decode(false);
        return delegate.hasField(fieldName);
    }

    @Override
    public void removeFieldsToDelete(List<QName> fieldNames) {
        decode(true);
        delegate.removeFieldsToDelete(fieldNames);
    }

    @Override
    public void setDefaultNamespace(String namespace) {
        decode(true);
        delegate.setDefaultNamespace(namespace);
    }

    @Override
    public void setField(QName fieldName, Object value) {
        decode(true);
        delegate.setField(fieldName, value);
    }

    @Override
    public void setField(String fieldName, Object value) throws RecordException {
        decode(true);
        delegate.setField(fieldName, value);
    }

    @Override
    public void setId(RecordId recordId) {
        decode(true);
        delegate.setId(recordId);
    }

    @Override
    public void setRecordType(QName name, Long version) {
        decode(true);
        delegate.setRecordType(name, version);
    }

    @Override
    public void setRecordType(QName name) {
        decode(true);
        delegate.setRecordType(name);
    }

    @Override
    public void setRecordType(Scope scope, QName name, Long version) {
        decode(true);
        delegate.setRecordType(scope, name, version);
    }

    @Override
    public void setRecordType(String recordTypeName) throws RecordException {
        decode(true);
        delegate.setRecordType(recordTypeName);
    }

    @Override
    public void setRecordType(String recordTypeName, Long version) throws RecordException {
        decode(true);
        delegate.setRecordType(recordTypeName, version);
    }

    @Override
    public void setRecordType(Scope scope, String recordTypeName, Long version) throws RecordException {
        decode(true);
        delegate.setRecordType(scope, recordTypeName, version);
    }

    @Override
    public void setResponseStatus(ResponseStatus status) {
        decode(true);
        delegate.setResponseStatus(status);
    }

    @Override
    public void setVersion(Long version) {
        decode(true);
        delegate.setVersion(version);
    }

    @Override
    public IdRecord clone() {
        return new RecordRvtImpl(bytes, recordValueType);
    }

    @Override
    public IdRecord cloneRecord() throws RecordException {
        return clone();
    }

    @Override
    public IdRecord cloneRecord(IdentityRecordStack parentRecords) throws RecordException {
        return clone();
    }

    @Override
    public boolean softEquals(Object obj) {
        decode(false);
        return delegate.softEquals(obj);
    }

    @Override
    public boolean equals(Object obj) {
        decode(false);
        return delegate.equals(obj);
    }

    @Override
    public int hashCode() {
        decode(false);
        return delegate.hashCode();
    }

    @Override
    public Record getRecord() {
        decode(true);
        return delegate;
    }

    @Override
    public Map<SchemaId, QName> getFieldIdToNameMapping() {
        decode(false);
        return delegate.getFieldIdToNameMapping();
    }

    @Override
    public SchemaId getRecordTypeId() {
        decode(false);
        return delegate.getRecordTypeId();
    }

    @Override
    public SchemaId getRecordTypeId(Scope scope) {
        decode(false);
        return delegate.getRecordTypeId(scope);
    }

    @Override
    public <T> T getField(SchemaId fieldId) throws FieldNotFoundException {
        decode(false);
        return delegate.getField(fieldId);
    }

    @Override
    public boolean hasField(SchemaId fieldId) {
        decode(false);
        return delegate.hasField(fieldId);
    }

    @Override
    public Map<SchemaId, Object> getFieldsById() {
        decode(false);
        return delegate.getFieldsById();
    }

    @Override
    public Map<String, String> getAttributes() {
        decode(false);
        return delegate.getAttributes();
    }

    @Override
    public boolean hasAttributes() {
        decode(false);
        return delegate.hasAttributes();
    }

    @Override
    public void setAttributes(Map<String, String> attributes) {
        decode(true);
        delegate.setAttributes(attributes);
    }
}
