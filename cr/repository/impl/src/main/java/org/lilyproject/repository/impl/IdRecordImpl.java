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

import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.lilyproject.repository.api.FieldNotFoundException;
import org.lilyproject.repository.api.IdRecord;
import org.lilyproject.repository.api.IdentityRecordStack;
import org.lilyproject.repository.api.Metadata;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordException;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.ResponseStatus;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.Scope;

public class IdRecordImpl implements IdRecord, Cloneable {
    private Record record;
    private Map<SchemaId, QName> mapping;
    private Map<Scope, SchemaId> recordTypeIds;

    public IdRecordImpl(Record record, Map<SchemaId, QName> idToQNameMapping, Map<Scope, SchemaId> recordTypeIds) {
        this.record = record;
        this.mapping = idToQNameMapping;
        this.recordTypeIds = recordTypeIds;
    }

    @Override
    public <T> T getField(SchemaId fieldId) throws FieldNotFoundException {
        QName qname = mapping.get(fieldId);
        if (qname == null) {
            throw new FieldNotFoundException(fieldId);
        }
        // The cast to (T) is only needed for a bug in JDK's < 1.6u24
        return (T)record.getField(qname);
    }

    @Override
    public boolean hasField(SchemaId fieldId) {
        QName qname = mapping.get(fieldId);
        if (qname == null) {
            return false;
        }
        // Normally, we will only have a mapping for fields which are actually in the record,
        // but just to be sure:
        return record.hasField(qname);
    }

    @Override
    public Map<SchemaId, Object> getFieldsById() {
        Map<QName, Object> fields = record.getFields();
        Map<SchemaId, Object> fieldsById = new HashMap<SchemaId, Object>(fields.size());

        for (Map.Entry<SchemaId, QName> entry : mapping.entrySet()) {
            Object value = fields.get(entry.getValue());
            if (value != null) {
                fieldsById.put(entry.getKey(), value);
            }
        }

        return fieldsById;
    }

    @Override
    public Map<SchemaId, QName> getFieldIdToNameMapping() {
        return mapping;
    }

    @Override
    public SchemaId getRecordTypeId() {
        return recordTypeIds.get(Scope.NON_VERSIONED);
    }

    @Override
    public SchemaId getRecordTypeId(Scope scope) {
        return recordTypeIds.get(scope);
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public void setId(RecordId recordId) {
        record.setId(recordId);
    }

    @Override
    public RecordId getId() {
        return record.getId();
    }

    @Override
    public void setVersion(Long version) {
        record.setVersion(version);
    }

    @Override
    public Long getVersion() {
        return record.getVersion();
    }

    @Override
    public void setRecordType(QName name, Long version) {
        record.setRecordType(name, version);
    }

    @Override
    public void setRecordType(QName name) {
        record.setRecordType(name);
    }

    @Override
    public QName getRecordTypeName() {
        return record.getRecordTypeName();
    }

    @Override
    public Long getRecordTypeVersion() {
        return record.getRecordTypeVersion();
    }

    @Override
    public void setRecordType(Scope scope, QName name, Long version) {
        record.setRecordType(scope, name, version);
    }

    @Override
    public QName getRecordTypeName(Scope scope) {
        return record.getRecordTypeName(scope);
    }

    @Override
    public Long getRecordTypeVersion(Scope scope) {
        return record.getRecordTypeVersion(scope);
    }

    @Override
    public void setField(QName fieldName, Object value) {
        record.setField(fieldName, value);
    }

    @Override
    public <T> T getField(QName fieldName) throws FieldNotFoundException {
        return (T)record.getField(fieldName);
    }

    @Override
    public boolean hasField(QName fieldName) {
        return record.hasField(fieldName);
    }

    @Override
    public Map<QName, Object> getFields() {
        return record.getFields();
    }

    @Override
    public void addFieldsToDelete(List<QName> fieldNames) {
        record.addFieldsToDelete(fieldNames);
    }

    @Override
    public void removeFieldsToDelete(List<QName> fieldNames) {
        record.removeFieldsToDelete(fieldNames);
    }

    @Override
    public List<QName> getFieldsToDelete() {
        return record.getFieldsToDelete();
    }

    @Override
    public void delete(QName fieldName, boolean addToFieldsToDelete) {
        record.delete(fieldName, addToFieldsToDelete);
    }

    @Override
    public ResponseStatus getResponseStatus() {
        return record.getResponseStatus();
    }

    @Override
    public void setResponseStatus(ResponseStatus status) {
        record.setResponseStatus(status);
    }

    @Override
    public IdRecord clone() {
        Record recordClone = this.record.clone();
        return new IdRecordImpl(recordClone, new HashMap<SchemaId, QName>(mapping),
                new EnumMap<Scope, SchemaId>(recordTypeIds));
    }

    @Override
    public IdRecord cloneRecord() throws RecordException {
        return cloneRecord(new IdentityRecordStack());
    }

    @Override
    public IdRecord cloneRecord(IdentityRecordStack parentRecords) throws RecordException {
        Record recordClone = this.record.cloneRecord(parentRecords);
        return new IdRecordImpl(recordClone, new HashMap<SchemaId, QName>(mapping),
                new EnumMap<Scope, SchemaId>(recordTypeIds));
    }

    @Override
    public boolean equals(Object obj) {
        return record.equals(obj);
    }

    @Override
    public int hashCode() {
        return record != null ? record.hashCode() : 0;
    }

    @Override
    public boolean softEquals(Object obj) {
        return record.equals(obj);
    }

    @Override
    public void setDefaultNamespace(String namespace) {
        record.setDefaultNamespace(namespace);
    }

    @Override
    public void setRecordType(String recordTypeName) throws RecordException {
        record.setRecordType(recordTypeName);
    }

    @Override
    public void setRecordType(String recordTypeName, Long version) throws RecordException {
        record.setRecordType(recordTypeName, version);
    }

    @Override
    public void setRecordType(Scope scope, String recordTypeName, Long version) throws RecordException {
        record.setRecordType(scope, recordTypeName, version);
    }

    @Override
    public void setField(String fieldName, Object value) throws RecordException {
        record.setField(fieldName, value);
    }

    @Override
    public <T> T getField(String fieldName) throws FieldNotFoundException, RecordException {
        // The cast to (T) is only needed for a bug in JDK's < 1.6u24
        return (T)record.getField(fieldName);
    }

    @Override
    public void delete(String fieldName, boolean addFieldsToDelete) throws RecordException {
        record.delete(fieldName, addFieldsToDelete);
    }

    @Override
    public boolean hasField(String fieldName) throws RecordException {
        return record.hasField(fieldName);
    }

    @Override
    public Map<String, String> getAttributes() {
        return record.getAttributes();
    }

    @Override
    public boolean hasAttributes() {
        return record.hasAttributes();
    }

    @Override
    public void setAttributes(Map<String, String> attributes) {
        record.setAttributes(attributes);
    }

    @Override
    public Metadata getMetadata(QName fieldName) {
        return record.getMetadata(fieldName);
    }

    @Override
    public void setMetadata(QName fieldName, Metadata metadata) {
        record.setMetadata(fieldName, metadata);
    }

    @Override
    public Map<QName, Metadata> getMetadataMap() {
        return record.getMetadataMap();
    }
}
