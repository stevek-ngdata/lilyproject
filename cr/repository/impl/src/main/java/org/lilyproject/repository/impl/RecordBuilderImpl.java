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

import java.util.*;

import org.lilyproject.repository.api.*;
import org.lilyproject.util.ArgumentValidator;

/**
 *
 */
public class RecordBuilderImpl implements RecordBuilder {

    private final Repository repository;
    private Record record;
    private List<MutationCondition> mutationConditions = null;
    private boolean updateVersion = false;
    private boolean useLatestRecordType = true;
    private String defaultNamespace;

    // Fields related to nested builders
    private RecordBuilderImpl parent;
    private QName parentField;
    private Mode mode;
    private List<Record> records;
    private enum Mode {ROOT_RECORD, NESTED_RECORD, NESTED_RECORD_LIST}

    public RecordBuilderImpl(Repository repository) throws RecordException {
        this.repository = repository;
        this.mode = Mode.ROOT_RECORD;
        this.record = repository.newRecord();
    }

    public RecordBuilderImpl(RecordBuilderImpl parent, QName parentField, Mode mode) throws RecordException {
        this.repository = parent.repository;
        this.mode = mode;
        this.parent = parent;
        this.parentField = parentField;
        this.record = repository.newRecord();
        defaultNamespace(parent.defaultNamespace);
    }

    @Override
    public RecordBuilder defaultNamespace(String namespace) {
        this.defaultNamespace = namespace;
        record.setDefaultNamespace(namespace);
        return this;
    }

    @Override
    public RecordBuilder field(QName name, Object value) {
        ArgumentValidator.notNull(name, "name");
        record.setField(name, value);
        return this;
    }

    @Override
    public RecordBuilder field(String name, Object value) throws RecordException {
        ArgumentValidator.notNull(name, "name");
        record.setField(name, value);
        return this;
    }

    @Override
    public RecordBuilder mutationCondition(MutationCondition condition) {
        ArgumentValidator.notNull(condition, "condition");
        if (mutationConditions == null) {
            this.mutationConditions = new ArrayList<MutationCondition>();
        }
        mutationConditions.add(condition);
        return this;
    }

    @Override
    public RecordBuilder id(RecordId id) {
        record.setId(id);
        return this;
    }

    @Override
    public RecordBuilder id(RecordId id, Map<String, String> variantProperties) {
        record.setId(repository.getIdGenerator().newRecordId(id.getMaster(), variantProperties));
        return this;
    }

    @Override
    public RecordBuilder id(String userId) {
        record.setId(repository.getIdGenerator().newRecordId(userId));
        return this;
    }

    @Override
    public RecordBuilder id(String userId, Map<String, String> variantProperties) {
        record.setId(repository.getIdGenerator().newRecordId(userId, variantProperties));
        return this;
    }

    @Override
    public RecordBuilder assignNewUuid() {
        record.setId(repository.getIdGenerator().newRecordId());
        return this;
    }

    @Override
    public RecordBuilder assignNewUuid(Map<String, String> variantProperties) {
        record.setId(repository.getIdGenerator().newRecordId(variantProperties));
        return this;
    }

    @Override
    public RecordBuilder recordType(QName name) {
        return recordType(name, null);
    }

    @Override
    public RecordBuilder recordType(QName name, Long version) {
        record.setRecordType(name, version);
        return this;
    }

    @Override
    public RecordBuilder recordType(String name) throws RecordException {
        return recordType(name, null);
    }

    @Override
    public RecordBuilder recordType(String name, Long version) throws RecordException {
        record.setRecordType(name, version);
        return this;
    }

    @Override
    public RecordBuilder updateVersion(boolean updateVersion) {
        this.updateVersion = updateVersion;
        return this;
    }

    @Override
    public RecordBuilder useLatestRecordType(boolean latestRT) {
        this.useLatestRecordType = latestRT;
        return this;
    }

    @Override
    public RecordBuilder attribute(String name, String value) {
        record.getAttributes().put(name, value);
        return this;
    }

    @Override
    public RecordBuilder version(Long version) {
        record.setVersion(version);
        return this;
    }

    @Override
    public RecordBuilder metadata(QName fieldName, Metadata metadata) throws RecordException {
        if (mode == Mode.ROOT_RECORD) {
            record.setMetadata(fieldName, metadata);
        } else {
            throw new IllegalStateException("Metadata is only supported for top-level records.");
        }
        return this;
    }

    @Override
    public RecordBuilder reset() throws RecordException {
        record = repository.newRecord();
        mutationConditions = null;
        updateVersion = false;
        useLatestRecordType = true;
        return this;
    }

    private Record createRecord() {
        return record;
    }

    @Override
    public Record build() {
        if (mode == Mode.ROOT_RECORD) {
            return createRecord();
        } else {
            throw new IllegalStateException("update should only be called for root records, current mode is " + mode);
        }
    }

    @Override
    public Record update() throws RepositoryException, InterruptedException {
        if (mode == Mode.ROOT_RECORD) {
            return repository.update(record, updateVersion, useLatestRecordType, mutationConditions);
        } else {
            throw new IllegalStateException("update should only be called for root records, current mode is " + mode);
        }
    }

    @Override
    public Record create() throws RepositoryException, InterruptedException {
        if (mode == Mode.ROOT_RECORD) {
            return repository.create(record);
        } else {
            throw new IllegalStateException("update should only be called for root records, current mode is " + mode);
        }
    }

    @Override
    public Record createOrUpdate() throws RepositoryException, InterruptedException {
        if (mode == Mode.ROOT_RECORD) {
            return repository.createOrUpdate(record, useLatestRecordType);
        } else {
            throw new IllegalStateException("update should only be called for root records, current mode is " + mode);
        }
    }

    @Override
    public RecordBuilder recordField(String name) throws RecordException {
        return recordField(resolveNamespace(name));
    }

    @Override
    public RecordBuilder recordField(QName name) throws RecordException {
        return new RecordBuilderImpl(this, name, Mode.NESTED_RECORD);
    }

    @Override
    public RecordBuilder set() {
        if (mode == Mode.NESTED_RECORD) {
            parent.record.setField(parentField, createRecord());
            return parent;
        } else {
            throw new IllegalStateException("set should only be called for nested records, current mode is " + mode);
        }
    }

    @Override
    public RecordBuilder recordListField(String name) throws RecordException {
        return recordListField(resolveNamespace(name));
    }

    @Override
    public RecordBuilder recordListField(QName name) throws RecordException {
        return new RecordBuilderImpl(this, name, Mode.NESTED_RECORD_LIST);
    }

    @Override
    public RecordBuilder add() throws RecordException {
        if (mode == Mode.NESTED_RECORD_LIST) {
            if (records == null) {
                records = new ArrayList<Record>();
            }
            records.add(createRecord());

            // Reset, but keep the record type setting (and the default namespace)
            QName prevRecordTypeName = record.getRecordTypeName();
            Long prevRecordTypeVersion = record.getRecordTypeVersion();

            reset();

            recordType(prevRecordTypeName, prevRecordTypeVersion);

            return this;
        } else {
            throw new IllegalStateException("add should only be called for a nested list of records, current mode is " +
                    mode);
        }
    }

    @Override
    public RecordBuilder endList() {
        if (mode == Mode.NESTED_RECORD_LIST) {
            if (records == null) {
                records = new ArrayList<Record>();
            }
            records.add(createRecord());

            parent.field(parentField, records);

            return parent;
        } else {
            throw new IllegalStateException("endList should only be called when creating a nested list of records");
        }
    }

    protected QName resolveNamespace(String name) {
        if (defaultNamespace != null) {
            return new QName(defaultNamespace, name);
        }

        QName recordTypeName = record.getRecordTypeName();
        if (recordTypeName != null) {
            return new QName(recordTypeName.getNamespace(), name);
        }

        throw new IllegalStateException("Namespace could not be resolved for name '" + name +
            "' since no default namespace was given and no record type is set.");
    }
}