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

    public RecordBuilderImpl(Repository repository) throws RecordException {
        this.repository = repository;
        this.record = repository.newRecord();
    }
    
    @Override
    public RecordBuilder defaultNamespace(String namespace) {
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
    public RecordBuilder version(Long version) {
        record.setVersion(version);
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

    @Override
    public Record build() {
        return record;
    }

    @Override
    public Record update() throws RepositoryException, InterruptedException {
        return repository.update(record, updateVersion, useLatestRecordType, mutationConditions);
    }

    @Override
    public Record create() throws RepositoryException, InterruptedException {
        return repository.create(record);
    }
    
    @Override
    public Record createOrUpdate() throws RepositoryException, InterruptedException {
        return repository.createOrUpdate(record, useLatestRecordType);
    }
}