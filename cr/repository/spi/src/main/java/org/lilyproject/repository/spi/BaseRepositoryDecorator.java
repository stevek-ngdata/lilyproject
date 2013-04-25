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
package org.lilyproject.repository.spi;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Set;

import org.lilyproject.repository.api.Blob;
import org.lilyproject.repository.api.BlobAccess;
import org.lilyproject.repository.api.BlobStoreAccess;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.IdRecord;
import org.lilyproject.repository.api.IdRecordScanner;
import org.lilyproject.repository.api.LTable;
import org.lilyproject.repository.api.MutationCondition;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordBuilder;
import org.lilyproject.repository.api.RecordException;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.RecordScan;
import org.lilyproject.repository.api.RecordScanner;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.RepositoryManager;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.TypeManager;

/**
 * Base class for implementing your RepositoryDecorator, avoids having to delegate all methods.
 */
public class BaseRepositoryDecorator implements RepositoryDecorator {
    protected Repository delegate;

    @Override
    public void setDelegate(Repository repository) {
        this.delegate = repository;
    }

    @Override
    public LTable getTable(String tableName) throws IOException, InterruptedException {
        return delegate.getTable(tableName);
    }

    @Override
    public LTable getDefaultTable() throws IOException, InterruptedException {
        return delegate.getDefaultTable();
    }

    @Override
    public Record newRecord() throws RecordException {
        return delegate.newRecord();
    }

    @Override
    public Record newRecord(RecordId recordId) throws RecordException {
        return delegate.newRecord(recordId);
    }

    @Override
    public Record create(Record record) throws RepositoryException, InterruptedException {
        return delegate.create(record);
    }

    @Override
    public Record update(Record record, boolean updateVersion, boolean useLatestRecordType)
            throws RepositoryException, InterruptedException {
        return delegate.update(record, updateVersion, useLatestRecordType);
    }

    @Override
    public Record update(Record record, boolean updateVersion, boolean useLatestRecordType,
                         List<MutationCondition> conditions) throws RepositoryException, InterruptedException {
        return delegate.update(record, updateVersion, useLatestRecordType, conditions);
    }

    @Override
    public Record update(Record record) throws RepositoryException, InterruptedException {
        return delegate.update(record);
    }

    @Override
    public Record update(Record record, List<MutationCondition> conditions)
            throws RepositoryException, InterruptedException {
        return delegate.update(record, conditions);
    }

    @Override
    public Record createOrUpdate(Record record) throws RepositoryException, InterruptedException {
        return delegate.createOrUpdate(record);
    }

    @Override
    public Record createOrUpdate(Record record, boolean useLatestRecordType)
            throws RepositoryException, InterruptedException {
        return delegate.createOrUpdate(record, useLatestRecordType);
    }

    @Override
    public Record read(RecordId recordId, List<QName> fieldNames) throws RepositoryException, InterruptedException {
        return delegate.read(recordId, fieldNames);
    }

    @Override
    public Record read(RecordId recordId, QName... fieldNames) throws RepositoryException, InterruptedException {
        return delegate.read(recordId, fieldNames);
    }

    @Override
    public List<Record> read(List<RecordId> recordIds, List<QName> fieldNames)
            throws RepositoryException, InterruptedException {
        return delegate.read(recordIds, fieldNames);
    }

    @Override
    public List<Record> read(List<RecordId> recordIds, QName... fieldNames)
            throws RepositoryException, InterruptedException {
        return delegate.read(recordIds, fieldNames);
    }

    @Override
    public Record read(RecordId recordId, Long version, List<QName> fieldNames)
            throws RepositoryException, InterruptedException {
        return delegate.read(recordId, version, fieldNames);
    }

    @Override
    public Record read(RecordId recordId, Long version, QName... fieldNames)
            throws RepositoryException, InterruptedException {
        return delegate.read(recordId, version, fieldNames);
    }

    @Override
    public List<Record> readVersions(RecordId recordId, Long fromVersion, Long toVersion, List<QName> fieldNames)
            throws RepositoryException, InterruptedException {
        return delegate.readVersions(recordId, fromVersion, toVersion, fieldNames);
    }

    @Override
    public List<Record> readVersions(RecordId recordId, Long fromVersion, Long toVersion, QName... fieldNames)
            throws RepositoryException, InterruptedException {
        return delegate.readVersions(recordId, fromVersion, toVersion, fieldNames);
    }

    @Override
    public List<Record> readVersions(RecordId recordId, List<Long> versions, List<QName> fieldNames)
            throws RepositoryException, InterruptedException {
        return delegate.readVersions(recordId, versions, fieldNames);
    }

    @Override
    public List<Record> readVersions(RecordId recordId, List<Long> versions, QName... fieldNames)
            throws RepositoryException, InterruptedException {
        return delegate.readVersions(recordId, versions, fieldNames);
    }

    @Override
    public IdRecord readWithIds(RecordId recordId, Long version, List<SchemaId> fieldIds)
            throws RepositoryException, InterruptedException {
        return delegate.readWithIds(recordId, version, fieldIds);
    }

    @Override
    public void delete(RecordId recordId) throws RepositoryException, InterruptedException {
        delegate.delete(recordId);
    }

    @Override
    public Record delete(RecordId recordId, List<MutationCondition> conditions)
            throws RepositoryException, InterruptedException {
        return delegate.delete(recordId, conditions);
    }

    @Override
    public void delete(Record record) throws RepositoryException, InterruptedException {
        delegate.delete(record);
    }

    @Override
    public IdGenerator getIdGenerator() {
        return delegate.getIdGenerator();
    }

    @Override
    public TypeManager getTypeManager() {
        return delegate.getTypeManager();
    }

    @Override
    public void registerBlobStoreAccess(BlobStoreAccess blobStoreAccess) {
        delegate.registerBlobStoreAccess(blobStoreAccess);
    }

    @Override
    public OutputStream getOutputStream(Blob blob) throws RepositoryException, InterruptedException {
        return delegate.getOutputStream(blob);
    }

    @Override
    public BlobAccess getBlob(RecordId recordId, Long version, QName fieldName, int... indexes)
            throws RepositoryException, InterruptedException {
        return delegate.getBlob(recordId, version, fieldName, indexes);
    }

    @Override
    public BlobAccess getBlob(RecordId recordId, Long version, QName fieldName, Integer mvIndex, Integer hIndex)
            throws RepositoryException, InterruptedException {
        return delegate.getBlob(recordId, version, fieldName, mvIndex, hIndex);
    }

    @Override
    public BlobAccess getBlob(RecordId recordId, QName fieldName) throws RepositoryException, InterruptedException {
        return delegate.getBlob(recordId, fieldName);
    }

    @Override
    public InputStream getInputStream(RecordId recordId, Long version, QName fieldName, int... indexes)
            throws RepositoryException, InterruptedException {
        return delegate.getInputStream(recordId, version, fieldName, indexes);
    }

    @Override
    public InputStream getInputStream(RecordId recordId, QName fieldName) throws RepositoryException,
            InterruptedException {
        return delegate.getInputStream(recordId, fieldName);
    }

    @Override
    public InputStream getInputStream(Record record, QName fieldName, int... indexes)
            throws RepositoryException, InterruptedException {
        return delegate.getInputStream(record, fieldName, indexes);
    }

    @Override
    public InputStream getInputStream(RecordId recordId, Long version, QName fieldName, Integer mvIndex, Integer hIndex)
            throws RepositoryException, InterruptedException {
        return delegate.getInputStream(recordId, version, fieldName, mvIndex, hIndex);
    }

    @Override
    public InputStream getInputStream(Record record, QName fieldName, Integer mvIndex, Integer hIndex)
            throws RepositoryException, InterruptedException {
        return delegate.getInputStream(record, fieldName, mvIndex, hIndex);
    }

    @Override
    public Set<RecordId> getVariants(RecordId recordId) throws RepositoryException, InterruptedException {
        return delegate.getVariants(recordId);
    }

    @Override
    public RecordScanner getScanner(RecordScan scan) throws RepositoryException, InterruptedException {
        return delegate.getScanner(scan);
    }

    @Override
    public IdRecordScanner getScannerWithIds(RecordScan scan) throws RepositoryException, InterruptedException {
        return delegate.getScannerWithIds(scan);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public RecordBuilder recordBuilder() throws RecordException, InterruptedException {
        return delegate.recordBuilder();
    }

    @Override
    public RepositoryManager getRepositoryManager() {
        return delegate.getRepositoryManager();
    }

    @Override
    public String getTableName() {
        return delegate.getTableName();
    }
}
