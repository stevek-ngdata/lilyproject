package org.lilyproject.repository.spi;

import org.lilyproject.repository.api.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Set;

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
    public Record update(Record record, List<MutationCondition> conditions) throws RepositoryException {
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
    public Record read(RecordId recordId) throws RepositoryException, InterruptedException {
        return delegate.read(recordId);
    }

    @Override
    public Record read(RecordId recordId, List<QName> fieldNames) throws RepositoryException, InterruptedException {
        return delegate.read(recordId, fieldNames);
    }

    @Override
    public List<Record> read(List<RecordId> recordIds) throws RepositoryException, InterruptedException {
        return delegate.read(recordIds);
    }

    @Override
    public List<Record> read(List<RecordId> recordIds, List<QName> fieldNames)
            throws RepositoryException, InterruptedException {
        return delegate.read(recordIds, fieldNames);
    }

    @Override
    public Record read(RecordId recordId, Long version) throws RepositoryException, InterruptedException {
        return delegate.read(recordId, version);
    }

    @Override
    public Record read(RecordId recordId, Long version, List<QName> fieldNames)
            throws RepositoryException, InterruptedException {
        return delegate.read(recordId, version, fieldNames);
    }

    @Override
    public List<Record> readVersions(RecordId recordId, Long fromVersion, Long toVersion, List<QName> fieldNames)
            throws RepositoryException, InterruptedException {
        return delegate.readVersions(recordId, fromVersion, toVersion, fieldNames);
    }

    @Override
    public List<Record> readVersions(RecordId recordId, List<Long> versions, List<QName> fieldNames)
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
    public BlobAccess getBlob(RecordId recordId, Long version, QName fieldName, Integer multiValueIndex,
            Integer hierarchyIndex) throws RepositoryException, InterruptedException {
        return delegate.getBlob(recordId, version, fieldName, multiValueIndex, hierarchyIndex);
    }

    @Override
    public BlobAccess getBlob(RecordId recordId, QName fieldName) throws RepositoryException, InterruptedException {
        return delegate.getBlob(recordId, fieldName);
    }

    @Override
    public InputStream getInputStream(RecordId recordId, Long version, QName fieldName, Integer multivalueIndex,
            Integer hierarchyIndex) throws RepositoryException, InterruptedException {
        return delegate.getInputStream(recordId, version, fieldName, multivalueIndex, hierarchyIndex);
    }

    @Override
    public InputStream getInputStream(RecordId recordId, QName fieldName) throws RepositoryException,
            InterruptedException {
        return delegate.getInputStream(recordId, fieldName);
    }

    @Override
    public InputStream getInputStream(Record record, QName fieldName, Integer multivalueIndex, Integer hierarchyIndex)
            throws RepositoryException, InterruptedException {
        return delegate.getInputStream(record, fieldName, multivalueIndex, hierarchyIndex);
    }

    @Override
    public InputStream getInputStream(Record record, QName fieldName) throws RepositoryException, InterruptedException {
        return delegate.getInputStream(record, fieldName);
    }

    @Override
    public Set<RecordId> getVariants(RecordId recordId) throws RepositoryException, InterruptedException {
        return delegate.getVariants(recordId);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
