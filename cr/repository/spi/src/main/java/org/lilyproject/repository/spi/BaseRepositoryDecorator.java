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
    public BlobAccess getBlob(RecordId recordId, Long version, QName fieldName, Integer...indexes) throws RepositoryException, InterruptedException {
        return delegate.getBlob(recordId, version, fieldName, indexes);
    }

    @Override
    public BlobAccess getBlob(RecordId recordId, QName fieldName) throws RepositoryException, InterruptedException {
        return delegate.getBlob(recordId, fieldName);
    }
    
    @Override
    public BlobAccess getBlobNested(RecordId recordId, Long version, QName... fieldNames) throws RepositoryException,
            InterruptedException {
        return delegate.getBlobNested(recordId, version, fieldNames);
    }

    @Override
    public InputStream getInputStream(RecordId recordId, Long version, QName fieldName, Integer...indexes) throws RepositoryException, InterruptedException {
        return delegate.getInputStream(recordId, version, fieldName, indexes);
    }

    @Override
    public InputStream getInputStream(RecordId recordId, QName fieldName) throws RepositoryException,
            InterruptedException {
        return delegate.getInputStream(recordId, fieldName);
    }

    @Override
    public InputStream getInputStream(Record record, QName fieldName, Integer...indexes)
            throws RepositoryException, InterruptedException {
        return delegate.getInputStream(record, fieldName, indexes);
    }
    
    @Override
    public InputStream getInputStreamNested(RecordId recordId, Long version, QName... fieldNames)
            throws RepositoryException, InterruptedException {
        return delegate.getInputStreamNested(recordId, version, fieldNames);
    }
    
    @Override
    public InputStream getInputStreamNested(RecordId recordId, QName... fieldNames) throws RepositoryException,
            InterruptedException {
        return delegate.getInputStreamNested(recordId, fieldNames);
    }

    @Override
    public InputStream getInputStreamNested(Record record, QName... fieldNames) throws RepositoryException,
            InterruptedException {
        return delegate.getInputStreamNested(record, fieldNames);
    }

    @Override
    public Set<RecordId> getVariants(RecordId recordId) throws RepositoryException, InterruptedException {
        return delegate.getVariants(recordId);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
    
    @Override
    public RecordBuilder recordBuilder() throws RecordException, InterruptedException {
        return delegate.recordBuilder();
    }


    

}
