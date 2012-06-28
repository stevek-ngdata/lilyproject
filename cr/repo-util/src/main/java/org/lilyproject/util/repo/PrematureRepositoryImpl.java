package org.lilyproject.util.repo;

import org.lilyproject.repository.api.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Set;

/**
 * See {@link PrematureRepository}.
 */
public class PrematureRepositoryImpl implements PrematureRepository {
    private volatile Repository delegate;
    private final Object delegateAvailable = new Object();

    public PrematureRepositoryImpl() {
    }

    private void waitOnRepo() {
        while (delegate == null) {
            synchronized (delegateAvailable) {
                try {
                    delegateAvailable.wait();
                } catch (InterruptedException e) {
                    throw new RuntimeException("Interrupted while waiting for repository to become available.", e);
                }
            }
        }
    }

    public void setRepository(Repository repository) {
        this.delegate = repository;
        synchronized (delegateAvailable) {
            delegateAvailable.notifyAll();
        }
    }

    @Override
    public Record newRecord() throws RecordException {
        waitOnRepo();
        return delegate.newRecord();
    }

    @Override
    public Record newRecord(RecordId recordId) throws RecordException {
        waitOnRepo();
        return delegate.newRecord(recordId);
    }

    @Override
    public Record create(Record record) throws RepositoryException, InterruptedException {
        waitOnRepo();
        return delegate.create(record);
    }

    @Override
    public Record update(Record record, boolean updateVersion, boolean useLatestRecordType)
            throws RepositoryException, InterruptedException {
        waitOnRepo();
        return delegate.update(record, updateVersion, useLatestRecordType);
    }

    @Override
    public Record update(Record record) throws RepositoryException, InterruptedException {
        waitOnRepo();
        return delegate.update(record);
    }

    @Override
    public Record update(Record record, List<MutationCondition> conditions)
            throws RepositoryException, InterruptedException {
        waitOnRepo();
        return delegate.update(record, conditions);
    }

    @Override
    public Record update(Record record, boolean updateVersion, boolean useLatestRecordType,
            List<MutationCondition> conditions) throws RepositoryException, InterruptedException {
        waitOnRepo();
        return delegate.update(record, updateVersion, useLatestRecordType, conditions);
    }

    @Override
    public Record createOrUpdate(Record record) throws RepositoryException, InterruptedException {
        waitOnRepo();
        return delegate.createOrUpdate(record);
    }

    @Override
    public Record createOrUpdate(Record record, boolean useLatestRecordType)
            throws RepositoryException, InterruptedException {
        waitOnRepo();
        return delegate.createOrUpdate(record, useLatestRecordType);
    }

    @Override
    public Record read(RecordId recordId, List<QName> fieldNames) throws RepositoryException, InterruptedException {
        waitOnRepo();
        return delegate.read(recordId, fieldNames);
    }

    @Override
    public Record read(RecordId recordId, QName... fieldNames) throws RepositoryException, InterruptedException {
        waitOnRepo();
        return delegate.read(recordId, fieldNames);
    }

    @Override
    public List<Record> read(List<RecordId> recordIds, List<QName> fieldNames)
            throws RepositoryException, InterruptedException {
        waitOnRepo();
        return delegate.read(recordIds, fieldNames);
    }

    @Override
    public List<Record> read(List<RecordId> recordIds, QName... fieldNames)
            throws RepositoryException, InterruptedException {
        waitOnRepo();
        return delegate.read(recordIds, fieldNames);
    }

    @Override
    public Record read(RecordId recordId, Long version, List<QName> fieldNames)
            throws RepositoryException, InterruptedException {
        waitOnRepo();
        return delegate.read(recordId, version, fieldNames);
    }

    @Override
    public Record read(RecordId recordId, Long version, QName... fieldNames)
            throws RepositoryException, InterruptedException {
        waitOnRepo();
        return delegate.read(recordId, version, fieldNames);
    }

    @Override
    public List<Record> readVersions(RecordId recordId, Long fromVersion, Long toVersion, List<QName> fieldNames)
            throws RepositoryException, InterruptedException {
        waitOnRepo();
        return delegate.readVersions(recordId, fromVersion, toVersion, fieldNames);
    }

    @Override
    public List<Record> readVersions(RecordId recordId, Long fromVersion, Long toVersion, QName... fieldNames)
            throws RepositoryException, InterruptedException {
        waitOnRepo();
        return delegate.readVersions(recordId, fromVersion, toVersion, fieldNames);
    }

    @Override
    public List<Record> readVersions(RecordId recordId, List<Long> versions, List<QName> fieldNames)
            throws RepositoryException, InterruptedException {
        waitOnRepo();
        return delegate.readVersions(recordId, versions, fieldNames);
    }

    @Override
    public List<Record> readVersions(RecordId recordId, List<Long> versions, QName... fieldNames)
            throws RepositoryException, InterruptedException {
        waitOnRepo();
        return delegate.readVersions(recordId, versions, fieldNames);
    }

    @Override
    public IdRecord readWithIds(RecordId recordId, Long version, List<SchemaId> fieldIds)
            throws RepositoryException, InterruptedException {
        waitOnRepo();
        return delegate.readWithIds(recordId, version, fieldIds);
    }

    @Override
    public void delete(RecordId recordId) throws RepositoryException, InterruptedException {
        waitOnRepo();
        delegate.delete(recordId);
    }

    @Override
    public Record delete(RecordId recordId, List<MutationCondition> conditions)
            throws RepositoryException, InterruptedException {
        waitOnRepo();
        return delegate.delete(recordId, conditions);
    }

    @Override
    public IdGenerator getIdGenerator() {
        waitOnRepo();
        return delegate.getIdGenerator();
    }

    @Override
    public TypeManager getTypeManager() {
        waitOnRepo();
        return delegate.getTypeManager();
    }

    @Override
    public void registerBlobStoreAccess(BlobStoreAccess blobStoreAccess) {
        waitOnRepo();
        delegate.registerBlobStoreAccess(blobStoreAccess);
    }

    @Override
    public OutputStream getOutputStream(Blob blob) throws RepositoryException, InterruptedException {
        waitOnRepo();
        return delegate.getOutputStream(blob);
    }

    @Override
    public BlobAccess getBlob(RecordId recordId, Long version, QName fieldName, int... indexes)
            throws RepositoryException, InterruptedException {
        waitOnRepo();
        return delegate.getBlob(recordId, version, fieldName, indexes);
    }

    @Override
    public BlobAccess getBlob(RecordId recordId, Long version, QName fieldName, Integer mvIndex, Integer hIndex)
            throws RepositoryException, InterruptedException {
        waitOnRepo();
        return delegate.getBlob(recordId, version, fieldName, mvIndex, hIndex);
    }

    @Override
    public BlobAccess getBlob(RecordId recordId, QName fieldName) throws RepositoryException, InterruptedException {
        waitOnRepo();
        return delegate.getBlob(recordId, fieldName);
    }

    @Override
    public InputStream getInputStream(RecordId recordId, Long version, QName fieldName, int... indexes)
            throws RepositoryException, InterruptedException {
        waitOnRepo();
        return delegate.getInputStream(recordId, version, fieldName, indexes);
    }

    @Override
    public InputStream getInputStream(RecordId recordId, Long version, QName fieldName, Integer mvIndex, Integer hIndex)
            throws RepositoryException, InterruptedException {
        waitOnRepo();
        return delegate.getInputStream(recordId, version, fieldName, mvIndex, hIndex);
    }

    @Override
    public InputStream getInputStream(RecordId recordId, QName fieldName)
            throws RepositoryException, InterruptedException {
        waitOnRepo();
        return delegate.getInputStream(recordId, fieldName);
    }

    @Override
    public InputStream getInputStream(Record record, QName fieldName, int... indexes)
            throws RepositoryException, InterruptedException {
        waitOnRepo();
        return delegate.getInputStream(record, fieldName, indexes);
    }

    @Override
    public InputStream getInputStream(Record record, QName fieldName, Integer mvIndex, Integer hIndex)
            throws RepositoryException, InterruptedException {
        waitOnRepo();
        return delegate.getInputStream(record, fieldName, mvIndex, hIndex);
    }

    @Override
    public Set<RecordId> getVariants(RecordId recordId) throws RepositoryException, InterruptedException {
        waitOnRepo();
        return delegate.getVariants(recordId);
    }

    @Override
    public RecordScanner getScanner(RecordScan scan) throws RepositoryException, InterruptedException {
        waitOnRepo();
        return delegate.getScanner(scan);
    }

    @Override
    public IdRecordScanner getScannerWithIds(RecordScan scan) throws RepositoryException, InterruptedException {
        waitOnRepo();
        return delegate.getScannerWithIds(scan);
    }

    @Override
    public RecordBuilder recordBuilder() throws RecordException, InterruptedException {
        waitOnRepo();
        return delegate.recordBuilder();
    }

    @Override
    public void close() throws IOException {
        waitOnRepo();
        delegate.close();
    }
}
