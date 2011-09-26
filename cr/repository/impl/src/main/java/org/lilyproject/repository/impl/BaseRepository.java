package org.lilyproject.repository.impl;

import org.lilyproject.repository.api.*;
import org.lilyproject.util.ArgumentValidator;

import java.io.InputStream;
import java.io.OutputStream;

public abstract class BaseRepository implements Repository {
    protected final BlobManager blobManager;
    protected final TypeManager typeManager;
    protected final IdGenerator idGenerator;

    protected BaseRepository(TypeManager typeManager, BlobManager blobManager, IdGenerator idGenerator) {
        this.typeManager = typeManager;
        this.blobManager = blobManager;
        this.idGenerator = idGenerator;
    }
    
    public TypeManager getTypeManager() {
        return typeManager;
    }

    public Record newRecord() {
        return new RecordImpl();
    }

    public Record newRecord(RecordId recordId) {
        ArgumentValidator.notNull(recordId, "recordId");
        return new RecordImpl(recordId);
    }

    public void registerBlobStoreAccess(BlobStoreAccess blobStoreAccess) {
        blobManager.register(blobStoreAccess);
    }

    public OutputStream getOutputStream(Blob blob) throws BlobException {
        return blobManager.getOutputStream(blob);
    }

    public InputStream getInputStream(RecordId recordId, Long version, QName fieldName, int... indexes)
            throws RepositoryException, InterruptedException {
        Record record = read(recordId, version, fieldName);
        return getInputStream(record, fieldName, indexes);
    }
    
    public InputStream getInputStream(RecordId recordId, QName fieldName)
            throws RepositoryException, InterruptedException {
        return getInputStream(recordId, null, fieldName);
    }
    
    public InputStream getInputStream(Record record, QName fieldName, int... indexes)
            throws RepositoryException, InterruptedException {
        FieldType fieldType = typeManager.getFieldTypeByName(fieldName);
        return blobManager.getBlobAccess(record, fieldName, fieldType, indexes).getInputStream();
    }
    
    public BlobAccess getBlob(RecordId recordId, Long version, QName fieldName, int... indexes)
            throws RepositoryException, InterruptedException {
        Record record = read(recordId, version, fieldName);
        FieldType fieldType = typeManager.getFieldTypeByName(fieldName);
        return blobManager.getBlobAccess(record, fieldName, fieldType, indexes);
    }

    @Override
    public BlobAccess getBlob(RecordId recordId, Long version, QName fieldName, Integer mvIndex, Integer hIndex)
            throws RepositoryException, InterruptedException {
        return getBlob(recordId, version, fieldName, convertToIndexes(mvIndex, hIndex));
    }

    public BlobAccess getBlob(RecordId recordId, QName fieldName) throws RepositoryException, InterruptedException {
        return getBlob(recordId, null, fieldName);
    }

    @Override
    public InputStream getInputStream(RecordId recordId, Long version, QName fieldName, Integer mvIndex, Integer hIndex)
            throws RepositoryException, InterruptedException {
        return getInputStream(recordId, version, fieldName, convertToIndexes(mvIndex, hIndex));
    }

    @Override
    public InputStream getInputStream(Record record, QName fieldName, Integer mvIndex, Integer hIndex)
            throws RepositoryException, InterruptedException {
        return getInputStream(record, fieldName, convertToIndexes(mvIndex, hIndex));
    }

    private int[] convertToIndexes(Integer mvIndex, Integer hIndex) {
        int[] indexes;
        if (mvIndex == null && hIndex == null) {
            indexes = new int[0];
        } else if (mvIndex == null) {
            indexes = new int[] { hIndex };
        } else if (hIndex == null) {
            indexes = new int[] { mvIndex };
        } else {
            indexes = new int[] { mvIndex, hIndex };
        }

        return indexes;
    }
}
