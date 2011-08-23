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

    public InputStream getInputStream(RecordId recordId, Long version, QName fieldName, Integer...indexes) throws RepositoryException, InterruptedException {
        Record record = read(recordId, version, fieldName);
        return getInputStream(record, fieldName, indexes);
    }
    
    public InputStream getInputStream(RecordId recordId, QName fieldName)
            throws RepositoryException, InterruptedException {
        return getInputStream(recordId, null, fieldName);
    }
    
    public InputStream getInputStream(Record record, QName fieldName, Integer...indexes) throws RepositoryException, InterruptedException {
        FieldType fieldType = typeManager.getFieldTypeByName(fieldName);
        return blobManager.getBlobAccess(record, fieldName, fieldType, indexes).getInputStream();
    }
    
    public InputStream getInputStreamNested(RecordId recordId, QName...fieldNames) throws RepositoryException, InterruptedException {
        return getInputStreamNested(recordId, null, fieldNames);
    }
    
    public InputStream getInputStreamNested(RecordId recordId, Long version, QName...fieldNames) throws RepositoryException, InterruptedException {
        ArgumentValidator.notNull(fieldNames, "fieldNames");
        if (fieldNames.length < 1)
            throw new IllegalArgumentException("At least one fieldName should be given");
        Record record = read(recordId, version, fieldNames[0]);
        return getInputStreamNested(record, fieldNames);
    }
    
    public InputStream getInputStreamNested(Record record, QName...fieldNames) throws RepositoryException, InterruptedException {
        ArgumentValidator.notNull(fieldNames, "fieldNames");
        if (fieldNames.length < 1)
            throw new IllegalArgumentException("At least one fieldName should be given");
        int i = 0;
        for (; i < fieldNames.length-1; i++) {
            record = record.getField(fieldNames[i]);
        }
        FieldType fieldType = typeManager.getFieldTypeByName(fieldNames[i]);
        return blobManager.getBlobAccess(record, fieldNames[i], fieldType).getInputStream();
    }

    public BlobAccess getBlob(RecordId recordId, Long version, QName fieldName, Integer...indexes) throws RepositoryException, InterruptedException {
        Record record = read(recordId, version, fieldName);
        FieldType fieldType = typeManager.getFieldTypeByName(fieldName);
        return blobManager.getBlobAccess(record, fieldName, fieldType, indexes);
    }
    
    public BlobAccess getBlob(RecordId recordId, QName fieldName) throws RepositoryException, InterruptedException {
        return getBlob(recordId, null, fieldName);
    }
    
    public BlobAccess getBlobNested(RecordId recordId, Long version, QName...fieldNames) throws RepositoryException, InterruptedException {
        ArgumentValidator.notNull(fieldNames, "fieldNames");
        if (fieldNames.length < 1)
            throw new IllegalArgumentException("At least one fieldName should be given");
        Record record = read(recordId, version, fieldNames[0]);
        int i = 0;
        for (; i < fieldNames.length-1; i++) {
            record = record.getField(fieldNames[i]);
        }
        FieldType fieldType = typeManager.getFieldTypeByName(fieldNames[i]);
        return blobManager.getBlobAccess(record, fieldNames[i], fieldType);
    }
}
