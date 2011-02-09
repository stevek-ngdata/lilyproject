package org.lilyproject.repository.impl;

import org.lilyproject.repository.api.*;
import org.lilyproject.util.ArgumentValidator;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

public abstract class BaseRepository implements Repository {
    protected final BlobManager blobManager;
    protected final TypeManager typeManager;

    protected BaseRepository(TypeManager typeManager, BlobManager blobManager) {
        this.typeManager = typeManager;
        this.blobManager = blobManager;
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

    public InputStream getInputStream(RecordId recordId, QName fieldName) throws BlobNotFoundException, BlobException,
    RecordNotFoundException, RecordTypeNotFoundException, FieldTypeNotFoundException, RecordException,
    VersionNotFoundException, TypeException, InterruptedException {
        return getInputStream(recordId, null, fieldName, null, null);
    }

    public InputStream getInputStream(RecordId recordId, Long version, QName fieldName, Integer multivalueIndex,
            Integer hierarchyIndex) throws BlobNotFoundException, BlobException, RecordNotFoundException,
            RecordTypeNotFoundException, FieldTypeNotFoundException, RecordException, VersionNotFoundException,
            TypeException, InterruptedException {
        Record record = read(recordId, version, Arrays.asList(new QName[]{fieldName}));
        return getInputStream(record, fieldName, multivalueIndex, hierarchyIndex);
    }
    
    public InputStream getInputStream(Record record, QName fieldName) throws FieldTypeNotFoundException, TypeException, BlobException, BlobNotFoundException, InterruptedException {
        return getInputStream(record, fieldName, null, null);
    }
    
    public InputStream getInputStream(Record record, QName fieldName, Integer multivalueIndex, Integer hierarchyIndex) throws FieldTypeNotFoundException, TypeException, InterruptedException, BlobException, BlobNotFoundException {
        FieldType fieldType = typeManager.getFieldTypeByName(fieldName);
        return blobManager.getBlobAccess(record, fieldName, multivalueIndex, hierarchyIndex, fieldType).getInputStream();
    }

    public BlobAccess getBlob(RecordId recordId, Long version, QName fieldName, Integer multiValueIndex,
            Integer hierarchyIndex) throws BlobNotFoundException, BlobException, InterruptedException,
            RecordNotFoundException, RecordTypeNotFoundException, FieldTypeNotFoundException, RecordException,
            VersionNotFoundException, TypeException {

        Record record = read(recordId, version, Arrays.asList(new QName[]{fieldName}));
        FieldType fieldType = typeManager.getFieldTypeByName(fieldName);
        return blobManager.getBlobAccess(record, fieldName, multiValueIndex, hierarchyIndex, fieldType);

    }


    public BlobAccess getBlob(RecordId recordId, QName fieldName) throws BlobNotFoundException, BlobException,
            InterruptedException, RecordNotFoundException, RecordTypeNotFoundException, FieldTypeNotFoundException,
            RecordException, VersionNotFoundException, TypeException {

        return getBlob(recordId, null, fieldName, null, null);

    }
}
