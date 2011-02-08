package org.lilyproject.repository.impl;

import org.lilyproject.repository.api.Blob;
import org.lilyproject.repository.api.BlobAccess;
import org.lilyproject.repository.api.BlobException;
import org.lilyproject.repository.api.BlobStoreAccess;

import java.io.InputStream;

public class BlobAccessImpl implements BlobAccess {
    private Blob blob;
    private BlobStoreAccess blobStoreAccess;
    private byte[] blobKey;

    public BlobAccessImpl(Blob blob, BlobStoreAccess blobStoreAccess, byte[] blobKey) {
        this.blob = blob;
        this.blobStoreAccess = blobStoreAccess;
        this.blobKey = blobKey;
    }

    public Blob getBlob() {
        return blob;
    }

    public InputStream getInputStream() throws BlobException {
        return blobStoreAccess.getInputStream(blobKey);
    }
}
