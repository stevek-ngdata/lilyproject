package org.lilyproject.repository.api;

import java.io.InputStream;

/**
 * Provides access to both the metadata of a blob and its InputStream.
 */
public interface BlobAccess {
    Blob getBlob();

    /**
     * The InputStream is only opened when this method is called.
     */
    InputStream getInputStream() throws BlobException;
}
