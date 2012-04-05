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

    @Override
    public Blob getBlob() {
        return blob;
    }

    @Override
    public InputStream getInputStream() throws BlobException {
        return blobStoreAccess.getInputStream(blobKey);
    }
}
