/*
 * Copyright 2010 Outerthought bvba
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
package org.lilycms.repository.impl;

import java.io.IOException;

import org.lilycms.repository.api.Blob;
import org.lilycms.repository.api.BlobStoreAccess;
import org.lilycms.repository.api.BlobStoreAccessFactory;

public class SizeBasedBlobOutputStreamFactory implements BlobStoreAccessFactory {


    private final long limit;
    private final BlobStoreAccess smallBlobStoreAccess;
    private final BlobStoreAccess largeBlobStoreAccess;

    public SizeBasedBlobOutputStreamFactory(long limit, BlobStoreAccess smallBlobStoreAccess, BlobStoreAccess largeBlobStoreAccess) {
        this.limit = limit;
        this.smallBlobStoreAccess = smallBlobStoreAccess;
        this.largeBlobStoreAccess = largeBlobStoreAccess;
    }
    
    public BlobStoreAccess getBlobStoreAccess(Blob blob) throws IOException {
        if (blob.getSize() == null || blob.getSize() > limit) {
            return largeBlobStoreAccess;
        } else {
            return smallBlobStoreAccess;
        }
    }
}
