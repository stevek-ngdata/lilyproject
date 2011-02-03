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
package org.lilyproject.repository.impl;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;
import org.lilyproject.repository.api.Blob;
import org.lilyproject.repository.api.BlobException;
import org.lilyproject.repository.api.BlobInputStream;
import org.lilyproject.repository.api.BlobManager;
import org.lilyproject.repository.api.BlobNotFoundException;
import org.lilyproject.repository.api.BlobStoreAccess;
import org.lilyproject.repository.api.BlobStoreAccessFactory;
import org.lilyproject.util.Pair;

public class BlobStoreAccessRegistry {

    Map<String, BlobStoreAccess> registry = new HashMap<String, BlobStoreAccess>();
    private BlobStoreAccessFactory blobStoreAccessFactory;
    private final BlobManager blobManager;
    
    public BlobStoreAccessRegistry(BlobManager blobManager) {
        this.blobManager = blobManager;
    }
    
    public void register(BlobStoreAccess blobStoreAccess) {
        registry.put(blobStoreAccess.getId(), blobStoreAccess);
    }
    
    public void setBlobStoreAccessFactory(BlobStoreAccessFactory blobStoreAccessFactory) {
        this.blobStoreAccessFactory = blobStoreAccessFactory;
        for (BlobStoreAccess blobStoreAccess : blobStoreAccessFactory.getAll()) {
            register(blobStoreAccess);
        }
    }

    public OutputStream getOutputStream(Blob blob) throws BlobException {
        BlobStoreAccess blobStoreAccess = blobStoreAccessFactory.get(blob);
        return new BlobOutputStream(blobStoreAccess.getOutputStream(blob), blobStoreAccess.getId(), blob, blobManager, blobStoreAccess.incubate());
    }

    public BlobInputStream getInputStream(Blob blob) throws BlobNotFoundException, BlobException {
        Pair<String, byte[]> decodedKey = decodeKey(blob);
        BlobStoreAccess blobStoreAccess = registry.get(decodedKey.getV1());
        return new BlobInputStream(blobStoreAccess.getInputStream(decodedKey.getV2()), blob);
    }

    private Pair<String, byte[]> decodeKey(Blob blob) throws BlobNotFoundException, BlobException {
        if (blob.getValue() == null) {
            throw new BlobNotFoundException(blob, "Blob has no reference to a blob in the blobstore", null);
        }
        Pair<String, byte[]> decodedKey;
        try {
            decodedKey = decode(blob.getValue());
        } catch (Exception e) {
            throw new BlobException("Failed to decode the blobkey of the blob <" + blob + ">", e);
        }
        return decodedKey;
    }

    
    public void delete(Blob blob) throws BlobNotFoundException, BlobException {
        Pair<String, byte[]> decodedKey = decodeKey(blob);
        BlobStoreAccess blobStoreAccess = registry.get(decodedKey.getV1());
        blobStoreAccess.delete(decodedKey.getV2());
    }
    
    static private byte[] encode(String id, byte[] blobKey) {
        byte[] bytes = new byte[0];
        bytes = Bytes.add(bytes, Bytes.toBytes(id.length()));
        bytes = Bytes.add(bytes, Bytes.toBytes(id));
        bytes = Bytes.add(bytes, blobKey);
        return bytes;
    }
    
    static private Pair<String, byte[]>  decode(byte[] key) {
        int idLength = Bytes.toInt(key);
        String id = Bytes.toString(key, Bytes.SIZEOF_INT, idLength);
        byte[] blobKey = Bytes.tail(key, key.length - Bytes.SIZEOF_INT - idLength);
        return new Pair<String, byte[]>(id, blobKey);
    }
    
    private class BlobOutputStream extends FilterOutputStream {

        private final Blob blob;
        private final String blobStoreAccessId;
        private final BlobManager blobManager;
        private final boolean incubate;

        public BlobOutputStream(OutputStream outputStream, String blobStoreAccessId, Blob blob, BlobManager blobManager, boolean incubate) {
            super(outputStream);
            this.blobStoreAccessId = blobStoreAccessId;
            this.blob = blob;
            this.blobManager = blobManager;
            this.incubate = incubate;
        }

        @Override
        public void close() throws IOException {
            super.close();
            byte[] encodedBlobKey = encode(blobStoreAccessId, blob.getValue());
            if (incubate) {
                blobManager.incubateBlob(encodedBlobKey);
            }
            blob.setValue(encodedBlobKey);
        }
    }


}
