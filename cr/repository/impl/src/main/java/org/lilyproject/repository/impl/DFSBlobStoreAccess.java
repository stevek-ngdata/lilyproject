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
import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilyproject.repository.api.Blob;
import org.lilyproject.repository.api.BlobException;
import org.lilyproject.repository.api.BlobStoreAccess;

public class DFSBlobStoreAccess implements BlobStoreAccess {

    private static final String ID = "HDFS";
    
    private final FileSystem fileSystem;
    private final Path rootDir;

    public DFSBlobStoreAccess(FileSystem fileSystem, Path rootDir) throws IOException {
        this.fileSystem = fileSystem;
        this.rootDir = rootDir;

        if (!fileSystem.exists(rootDir)) {
            fileSystem.mkdirs(rootDir);
        }
    }
    
    public String getId() {
        return ID;
    }
        
    public OutputStream getOutputStream(Blob blob) throws BlobException {
        UUID uuid = UUID.randomUUID();
        byte[] blobKey = Bytes.toBytes(uuid.getMostSignificantBits());
        blobKey = Bytes.add(blobKey, Bytes.toBytes(uuid.getLeastSignificantBits()));
        FSDataOutputStream fsDataOutputStream;
        try {
            fsDataOutputStream = fileSystem.create(createPath(uuid));
        } catch (IOException e) {
            throw new BlobException("Failed to open an outputstream for blob <" +blob+ "> on the DFS blobstore", e);
        }
        return new DFSBlobOutputStream(fsDataOutputStream, blobKey, blob);
    }


    public InputStream getInputStream(byte[] blobKey) throws BlobException {
        UUID uuid = decode(blobKey);
        try {
            return fileSystem.open(createPath(uuid));
        } catch (IOException e) {
            throw new BlobException("Failed to open an inputstream for blobkey <"+ blobKey+"> on the DFS blobstore", e);
        }
    }

    private Path createPath(UUID uuid) {
        String fileName = uuid.toString();
        String dirLevel1 = fileName.substring(0, 2);
        String dirLevel2 = fileName.substring(2, 4);
        Path path = new Path(rootDir, dirLevel1);
        path = new Path(path, dirLevel2);
        path = new Path(path, fileName);
        return path;
    }

    public void delete(byte[] blobKey) throws BlobException {
        UUID uuid = decode(blobKey);
        try {
            fileSystem.delete(createPath(uuid), false);
        } catch (IOException e) {
            throw new BlobException("Failed to delete blob with key <" +blobKey+ "> from the DFS blobstore", e);
        }
    }

    public boolean incubate() {
        return true;
    }

    private UUID decode(byte[] blobKey) {
        return new UUID(Bytes.toLong(blobKey), Bytes.toLong(blobKey, Bytes.SIZEOF_LONG));
    }
    
    private class DFSBlobOutputStream extends FilterOutputStream {
        
        private final byte[] blobKey;
        private final Blob blob;
        public DFSBlobOutputStream(OutputStream outputStream, byte[] blobKey, Blob blob) {
            super(outputStream);
            this.blobKey = blobKey;
            this.blob = blob;
        }
        @Override
        public void close() throws IOException {
            super.close();
            blob.setValue(blobKey);
        }
    }
    
}
