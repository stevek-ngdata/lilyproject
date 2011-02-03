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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilyproject.repository.api.Blob;
import org.lilyproject.repository.api.BlobException;
import org.lilyproject.repository.api.BlobStoreAccess;
import org.lilyproject.util.hbase.HBaseTableFactory;
import org.lilyproject.util.hbase.HBaseTableFactoryImpl;

public class HBaseBlobStoreAccess implements BlobStoreAccess {

    private static final byte[] BLOB_TABLE = Bytes.toBytes("blob");
    private static final String ID = "HBASE";
    private static final String BLOBS_COLUMN_FAMILY = "data";
    private static final byte[] BLOBS_COLUMN_FAMILY_BYTES = Bytes.toBytes(BLOBS_COLUMN_FAMILY);
    private static final byte[] BLOB_COLUMN = Bytes.toBytes("b");

    private boolean clientMode = false;
    private HTableInterface table;

    public HBaseBlobStoreAccess(Configuration hbaseConf) throws IOException {
        this(hbaseConf, false);
    }

    public HBaseBlobStoreAccess(Configuration hbaseConf, boolean clientMode) throws IOException {
        this(new HBaseTableFactoryImpl(hbaseConf), clientMode);
    }

    public HBaseBlobStoreAccess(HBaseTableFactory tableFactory) throws IOException {
        this(tableFactory, false);
    }

    public HBaseBlobStoreAccess(HBaseTableFactory tableFactory, boolean clientMode) throws IOException {
        HTableDescriptor tableDescriptor = new HTableDescriptor(BLOB_TABLE);
        tableDescriptor.addFamily(new HColumnDescriptor(BLOBS_COLUMN_FAMILY));

        table = tableFactory.getTable(tableDescriptor, !clientMode);
    }
    
    public String getId() {
        return ID;
    }
        
    public OutputStream getOutputStream(Blob blob) throws BlobException {
        UUID uuid = UUID.randomUUID();
        byte[] blobKey = Bytes.toBytes(uuid.getMostSignificantBits());
        blobKey = Bytes.add(blobKey, Bytes.toBytes(uuid.getLeastSignificantBits()));
        return new HBaseBlobOutputStream(table, blobKey, blob);
    }

    public InputStream getInputStream(byte[] blobKey) throws BlobException {
        Get get = new Get(blobKey);
        get.addColumn(BLOBS_COLUMN_FAMILY_BYTES, BLOB_COLUMN);
        Result result;
        try {
            result = table.get(get);
        } catch (IOException e) {
            throw new BlobException("Failed to open an inputstream for blobkey <" + blobKey + "> on the HBASE blobstore", e);
        }
        byte[] value = result.getValue(BLOBS_COLUMN_FAMILY_BYTES, BLOB_COLUMN);
        if (value == null) {
            throw new BlobException("Failed to open an inputstream for blobkey <" + blobKey + "> since no blob was found on the HBASE blobstore");
        }
        return new ByteArrayInputStream(value);
    }
    
    public void delete(byte[] blobKey) throws BlobException {
        Delete delete = new Delete(blobKey);
        try {
            table.delete(delete);
        } catch (IOException e) {
            throw new BlobException("Failed to delete blob with key <" + blobKey + "> from the DFS blobstore", e);
        }
    }
    
    public boolean incubate() {
        return true;
    }

    private class HBaseBlobOutputStream extends ByteArrayOutputStream {
        
        private final HTableInterface blobTable;
        private final byte[] blobKey;
        private final Blob blob;
        public HBaseBlobOutputStream(HTableInterface table, byte[] blobKey, Blob blob) {
            super();
            blobTable = table;
            this.blobKey = blobKey;
            this.blob = blob;
        }
        @Override
        public void close() throws IOException {
            super.close();
            byte[] bytes = toByteArray();
            Put put = new Put(blobKey);
            put.add(BLOBS_COLUMN_FAMILY_BYTES, BLOB_COLUMN, bytes);
            blobTable.put(put);
            blob.setValue(blobKey);
        }
    }
}
