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

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.primitives.Ints;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.lilyproject.repository.api.Blob;
import org.lilyproject.repository.api.BlobAccess;
import org.lilyproject.repository.api.BlobException;
import org.lilyproject.repository.api.BlobManager;
import org.lilyproject.repository.api.BlobNotFoundException;
import org.lilyproject.repository.api.BlobReference;
import org.lilyproject.repository.api.BlobStoreAccess;
import org.lilyproject.repository.api.BlobStoreAccessFactory;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.HierarchyPath;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.ValueType;
import org.lilyproject.repository.impl.valuetype.BlobValueType;
import org.lilyproject.util.hbase.HBaseTableFactory;
import org.lilyproject.util.hbase.LilyHBaseSchema;
import org.lilyproject.util.hbase.LilyHBaseSchema.BlobIncubatorCf;
import org.lilyproject.util.hbase.LilyHBaseSchema.BlobIncubatorColumn;

public class BlobManagerImpl implements BlobManager {
    private Log log = LogFactory.getLog(getClass());

    protected static final byte[] INCUBATE = new byte[]{(byte)-1};
    private HTableInterface blobIncubatorTable;

    private BlobStoreAccessRegistry registry;

    public BlobManagerImpl(HBaseTableFactory hbaseTableFactory, BlobStoreAccessFactory blobStoreAccessFactory, boolean clientMode) throws IOException, InterruptedException {
        blobIncubatorTable = LilyHBaseSchema.getBlobIncubatorTable(hbaseTableFactory, clientMode);
        registry = new BlobStoreAccessRegistry(this);
        registry.setBlobStoreAccessFactory(blobStoreAccessFactory);
    }

    @Override
    public void register(BlobStoreAccess blobStoreAccess) {
        registry.register(blobStoreAccess);
    }

    @Override
    public OutputStream getOutputStream(Blob blob) throws BlobException {
        return registry.getOutputStream(blob);
    }

    @Override
    public BlobAccess getBlobAccess(Record record, QName fieldName, FieldType fieldType, int...indexes)
            throws BlobException {
        if (!(fieldType.getValueType().getDeepestValueType() instanceof BlobValueType)) {
            throw new BlobException("Cannot read a blob from a non-blob field type: " + fieldType.getName());
        }

        Blob blob = getBlobFromRecord(record, fieldName, fieldType, indexes);
        return registry.getBlobAccess(blob);
    }

    @Override
    public void incubateBlob(byte[] blobKey) throws IOException {
        Put put = new Put(blobKey);
        // We put a byte[] because we need to put at least one column
        // and that column needs to be non-empty for the checkAndPut in reserveBlob() to work
        put.add(BlobIncubatorCf.REF.bytes, BlobIncubatorColumn.RECORD.bytes, INCUBATE);
        blobIncubatorTable.put(put);
    }

    @Override
    public Set<BlobReference> reserveBlobs(Set<BlobReference> blobs) throws IOException {
        Set<BlobReference> failedBlobs = new HashSet<BlobReference>();
        for (BlobReference referencedBlob : blobs) {
            try {
                if (!reserveBlob(referencedBlob))
                    failedBlobs.add(referencedBlob);
            } catch (BlobNotFoundException bnfe) {
                failedBlobs.add(referencedBlob);
            } catch (BlobException be) {
                failedBlobs.add(referencedBlob);
            }
        }
        return failedBlobs;
    }

    private boolean reserveBlob(BlobReference referencedBlob)  throws BlobNotFoundException, BlobException, IOException {
        BlobStoreAccess blobStoreAccess = registry.getBlobStoreAccess(referencedBlob.getBlob());

        // Inline blobs are not incubated and therefore reserving them always succeeds
        if (!blobStoreAccess.incubate()) {
            return true;
        }
        byte[] row = referencedBlob.getBlob().getValue();
        byte[] family = BlobIncubatorCf.REF.bytes;
        byte[] recordQualifier = BlobIncubatorColumn.RECORD.bytes;
        byte[] fieldQualifier = BlobIncubatorColumn.FIELD.bytes;
        Put put = new Put(row);
        put.add(family, recordQualifier, referencedBlob.getRecordId().toBytes());
        put.add(family, fieldQualifier, referencedBlob.getFieldType().getId().getBytes());
        return blobIncubatorTable.checkAndPut(row, family, recordQualifier, INCUBATE, put);
    }

    @Override
    public void handleBlobReferences(RecordId recordId, Set<BlobReference> referencedBlobs, Set<BlobReference> unReferencedBlobs) {
        // Remove references from the blobIncubator for the blobs that are still referenced.
        if (referencedBlobs != null) {
            try {
                for (BlobReference blobReference : referencedBlobs) {
                    try {
                        BlobStoreAccess blobStoreAccess = registry.getBlobStoreAccess(blobReference.getBlob());
                        // Only delete from the blobIncubatorTable if incubation applies
                        if (blobStoreAccess.incubate()) {
                            blobIncubatorTable.delete(new Delete(blobReference.getBlob().getValue()));
                        }
                    } catch (BlobNotFoundException bnfe) {
                        // TODO
                    } catch (BlobException be) {
                        // TODO
                    }
                }
            } catch (IOException e) {
                // We do a best effort to remove the blobs from the blobIncubator
                // If it fails a background cleanup process will notice this later and clean it up
                log.info("Failed to remove blobs from the blobIncubator for record '" + recordId + "'", e);
            }
        }

        // Remove blobs that are no longer referenced.
        if (unReferencedBlobs != null) {
            Set<Blob> blobsToDelete = new HashSet<Blob>();
            for (BlobReference blobReference : unReferencedBlobs) {
                blobsToDelete.add(blobReference.getBlob());
            }
            for (Blob blobToDelete : blobsToDelete) {
                // We do a best effort to delete the blobs from the blobstore
                // If it fails, the blob will never be cleaned up.
                try {
                    registry.delete(blobToDelete);
                } catch (BlobException e) {
                    log.warn("Failed to remove blobs from the blobstore for record '" + recordId + "'", e);
                }
            }
        }
    }

    private Blob getBlobFromRecord(Record record, QName fieldName, FieldType fieldType, int... indexes)
            throws BlobNotFoundException {
        Object value = record.getField(fieldName);
        ValueType valueType = fieldType.getValueType();
        if (!valueType.getDeepestValueType().getBaseName().equals("BLOB")) {
            throw new BlobNotFoundException("Blob could not be retrieved from '" + record.getId() + "', '" +
                    fieldName + "' at index: " + Ints.join("/", indexes));
        }

        if (indexes == null) {
            indexes = new int[0];
        }

        for (int i = 0; i < indexes.length; i++) {
            int index = indexes[i];
            try {
                if (valueType.getBaseName().equals("LIST")) {
                    value = ((List<Object>) value).get(index);
                    valueType = valueType.getNestedValueType();
                    continue;
                }
                if (valueType.getBaseName().equals("PATH")) {
                    value = ((HierarchyPath)value).getElements()[index];
                    valueType = valueType.getNestedValueType();
                    continue;
                }
                throw new BlobNotFoundException("Invalid index to retrieve Blob from '" + record.getId() +
                        "', '" + fieldName + "' : " + Ints.join("/", Arrays.copyOf(indexes, i + 1)));
            } catch (IndexOutOfBoundsException e) {
                throw new BlobNotFoundException("Invalid index to retrieve Blob from '" + record.getId() +
                        "', '" + fieldName + "' : " + Ints.join("/", Arrays.copyOf(indexes, i + 1)), e);
            }
        }
        if (!valueType.getBaseName().equals("BLOB")) {
            throw new BlobNotFoundException("Blob could not be retrieved from '" + record.getId() +
                    "', '" + fieldName + "' at index: " + Ints.join("/", indexes));
        }
        return (Blob)value;
    }

    @Override
    public void delete(byte[] blobKey) throws BlobException {
        registry.delete(blobKey);
    }
}
