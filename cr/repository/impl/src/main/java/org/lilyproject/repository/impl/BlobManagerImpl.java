package org.lilyproject.repository.impl;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.lilyproject.repository.api.*;
import org.lilyproject.repository.impl.valuetype.*;
import org.lilyproject.util.hbase.HBaseTableFactory;
import org.lilyproject.util.hbase.LilyHBaseSchema;
import org.lilyproject.util.hbase.LilyHBaseSchema.BlobIncubatorCf;
import org.lilyproject.util.hbase.LilyHBaseSchema.BlobIncubatorColumn;

public class BlobManagerImpl implements BlobManager {
    private Log log = LogFactory.getLog(getClass());
    
    protected static final byte[] INCUBATE = new byte[]{(byte)-1};
    private HTableInterface blobIncubatorTable;

    private final BlobStoreAccessFactory factory;

    private BlobStoreAccessRegistry registry;

    public BlobManagerImpl(HBaseTableFactory hbaseTableFactory, BlobStoreAccessFactory blobStoreAccessFactory, boolean clientMode) throws IOException {
        this.factory = blobStoreAccessFactory;
        blobIncubatorTable = LilyHBaseSchema.getBlobIncubatorTable(hbaseTableFactory, clientMode);
        registry = new BlobStoreAccessRegistry(this);
        registry.setBlobStoreAccessFactory(blobStoreAccessFactory);
    }
    
    public void register(BlobStoreAccess blobStoreAccess) {
        registry.register(blobStoreAccess);
    }
    
    public OutputStream getOutputStream(Blob blob) throws BlobException {
        return registry.getOutputStream(blob);
    }

    public BlobAccess getBlobAccess(Record record, QName fieldName, Integer multivalueIndex, Integer hierarchyIndex,
            FieldType fieldType) throws BlobNotFoundException, BlobException {
        return getBlobAccess(record, fieldName, fieldType, multivalueIndex, hierarchyIndex);
    }
    
    public BlobAccess getBlobAccess(Record record, QName fieldName, FieldType fieldType, Integer...indexes) throws BlobNotFoundException, BlobException {
        if (!(fieldType.getValueType().getBaseValueType() instanceof BlobValueType)) {
            throw new BlobException("Cannot read a blob from a non-blob field type: " + fieldType.getName());
        }

        Blob blob = getBlobFromRecord(record, fieldName, fieldType, indexes);
        return registry.getBlobAccess(blob);
    }

    public void incubateBlob(byte[] blobKey) throws IOException {
        Put put = new Put(blobKey);
        // We put a byte[] because we need to put at least one column 
        // and that column needs to be non-empty for the checkAndPut in reserveBlob() to work
        put.add(BlobIncubatorCf.REF.bytes, BlobIncubatorColumn.RECORD.bytes, INCUBATE); 
        blobIncubatorTable.put(put);
    }
    
    public Set<BlobReference> reserveBlobs(Set<BlobReference> blobs) throws IOException {
        Set<BlobReference> failedBlobs = new HashSet<BlobReference>();
        for (BlobReference referencedBlob : blobs) {
                if (!reserveBlob(referencedBlob))
                    failedBlobs.add(referencedBlob);
        }
        return failedBlobs;
    }
    
    private boolean reserveBlob(BlobReference referencedBlob) throws IOException {
        BlobStoreAccess blobStoreAccess = factory.get(referencedBlob.getBlob());
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
    
    public void handleBlobReferences(RecordId recordId, Set<BlobReference> referencedBlobs, Set<BlobReference> unReferencedBlobs) {
        // Remove references from the blobIncubator for the blobs that are still referenced.
        if (referencedBlobs != null) {
            try {
                for (BlobReference blobReference : referencedBlobs) {
                    blobIncubatorTable.delete(new Delete(blobReference.getBlob().getValue()));
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
    
    private Blob getBlobFromRecord(Record record, QName fieldName, FieldType fieldType, Integer...indexes) throws BlobNotFoundException {
        Object value = record.getField(fieldName);
        ValueType valueType = fieldType.getValueType();
        if (!valueType.getBaseValueType().getSimpleName().equals("BLOB"))
            throw new BlobNotFoundException("Blob could not be retrieved from '" + record.getId() + "', '" + fieldName + "' at index: " + indexes);
        for (Integer index : indexes) {
            try {
                if (valueType.getSimpleName().equals("LIST")) {
                    value = ((List<Object>) value).get(index);
                    valueType = valueType.getNestedValueType();
                    continue;
                } 
                if (valueType.getSimpleName().equals("PATH")) {
                    value = ((HierarchyPath)value).getElements()[index];
                    valueType = valueType.getNestedValueType();
                    continue;
                }
                throw new BlobNotFoundException("Invalid index to retrieve Blob from '" + record.getId() + "', '" + fieldName + "' : " + indexes);
            } catch (IndexOutOfBoundsException e) {
                throw new BlobNotFoundException("Invalid index to retrieve Blob from '" + record.getId() + "', '" + fieldName + "' : " + indexes, e);
            }
        }
        if (!valueType.getSimpleName().equals("BLOB"))
            throw new BlobNotFoundException("Blob could not be retrieved from '" + record.getId() + "', '" + fieldName + "' at index: " + indexes);
        return (Blob)value;
    }
    
    public void delete(byte[] blobKey) throws BlobException {
        registry.delete(blobKey);
    }
}
