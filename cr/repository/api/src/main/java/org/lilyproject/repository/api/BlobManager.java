package org.lilyproject.repository.api;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Set;

public interface BlobManager {

    /**
     * After uploading a blob to the blobstore, calling this incubateBlob method puts a reference 
     * to the blob in the BlobIncubationTable. Blobs can only be used in a create or update operation 
     * of a record if it has a reference in this table or it the blob was already used in another version
     * of the same field of the same record.
     * If the blob is used in a record its reference is removed from the BlobIncubatorTable so that it 
     * cannot be used by any other record.
     * If the blob is not used within a defined timeout, the reference will also be removed, and the 
     * related blob will be deleted.
     * @param blobKey the key of the blob to be incubated
     * @throws IOException
     */
    void incubateBlob(byte[] blobKey) throws IOException;

    /**
     * When a blob is about to be used in a record create or update operation, it will first be reserved 
     * to avoid that concurrent create or update operations would use the same blob.
     * If a reference to the blob was present in the BlobIncubatorTable this reference will be updated
     * to indicate that it is going to be used in a record.
     * If no reference was present in the BlobIncubatorTable, it is checked if the blob was already used
     * in another version of the same field of the record. If so, the reservation is regarded as successful.
     * @param blobs a set of blobs to be reserved
     * @return a set of blobs for which the reservation failed
     * @throws IOException
     */
    Set<BlobReference> reserveBlobs(Set<BlobReference> blobs) throws IOException;

    /**
     * After a blob has been used in a successful record create or update operation its reservation can
     * be removed. Calling this method removes all given blob references from the BlobIncubatorTable.  
     * @param blobs the blobs for which to remove the references from the BlobIncubatorTable
     * @throws IOException
     */
    void handleBlobReferences(RecordId recordId, Set<BlobReference> referencedBlobs,
            Set<BlobReference> unReferencedBlobs);

    /**
     * Returns an OutputStream to which a blob can be uploaded.
     * After the OutputStream has been closed, a reference is put in the BlobIncubatorTable.
     * The blob is then also updated with information that allows the BlobManager the retrieve 
     * the blob from where it is stored in the blobstore.
     * The blob can then be used in a record create or update operation. 
     * @param blob the blob to be uploaded
     * @return an OutputStream
     * @throws BlobException
     */
    OutputStream getOutputStream(Blob blob) throws BlobException;
    
    /**
     * Returns a BlobInputStream based on the given location of a blob in a record.
     * @param record the record containing the blob
     * @param fieldName the name of the field containing the blob
     * @param multivalueIndex the index of the blob in a multivalue field, can be null 
     * @param hierarchyIndex the index of the blob in a  hierarchical field, can be null
     * @param fieldType the fieldType of the field
     * @return a BlobInputStream
     * @throws BlobNotFoundException thrown when no blob can be found at the given location
     * @throws BlobException thrown when no InputStream can be opened on the blob
     */
    BlobInputStream getInputStream(Record record, QName fieldName, Integer multivalueIndex, Integer hierarchyIndex,
            FieldType fieldType) throws BlobNotFoundException, BlobException;

    /**
     * The BlobManager manages the BlobStoreAccess functionality for the Repository. 
     * A {@link BlobStoreAccess} must be registered with the repository before
     * it can be used. Any BlobStoreAccess that has ever been used to store
     * binary data of a blob must be registered before that data can be
     * retrieved again.
     */
    void register(BlobStoreAccess blobStoreAccess);

}
