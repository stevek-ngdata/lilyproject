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

import static org.lilyproject.util.hbase.LilyHBaseSchema.DELETE_MARKER;
import static org.lilyproject.util.hbase.LilyHBaseSchema.EXISTS_FLAG;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilyproject.bytes.impl.DataInputImpl;
import org.lilyproject.hbaseext.ContainsValueComparator;
import org.lilyproject.repository.api.*;
import org.lilyproject.repository.impl.RepositoryMetrics.Action;
import org.lilyproject.repository.impl.primitivevaluetype.BlobValueType;
import org.lilyproject.rowlock.RowLock;
import org.lilyproject.rowlock.RowLocker;
import org.lilyproject.rowlog.api.RowLog;
import org.lilyproject.rowlog.api.RowLogException;
import org.lilyproject.rowlog.api.RowLogMessage;
import org.lilyproject.util.ArgumentValidator;
import org.lilyproject.util.Pair;
import org.lilyproject.util.hbase.HBaseTableFactory;
import org.lilyproject.util.hbase.LilyHBaseSchema;
import org.lilyproject.util.hbase.LilyHBaseSchema.RecordCf;
import org.lilyproject.util.hbase.LilyHBaseSchema.RecordColumn;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.repo.RecordEvent;
import org.lilyproject.util.repo.VersionTag;
import org.lilyproject.util.repo.RecordEvent.Type;

/**
 * Repository implementation.
 * 
 */
public class HBaseRepository extends BaseRepository {
 
    private HTableInterface recordTable;
    private final IdGenerator idGenerator;
    private Map<Scope, byte[]> recordTypeIdColumnNames = new HashMap<Scope, byte[]>();
    private Map<Scope, byte[]> recordTypeVersionColumnNames = new HashMap<Scope, byte[]>();
    private RowLog wal;
    private RowLocker rowLocker;
    
    private Log log = LogFactory.getLog(getClass());
    private RepositoryMetrics metrics;

    public HBaseRepository(TypeManager typeManager, IdGenerator idGenerator,
            RowLog wal, Configuration configuration, HBaseTableFactory hbaseTableFactory, BlobManager blobManager) throws IOException {
        super(typeManager, blobManager);

        this.idGenerator = idGenerator;
        this.wal = wal;

        recordTable = LilyHBaseSchema.getRecordTable(hbaseTableFactory);

        recordTypeIdColumnNames.put(Scope.NON_VERSIONED, RecordColumn.NON_VERSIONED_RT_ID.bytes);
        recordTypeIdColumnNames.put(Scope.VERSIONED, RecordColumn.VERSIONED_RT_ID.bytes);
        recordTypeIdColumnNames.put(Scope.VERSIONED_MUTABLE, RecordColumn.VERSIONED_MUTABLE_RT_ID.bytes);
        recordTypeVersionColumnNames.put(Scope.NON_VERSIONED, RecordColumn.NON_VERSIONED_RT_VERSION.bytes);
        recordTypeVersionColumnNames.put(Scope.VERSIONED, RecordColumn.VERSIONED_RT_VERSION.bytes);
        recordTypeVersionColumnNames.put(Scope.VERSIONED_MUTABLE, RecordColumn.VERSIONED_MUTABLE_RT_VERSION.bytes);

        rowLocker = new RowLocker(recordTable, RecordCf.DATA.bytes, RecordColumn.LOCK.bytes, 10000);
        metrics = new RepositoryMetrics("hbaserepository");
    }

    public void close() throws IOException {
    }

    public IdGenerator getIdGenerator() {
        return idGenerator;
    }

    public Record createOrUpdate(Record record) throws FieldTypeNotFoundException, RecordException,
            RecordTypeNotFoundException, InvalidRecordException, TypeException,
            VersionNotFoundException, RecordLockedException, WalProcessingException {
        return createOrUpdate(record, true);
    }

    public Record createOrUpdate(Record record, boolean useLatestRecordType) throws FieldTypeNotFoundException,
            RecordException, RecordTypeNotFoundException, InvalidRecordException, TypeException,
            VersionNotFoundException, RecordLockedException, WalProcessingException {

        if (record.getId() == null) {
            // While we could generate an ID ourselves in this case, this would defeat partly the purpose of
            // createOrUpdate, which is that clients would be able to retry the operation (in case of IO exceptions)
            // without having to worry that more than one record might be created.
            throw new RecordException("Record ID is mandatory when using create-or-update.");
        }

        byte[] rowId = record.getId().toBytes();
        Get get = new Get(rowId);
        get.addColumn(RecordCf.DATA.bytes, RecordColumn.DELETED.bytes);

        int attempts;
        
        for (attempts = 0; attempts < 3; attempts++) {            
            Result result;
            try {
                result = recordTable.get(get);
            } catch (IOException e) {
                throw new RecordException("Error reading record row for record id " + record.getId());
            }

            byte[] deleted = result.getValue(RecordCf.DATA.bytes, RecordColumn.DELETED.bytes);
            if ((deleted == null) || (Bytes.toBoolean(deleted))) {
                // do the create
                try {
                    Record createdRecord = create(record);
                    return createdRecord;
                } catch (RecordExistsException e) {
                     // someone created the record since we checked, we will try again
                }
            } else {
                // do the update
                try {
                    record = update(record, false, useLatestRecordType);
                    return record;
                } catch (RecordNotFoundException e) {
                    // some deleted the record since we checked, we will try again
                }
            }
        }

        throw new RecordException("Create-or-update failed after " + attempts +
                " attempts, toggling between create and update mode.");
    }

    public Record create(Record record) throws RecordExistsException, InvalidRecordException,
            RecordTypeNotFoundException, FieldTypeNotFoundException, RecordException, TypeException,
            RecordLockedException {

        long before = System.currentTimeMillis();
        try {
            checkCreatePreconditions(record);
            
            Record newRecord = record.clone();
    
            RecordId recordId = newRecord.getId();
            if (recordId == null) {
                recordId = idGenerator.newRecordId();
                newRecord.setId(recordId);
            }
    
            byte[] rowId = recordId.toBytes();
            RowLock rowLock = null;

            try {
                FieldTypes fieldTypes= typeManager.getFieldTypesSnapshot();
                
                // Lock the row
                rowLock = lockRow(recordId);

                long version = 1L;
                // If the record existed it would have been deleted.
                // The version numbering continues from where it has been deleted.
                Get get = new Get(rowId);
                get.addColumn(RecordCf.DATA.bytes, RecordColumn.DELETED.bytes);
                get.addColumn(RecordCf.DATA.bytes, RecordColumn.VERSION.bytes);
                Result result = recordTable.get(get);
                if (!result.isEmpty()) {
                    // If the record existed it should have been deleted
                    byte[] recordDeleted = result.getValue(RecordCf.DATA.bytes, RecordColumn.DELETED.bytes);
                    if (recordDeleted != null && !Bytes.toBoolean(recordDeleted)) {
                        throw new RecordExistsException(recordId);
                    }
                    byte[] oldVersion = result.getValue(RecordCf.DATA.bytes, RecordColumn.VERSION.bytes);
                    if (oldVersion != null) {
                        version = Bytes.toLong(oldVersion) + 1;
                        // Make sure any old data gets cleared and old blobs are deleted
                        // This is to cover the failure scenario where a record was deleted, but a failure
                        // occured before executing the clearData
                        // If this was already done, this is a no-op
                        clearData(recordId);
                    }
                }
                
                Record dummyOriginalRecord = newRecord();
                Put put = new Put(newRecord.getId().toBytes());
                put.add(RecordCf.DATA.bytes, RecordColumn.DELETED.bytes, 1L, Bytes.toBytes(false));
                RecordEvent recordEvent = new RecordEvent();
                recordEvent.setType(Type.CREATE);
                Set<BlobReference> referencedBlobs = new HashSet<BlobReference>();
                Set<BlobReference> unReferencedBlobs = new HashSet<BlobReference>();
                
                calculateRecordChanges(newRecord, dummyOriginalRecord, version, put, recordEvent, referencedBlobs, unReferencedBlobs, false, fieldTypes);

                // Make sure the record type changed flag stays false for a newly
                // created record
                recordEvent.setRecordTypeChanged(false);
                Long newVersion = newRecord.getVersion();
                if (newVersion != null)
                    recordEvent.setVersionCreated(newVersion);

                // Reserve blobs so no other records can use them
                reserveBlobs(null, referencedBlobs);

                putRowWithWalProcessing(recordId, rowLock, put, recordEvent);

                // Remove the used blobs from the blobIncubator
                blobManager.handleBlobReferences(recordId, referencedBlobs, unReferencedBlobs);
                
            } catch (IOException e) {
                throw new RecordException("Exception occurred while creating record <" + recordId + "> in HBase table",
                        e);
            } catch (RowLogException e) {
                throw new RecordException("Exception occurred while creating record <" + recordId + "> in HBase table",
                        e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RecordException("Exception occurred while creating record <" + recordId + "> in HBase table",
                        e);
            } catch (BlobException e) {
                throw new RecordException("Exception occurred while creating record <" + recordId + ">",
                        e);
            } finally {
                unlockRow(rowLock);
            }

            newRecord.setResponseStatus(ResponseStatus.CREATED);
            newRecord.getFieldsToDelete().clear();
            return newRecord;
        } finally {
            metrics.report(Action.CREATE, System.currentTimeMillis() - before);
        }
    }

    private void checkCreatePreconditions(Record record) throws InvalidRecordException {
        ArgumentValidator.notNull(record, "record");
        if (record.getRecordTypeName() == null) {
            throw new InvalidRecordException("The recordType cannot be null for a record to be created.", record.getId());
        }
        if (record.getFields().isEmpty()) {
            throw new InvalidRecordException("Creating an empty record is not allowed", record.getId());
        }
    }

    public Record update(Record record) throws InvalidRecordException, RecordNotFoundException,
    RecordTypeNotFoundException, FieldTypeNotFoundException, RecordException, VersionNotFoundException,
    TypeException, RecordLockedException, WalProcessingException {
        return update(record, false, true);
    }

    public Record update(Record record, boolean updateVersion, boolean useLatestRecordType)
            throws InvalidRecordException, RecordNotFoundException, RecordTypeNotFoundException,
            FieldTypeNotFoundException, RecordException, VersionNotFoundException, TypeException,
            RecordLockedException, WalProcessingException {

        long before = System.currentTimeMillis();
        RecordId recordId = record.getId();
        RowLock rowLock = null;
        try {
            if (recordId == null) {
                throw new InvalidRecordException("The recordId cannot be null for a record to be updated.", record.getId());
            }
            
            FieldTypes fieldTypes = typeManager.getFieldTypesSnapshot();
            
            // Take Custom Lock
            rowLock = lockRow(recordId);

            checkAndProcessOpenMessages(record.getId(), rowLock);
            
            // Check if the update is an update of mutable fields
            if (updateVersion) {
                try {
                    return updateMutableFields(record, useLatestRecordType, rowLock, fieldTypes);
                } catch (BlobException e) {
                    throw new RecordException("Exception occurred while updating record <" + record.getId() + ">",
                            e);
                }
            } else {
                return updateRecord(record, useLatestRecordType, rowLock, fieldTypes);
            }
        } catch (IOException e) {
            throw new RecordException("Exception occurred while updating record <" + recordId + "> on HBase table",
                    e); 
        } finally {
            unlockRow(rowLock);
            metrics.report(Action.UPDATE, System.currentTimeMillis() - before);
        }
    }
    
    
    private Record updateRecord(Record record, boolean useLatestRecordType, RowLock rowLock, FieldTypes fieldTypes) throws RecordNotFoundException,
            InvalidRecordException, RecordTypeNotFoundException, FieldTypeNotFoundException, RecordException,
            VersionNotFoundException, TypeException, RecordLockedException {
        Record newRecord = record.clone();

        RecordId recordId = record.getId();
        try {
            Record originalRecord = read(newRecord.getId(), null, null, new ReadContext(), fieldTypes);

            Put put = new Put(newRecord.getId().toBytes());
            Set<BlobReference> referencedBlobs = new HashSet<BlobReference>();
            Set<BlobReference> unReferencedBlobs = new HashSet<BlobReference>();
            RecordEvent recordEvent = new RecordEvent();
            recordEvent.setType(Type.UPDATE);
            long newVersion = originalRecord.getVersion() == null ? 1 : originalRecord.getVersion() + 1;
                
                if (calculateRecordChanges(newRecord, originalRecord, newVersion, put, recordEvent, referencedBlobs, unReferencedBlobs, useLatestRecordType, fieldTypes)) {
                    // Reserve blobs so no other records can use them
                    reserveBlobs(record.getId(), referencedBlobs);
                    putRowWithWalProcessing(recordId, rowLock, put, recordEvent);
                    // Remove the used blobs from the blobIncubator and delete unreferenced blobs from the blobstore
                    blobManager.handleBlobReferences(recordId, referencedBlobs, unReferencedBlobs);
                    newRecord.setResponseStatus(ResponseStatus.UPDATED);
                } else {
                    newRecord.setResponseStatus(ResponseStatus.UP_TO_DATE);
                }
        } catch (RowLogException e) {
            throw new RecordException("Exception occurred while putting updated record <" + recordId
                    + "> on HBase table", e);

        } catch (IOException e) {
            throw new RecordException("Exception occurred while updating record <" + recordId + "> on HBase table",
                    e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RecordException("Exception occurred while updating record <" + recordId + "> on HBase table",
                    e);
        } catch (BlobException e) {
            throw new RecordException("Exception occurred while putting updated record <" + recordId
                    + "> on HBase table", e);
        }

        newRecord.getFieldsToDelete().clear();
        return newRecord;
    }

    // This method takes a put object containing the row's data to be updated
    // A wal message is added to this put object
    // The rowLocker is asked to put the data and message on the record table using the given rowlock
    // Finally the wal is asked to process the message
    private void putRowWithWalProcessing(RecordId recordId, RowLock rowLock,
            Put put, RecordEvent recordEvent) throws InterruptedException, RowLogException, IOException, RecordException {
        RowLogMessage walMessage;
        walMessage = wal.putMessage(recordId.toBytes(), null, recordEvent.toJsonBytes(), put);
        if (!rowLocker.put(put, rowLock)) {
            throw new RecordException("Exception occurred while putting updated record <" + recordId
                    + "> on HBase table", null);
        }

        if (walMessage != null) {
            try {
                wal.processMessage(walMessage, rowLock);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Processing message '" + walMessage + "' by the WAL got interrupted. It will be retried later.", e);
            } catch (RowLogException e) {
                log.warn("Exception while processing message '" + walMessage + "' by the WAL. It will be retried later.", e);
            }
        }
    }

    // Calculates the changes that are to be made on the record-row and puts
    // this information on the Put object and the RecordEvent
    private boolean calculateRecordChanges(Record record, Record originalRecord, Long version, Put put,
            RecordEvent recordEvent, Set<BlobReference> referencedBlobs, Set<BlobReference> unReferencedBlobs, boolean useLatestRecordType, FieldTypes fieldTypes) throws InterruptedException, RecordTypeNotFoundException, TypeException, FieldTypeNotFoundException, BlobException, RecordException, InvalidRecordException {
        QName recordTypeName = record.getRecordTypeName();
        Long recordTypeVersion = null;
        if (recordTypeName == null) {
            recordTypeName = originalRecord.getRecordTypeName();
        } else {
            recordTypeVersion = useLatestRecordType ? null : record.getRecordTypeVersion();
        }

        RecordType recordType = typeManager.getRecordTypeByName(recordTypeName, recordTypeVersion);
        
        // Check which fields have changed
        Set<Scope> changedScopes = calculateChangedFields(record, originalRecord, recordType, version, put, recordEvent, referencedBlobs, unReferencedBlobs, fieldTypes);

        // If no versioned fields have changed, keep the original version
        boolean versionedFieldsHaveChanged = changedScopes.contains(Scope.VERSIONED)
                || changedScopes.contains(Scope.VERSIONED_MUTABLE);
        if (!versionedFieldsHaveChanged) {
            version = originalRecord.getVersion();
        }

        boolean fieldsHaveChanged = !changedScopes.isEmpty();
        if (fieldsHaveChanged) {
         // The provided recordTypeVersion could have been null, so the latest version of the recordType was taken and we need to know which version that is
            recordTypeVersion = recordType.getVersion(); 
            if (!recordTypeName.equals(originalRecord.getRecordTypeName())
                    || !recordTypeVersion.equals(originalRecord.getRecordTypeVersion())) {
                recordEvent.setRecordTypeChanged(true);
                put.add(RecordCf.DATA.bytes, RecordColumn.NON_VERSIONED_RT_ID.bytes, 1L, recordType.getId().getBytes());
                put.add(RecordCf.DATA.bytes, RecordColumn.NON_VERSIONED_RT_VERSION.bytes, 1L,
                        Bytes.toBytes(recordTypeVersion));
            }
            // Always set the record type on the record since the requested
            // record type could have been given without a version number
            record.setRecordType(recordTypeName, recordTypeVersion);
            if (version != null) {
                byte[] versionBytes = Bytes.toBytes(version);
                put.add(RecordCf.DATA.bytes, RecordColumn.VERSION.bytes, 1L, versionBytes);
                if (VersionTag.hasLastVTag(recordType, typeManager) || VersionTag.hasLastVTag(record, typeManager) || VersionTag.hasLastVTag(originalRecord, typeManager)) {
                    FieldTypeImpl lastVTagType = (FieldTypeImpl)VersionTag.getLastVTagType(typeManager);
                    put.add(RecordCf.DATA.bytes, lastVTagType.getQualifier(), 1L, encodeFieldValue(lastVTagType, version));
                    record.setField(lastVTagType.getName(), version);
                }
            }
            validateRecord(record, originalRecord, recordType, fieldTypes);

        }
        
        // Always set the version on the record. If no fields were changed this
        // will give the latest version in the repository
        record.setVersion(version);
        
        if (versionedFieldsHaveChanged) {
            recordEvent.setVersionCreated(version);
        }

        // Clear the list of deleted fields, as this is typically what the user will expect when using the
        // record object for future updates. 
        return fieldsHaveChanged;
    }
    
    private void validateRecord(Record record, Record originalRecord, RecordType recordType, FieldTypes fieldTypes) throws
            FieldTypeNotFoundException, TypeException, InvalidRecordException, InterruptedException {
        // Check mandatory fields
        Collection<FieldTypeEntry> fieldTypeEntries = recordType.getFieldTypeEntries();
        List<QName> fieldsToDelete = record.getFieldsToDelete();
        for (FieldTypeEntry fieldTypeEntry : fieldTypeEntries) {
            if (fieldTypeEntry.isMandatory()) {
                FieldType fieldType = fieldTypes.getFieldTypeById(fieldTypeEntry.getFieldTypeId());
                QName fieldName = fieldType.getName();
                if (fieldsToDelete.contains(fieldName)) {
                    throw new InvalidRecordException("Field: <"+fieldName+"> is mandatory.", record.getId());
                }
                if (!record.hasField(fieldName) && !originalRecord.hasField(fieldName)) {
                    throw new InvalidRecordException("Field: <"+fieldName+"> is mandatory.", record.getId());
                }
            }
        }
    }

    // Calculates which fields have changed and updates the record types of the scopes that have changed fields
    private Set<Scope> calculateChangedFields(Record record, Record originalRecord, RecordType recordType,
            Long version, Put put, RecordEvent recordEvent, Set<BlobReference> referencedBlobs, Set<BlobReference> unReferencedBlobs, FieldTypes fieldTypes) throws InterruptedException, FieldTypeNotFoundException, TypeException, BlobException, RecordTypeNotFoundException, RecordException {
        Map<QName, Object> originalFields = originalRecord.getFields();
        Set<Scope> changedScopes = new HashSet<Scope>();
        
        Map<QName, Object> fields = getFieldsToUpdate(record);
        
        changedScopes.addAll(calculateUpdateFields(fields, originalFields, null, version, put, recordEvent, referencedBlobs, unReferencedBlobs, false, fieldTypes));
        for (BlobReference referencedBlob : referencedBlobs) {
            referencedBlob.setRecordId(record.getId());
        }
        for (BlobReference unReferencedBlob : unReferencedBlobs) {
            unReferencedBlob.setRecordId(record.getId());
        }
        // Update record types
        for (Scope scope : changedScopes) {
            long versionOfRTField = version;
            if (Scope.NON_VERSIONED.equals(scope)) {
                versionOfRTField = 1L; // For non-versioned fields the record type is always stored at version 1.
            }
            // Only update the recordTypeNames and versions if they have indeed changed
            QName originalScopeRecordTypeName = originalRecord.getRecordTypeName(scope);
            if (originalScopeRecordTypeName == null) {
                put.add(RecordCf.DATA.bytes, recordTypeIdColumnNames.get(scope), versionOfRTField, recordType.getId().getBytes());
                put.add(RecordCf.DATA.bytes, recordTypeVersionColumnNames.get(scope), versionOfRTField, Bytes.toBytes(recordType.getVersion()));
            } else {
                RecordType originalScopeRecordType = typeManager.getRecordTypeByName(originalScopeRecordTypeName, originalRecord.getRecordTypeVersion(scope));
                if (!recordType.getId().equals(originalScopeRecordType.getId())) {
                    put.add(RecordCf.DATA.bytes, recordTypeIdColumnNames.get(scope), versionOfRTField, recordType.getId().getBytes());
                }
                if (!recordType.getVersion().equals(originalScopeRecordType.getVersion())) {
                    put.add(RecordCf.DATA.bytes, recordTypeVersionColumnNames.get(scope), versionOfRTField, Bytes.toBytes(recordType.getVersion()));
                }
            }
            record.setRecordType(scope, recordType.getName(), recordType.getVersion());
        }
        return changedScopes;
    }

    // Returns a map (fieldname -> field) of all fields that are indicated by the record to be updated.
    // The map also includes the fields that need to be deleted. Their name is mapped onto the delete marker.
    private Map<QName, Object> getFieldsToUpdate(Record record) {
        // Work with a copy of the map
        Map<QName, Object> fields = new HashMap<QName, Object>();
        fields.putAll(record.getFields());
        for (QName qName : record.getFieldsToDelete()) {
            fields.put(qName, DELETE_MARKER);
        }
        return fields;
    }

    // Checks for each field if it is different from its previous value and indeed needs to be updated.
    private Set<Scope> calculateUpdateFields(Map<QName, Object> fields, Map<QName, Object> originalFields, Map<QName, Object> originalNextFields,
            Long version, Put put, RecordEvent recordEvent, Set<BlobReference> referencedBlobs, Set<BlobReference> unReferencedBlobs, boolean mutableUpdate, FieldTypes fieldTypes) throws InterruptedException, FieldTypeNotFoundException, TypeException, BlobException, RecordTypeNotFoundException, RecordException {
        Set<Scope> changedScopes = new HashSet<Scope>();
        for (Entry<QName, Object> field : fields.entrySet()) {
            QName fieldName = field.getKey();
            Object newValue = field.getValue();
            boolean fieldIsNewOrDeleted = !originalFields.containsKey(fieldName);
            Object originalValue = originalFields.get(fieldName);
            if (!(
                    ((newValue == null) && (originalValue == null))         // Don't update if both are null
                    || (isDeleteMarker(newValue) && fieldIsNewOrDeleted)    // Don't delete if it doesn't exist
                    || (newValue.equals(originalValue)))) {                 // Don't update if they didn't change
                FieldTypeImpl fieldType = (FieldTypeImpl)fieldTypes.getFieldTypeByName(fieldName);
                Scope scope = fieldType.getScope();
                
                // Check if the newValue contains blobs 
                Set<BlobReference> newReferencedBlobs = getReferencedBlobs(fieldType, newValue);
                referencedBlobs.addAll(newReferencedBlobs);

                byte[] encodedFieldValue = encodeFieldValue(fieldType, newValue);

                // Check if the previousValue contained blobs which should be deleted since they are no longer used
                // In case of a mutable update, it is checked later if no other versions use the blob before deciding to delete it
                if (Scope.NON_VERSIONED.equals(scope) || (mutableUpdate && Scope.VERSIONED_MUTABLE.equals(scope))) {
                    if (originalValue != null) {
                        Set<BlobReference> previouslyReferencedBlobs = getReferencedBlobs(fieldType, originalValue);
                        previouslyReferencedBlobs.removeAll(newReferencedBlobs);
                        unReferencedBlobs.addAll(previouslyReferencedBlobs);
                    }
                }

                // Set the value 
                if (Scope.NON_VERSIONED.equals(scope)) {
                    put.add(RecordCf.DATA.bytes, fieldType.getQualifier(), 1L, encodedFieldValue);
                } else {
                    put.add(RecordCf.DATA.bytes, fieldType.getQualifier(), version, encodedFieldValue);
                    // If it is a mutable update and the next version of the field was the same as the one that is being updated,
                    // the original value needs to be copied to that next version (due to sparseness of the table). 
                    if (originalNextFields != null && !fieldIsNewOrDeleted && originalNextFields.containsKey(fieldName)) {
                        copyValueToNextVersionIfNeeded(version, put, originalNextFields, fieldName, originalValue, fieldTypes);
                    }
                }
                
                changedScopes.add(scope);

                recordEvent.addUpdatedField(fieldType.getId());
            }
        }
        return changedScopes;
    }
    
    private byte[] encodeFieldValue(FieldType fieldType, Object fieldValue) throws FieldTypeNotFoundException,
            RecordTypeNotFoundException, RecordException {
        if (isDeleteMarker(fieldValue))
            return DELETE_MARKER;
        ValueType valueType = fieldType.getValueType();

        byte[] encodedFieldValue = valueType.toBytes(fieldValue);
        encodedFieldValue = EncodingUtil.prefixValue(encodedFieldValue, EXISTS_FLAG);
        return encodedFieldValue;
    }

    private boolean isDeleteMarker(Object fieldValue) {
        return (fieldValue instanceof byte[]) && Arrays.equals(DELETE_MARKER, (byte[])fieldValue);
    }

    private Record updateMutableFields(Record record, boolean latestRecordType, RowLock rowLock, FieldTypes fieldTypes) throws InvalidRecordException,
            RecordNotFoundException, RecordTypeNotFoundException, FieldTypeNotFoundException, RecordException,
            VersionNotFoundException, TypeException, RecordLockedException, BlobException {

        Record newRecord = record.clone();

        RecordId recordId = record.getId();
        
        Long version = record.getVersion();
        if (version == null) {
            throw new InvalidRecordException("The version of the record cannot be null to update mutable fields",
                    record.getId());
        }

        try {
            Map<QName, Object> fields = getFieldsToUpdate(record);
            fields = filterMutableFields(fields, fieldTypes);

            Record originalRecord = read(recordId, version, null, new ReadContext(), fieldTypes);
            Map<QName, Object> originalFields = filterMutableFields(originalRecord.getFields(), fieldTypes);

            Record originalNextRecord = null;
            Map<QName, Object> originalNextFields = null;
            try {
                originalNextRecord = read(recordId, version + 1, null, new ReadContext(), fieldTypes);
                originalNextFields = filterMutableFields(originalNextRecord.getFields(), fieldTypes);
            } catch (VersionNotFoundException e) {
                // There is no next version of the record
            }

            Put put = new Put(recordId.toBytes());
            Set<BlobReference> referencedBlobs = new HashSet<BlobReference>();
            Set<BlobReference> unReferencedBlobs = new HashSet<BlobReference>();

            RecordEvent recordEvent = new RecordEvent();
            recordEvent.setType(Type.UPDATE);
            recordEvent.setVersionUpdated(version);

            
            Set<Scope> changedScopes = calculateUpdateFields(fields, originalFields, originalNextFields, version, put, recordEvent, referencedBlobs, unReferencedBlobs, true, fieldTypes);
            for (BlobReference referencedBlob : referencedBlobs) {
                referencedBlob.setRecordId(recordId);
            }
            for (BlobReference unReferencedBlob : unReferencedBlobs) {
                unReferencedBlob.setRecordId(recordId);
            }
            
            if (!changedScopes.isEmpty()) {
                // Update the record types
                
                // If no record type is specified explicitly, use the current one of the non-versioned scope
                QName recordTypeName = record.getRecordTypeName() != null ? record.getRecordTypeName() : originalRecord.getRecordTypeName();
                Long recordTypeVersion;
                if (latestRecordType) {
                    recordTypeVersion = null;
                } else if (record.getRecordTypeName() == null) {
                    recordTypeVersion = originalRecord.getRecordTypeVersion();
                } else {
                    recordTypeVersion = record.getRecordTypeVersion();
                }
                RecordType recordType = typeManager.getRecordTypeByName(recordTypeName, recordTypeVersion);
                
                // Update the mutable record type in the record object
                Scope mutableScope = Scope.VERSIONED_MUTABLE;
                newRecord.setRecordType(mutableScope, recordType.getName(), recordType.getVersion());
                
                // If the record type changed, update it on the record table
                QName originalMutableScopeRecordTypeName = originalRecord.getRecordTypeName(mutableScope);
                if (originalMutableScopeRecordTypeName == null) { // There was no initial mutable record type yet
                    put.add(RecordCf.DATA.bytes, recordTypeIdColumnNames.get(mutableScope), version, recordType.getId().getBytes());
                    put.add(RecordCf.DATA.bytes, recordTypeVersionColumnNames.get(mutableScope), version, Bytes.toBytes(recordType.getVersion()));
                } else {
                    RecordType originalMutableScopeRecordType = typeManager.getRecordTypeByName(originalMutableScopeRecordTypeName, originalRecord.getRecordTypeVersion(mutableScope));
                    if (!recordType.getId().equals(originalMutableScopeRecordType.getId())) {
                        // If the next record version had the same record type name, copy the original value to that one
                        if (originalNextRecord != null && originalMutableScopeRecordType.getName().equals(originalNextRecord.getRecordTypeName(mutableScope))) {
                            put.add(RecordCf.DATA.bytes, recordTypeIdColumnNames.get(mutableScope), version+1, originalMutableScopeRecordType.getId().getBytes());
                        }
                        put.add(RecordCf.DATA.bytes, recordTypeIdColumnNames.get(mutableScope), version, recordType.getId().getBytes());
                    }
                    if (!recordType.getVersion().equals(originalMutableScopeRecordType.getVersion())) {
                        // If the next record version had the same record type version, copy the original value to that one
                        if (originalNextRecord != null && originalMutableScopeRecordType.getVersion().equals(originalNextRecord.getRecordTypeVersion(mutableScope))) {
                            put.add(RecordCf.DATA.bytes, recordTypeIdColumnNames.get(mutableScope), version+1, Bytes.toBytes(originalMutableScopeRecordType.getVersion()));
                        }
                        put.add(RecordCf.DATA.bytes, recordTypeVersionColumnNames.get(mutableScope), version, Bytes.toBytes(recordType.getVersion()));
                    }
                }
                
                // Validate if the new values for the record are valid wrt the recordType (e.g. mandatory fields)
                validateRecord(newRecord, originalRecord, recordType, fieldTypes);

                recordEvent.setVersionUpdated(version);

                // Reserve blobs so no other records can use them
                reserveBlobs(record.getId(), referencedBlobs);
                
                putRowWithWalProcessing(recordId, rowLock, put, recordEvent);
                
                // The unReferencedBlobs could still be in use in another version of the mutable field,
                // therefore we filter them first
                unReferencedBlobs = filterReferencedBlobs(recordId, unReferencedBlobs, version);
                
                // Remove the used blobs from the blobIncubator
                blobManager.handleBlobReferences(recordId, referencedBlobs, unReferencedBlobs);

                newRecord.setResponseStatus(ResponseStatus.UPDATED);
            } else {
                newRecord.setResponseStatus(ResponseStatus.UP_TO_DATE);
            }
        } catch (RowLogException e) {
            throw new RecordException("Exception occurred while updating record <" + recordId+ "> on HBase table", e);
        } catch (IOException e) {
            throw new RecordException("Exception occurred while updating record <" + recordId + "> on HBase table",
                    e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RecordException("Exception occurred while updating record <" + recordId + "> on HBase table",
                    e);
        } finally {
            unlockRow(rowLock);
        }

        newRecord.getFieldsToDelete().clear();
        return newRecord;
    }

    private Map<QName, Object> filterMutableFields(Map<QName, Object> fields, FieldTypes fieldTypes) throws FieldTypeNotFoundException,
            RecordTypeNotFoundException, RecordException, TypeException, InterruptedException {
        Map<QName, Object> mutableFields = new HashMap<QName, Object>();
        for (Entry<QName, Object> field : fields.entrySet()) {
            FieldType fieldType = fieldTypes.getFieldTypeByName(field.getKey());
            if (Scope.VERSIONED_MUTABLE.equals(fieldType.getScope())) {
                mutableFields.put(field.getKey(), field.getValue());
            }
        }
        return mutableFields;
    }

    /**
     * If the original value is the same as for the next version
     * this means that the cell at the next version does not contain any value yet,
     * and the record relies on what is in the previous cell's version.
     * The original value needs to be copied into it. Otherwise we loose that value.
     */
    private void copyValueToNextVersionIfNeeded(Long version, Put put, Map<QName, Object> originalNextFields,
            QName fieldName, Object originalValue, FieldTypes fieldTypes)
            throws FieldTypeNotFoundException, RecordTypeNotFoundException, RecordException, TypeException,
            InterruptedException {
        Object originalNextValue = originalNextFields.get(fieldName);
        if ((originalValue == null && originalNextValue == null) || originalValue.equals(originalNextValue)) {
            FieldTypeImpl fieldType = (FieldTypeImpl)fieldTypes.getFieldTypeByName(fieldName);
            byte[] encodedValue = encodeFieldValue(fieldType, originalValue);
            put.add(RecordCf.DATA.bytes, fieldType.getQualifier(), version + 1, encodedValue);
        }
    }

    public Record read(RecordId recordId) throws RecordNotFoundException, RecordTypeNotFoundException,
            FieldTypeNotFoundException, RecordException, VersionNotFoundException, TypeException,
            InterruptedException {
        return read(recordId, null, null);
    }

    public Record read(RecordId recordId, List<QName> fieldNames) throws RecordNotFoundException,
            RecordTypeNotFoundException, FieldTypeNotFoundException, RecordException, VersionNotFoundException,
            TypeException, InterruptedException {
        return read(recordId, null, fieldNames);
    }

    public Record read(RecordId recordId, Long version) throws RecordNotFoundException, RecordTypeNotFoundException,
            FieldTypeNotFoundException, RecordException, VersionNotFoundException, TypeException,
            InterruptedException {
        return read(recordId, version, null);
    }

    public Record read(RecordId recordId, Long version, List<QName> fieldNames) throws RecordNotFoundException,
            RecordTypeNotFoundException, FieldTypeNotFoundException, RecordException, VersionNotFoundException,
            TypeException, InterruptedException {
        FieldTypes fieldTypes = typeManager.getFieldTypesSnapshot();
        List<FieldType> fields = getFieldTypesFromNames(fieldNames, fieldTypes);

        return read(recordId, version, fields, new ReadContext(), fieldTypes);
    }

    private List<FieldType> getFieldTypesFromNames(List<QName> fieldNames, FieldTypes fieldTypes) throws FieldTypeNotFoundException,
            TypeException, InterruptedException {
        List<FieldType> fields = null;
        if (fieldNames != null) {
            fields = new ArrayList<FieldType>();
            for (QName fieldName : fieldNames) {
                fields.add(fieldTypes.getFieldTypeByName(fieldName));
            }
        }
        return fields;
    }
    
    private List<FieldType> getFieldTypesFromIds(List<SchemaId> fieldIds, FieldTypes fieldTypes) throws FieldTypeNotFoundException,
            TypeException, InterruptedException {
        List<FieldType> fields = null;
        if (fieldIds != null) {
            fields = new ArrayList<FieldType>(fieldIds.size());
            for (SchemaId fieldId : fieldIds) {
                fields.add(fieldTypes.getFieldTypeById(fieldId));
            }
        }
        return fields;
    }
    
    public List<Record> readVersions(RecordId recordId, Long fromVersion, Long toVersion, List<QName> fieldNames)
            throws FieldTypeNotFoundException, TypeException, RecordNotFoundException, RecordException,
            VersionNotFoundException, RecordTypeNotFoundException, InterruptedException {
        ArgumentValidator.notNull(recordId, "recordId");
        ArgumentValidator.notNull(fromVersion, "fromVersion");
        ArgumentValidator.notNull(toVersion, "toVersion");
        if (fromVersion > toVersion) {
            throw new IllegalArgumentException("fromVersion <" + fromVersion + "> must be smaller or equal to toVersion <" + toVersion + ">");
        }

        FieldTypes fieldTypes = typeManager.getFieldTypesSnapshot();
        List<FieldType> fields = getFieldTypesFromNames(fieldNames, fieldTypes);

        Result result = getRow(recordId, toVersion, true, fields);
        if (fromVersion < 1L)
            fromVersion = 1L;
        Long latestVersion = getLatestVersion(result);
        if (latestVersion < toVersion)
            toVersion = latestVersion;
        List<Record> records = new ArrayList<Record>();
        for (long version = fromVersion; version <= toVersion; version++) {
            records.add(getRecordFromRowResult(recordId, version, new ReadContext(), result, fieldTypes));
        }
        return records;
    }
    
    public IdRecord readWithIds(RecordId recordId, Long version, List<SchemaId> fieldIds) throws RecordNotFoundException,
            RecordTypeNotFoundException, FieldTypeNotFoundException, RecordException, VersionNotFoundException,
            TypeException, InterruptedException {
        ReadContext readContext = new ReadContext();

        FieldTypes fieldTypes = typeManager.getFieldTypesSnapshot();
        List<FieldType> fields = getFieldTypesFromIds(fieldIds, fieldTypes);

        Record record = read(recordId, version, fields, readContext, fieldTypes);

        Map<SchemaId, QName> idToQNameMapping = new HashMap<SchemaId, QName>();
        for (FieldType fieldType : readContext.getFieldTypes().values()) {
            idToQNameMapping.put(fieldType.getId(), fieldType.getName());
        }

        Map<Scope, SchemaId> recordTypeIds = new HashMap<Scope, SchemaId>();
        for (Map.Entry<Scope, RecordType> entry : readContext.getRecordTypes().entrySet()) {
            recordTypeIds.put(entry.getKey(), entry.getValue().getId());
        }

        return new IdRecordImpl(record, idToQNameMapping, recordTypeIds);
    }
    
    private Record read(RecordId recordId, Long requestedVersion, List<FieldType> fields, ReadContext readContext, FieldTypes fieldTypes)
            throws RecordNotFoundException, RecordTypeNotFoundException, FieldTypeNotFoundException,
            RecordException, VersionNotFoundException, TypeException, InterruptedException {
        long before = System.currentTimeMillis();
        try {
            ArgumentValidator.notNull(recordId, "recordId");
    
            Result result = getRow(recordId, requestedVersion, false, fields);
    
            Long latestVersion = getLatestVersion(result);
            if (requestedVersion == null) {
                requestedVersion = latestVersion;
            } else {
                if (latestVersion == null || latestVersion < requestedVersion ) {
                    throw new VersionNotFoundException(recordId, requestedVersion);
                }
            }
            return getRecordFromRowResult(recordId, requestedVersion, readContext, result, fieldTypes);
        } finally {
            metrics.report(Action.READ, System.currentTimeMillis() - before);
        }
    }

    private Record getRecordFromRowResult(RecordId recordId, Long requestedVersion, ReadContext readContext,
            Result result, FieldTypes fieldTypes) throws VersionNotFoundException, RecordTypeNotFoundException, FieldTypeNotFoundException,
            RecordException, TypeException, InterruptedException {
        Record record = newRecord(recordId);
        record.setVersion(requestedVersion);

        // Extract the actual fields from the retrieved data
        if (extractFieldsAndRecordTypes(result, requestedVersion, record, readContext, fieldTypes)) {
            // Set the recordType explicitly in case only versioned fields were
            // extracted
            Pair<SchemaId, Long> recordTypePair = extractRecordType(Scope.NON_VERSIONED, result, null, record);
            if (recordTypePair != null) {
                RecordType recordType = typeManager.getRecordTypeById(recordTypePair.getV1(), recordTypePair.getV2());
                record.setRecordType(recordType.getName(), recordType.getVersion());
                readContext.setRecordTypeId(Scope.NON_VERSIONED, recordType);
            }
        }
        return record;
    }

    private Long getLatestVersion(Result result) {
        byte[] latestVersionBytes = result.getValue(RecordCf.DATA.bytes, RecordColumn.VERSION.bytes);
        Long latestVersion = latestVersionBytes != null ? Bytes.toLong(latestVersionBytes) : null;
        return latestVersion;
    }

    // Retrieves the row from the table and check if it exists and has not been flagged as deleted
    private Result getRow(RecordId recordId, Long version, boolean multipleVersions, List<FieldType> fields)
            throws RecordNotFoundException, RecordException {
        Result result;
        Get get = new Get(recordId.toBytes());

        try {
            // Add the columns for the fields to get
            addFieldsToGet(get, fields);
            
            if (version != null)
                get.setTimeRange(0, version+1); // Only retrieve data within this timerange
            if (multipleVersions) {
                get.setMaxVersions(); // Get all versions
            } else {
                get.setMaxVersions(1); // Only retrieve the most recent version of each field
            }
            
            // Retrieve the data from the repository
            result = recordTable.get(get);
            
            if (result == null || result.isEmpty())
                throw new RecordNotFoundException(recordId);
            
            // Check if the record was deleted
            byte[] deleted = result.getValue(RecordCf.DATA.bytes, RecordColumn.DELETED.bytes);
            if ((deleted == null) || (Bytes.toBoolean(deleted))) {
                throw new RecordNotFoundException(recordId);
            }
        } catch (IOException e) {
            throw new RecordException("Exception occurred while retrieving record <" + recordId
                    + "> from HBase table", e);
        }
        return result;
    }
    
    private boolean recordExists(byte[] rowId) throws IOException {
        Get get = new Get(rowId);
        
        get.addColumn(RecordCf.DATA.bytes, RecordColumn.DELETED.bytes);
        Result result = recordTable.get(get);
        if (result == null || result.isEmpty()) {
            return false;
        } else {
            byte[] deleted = result.getValue(RecordCf.DATA.bytes, RecordColumn.DELETED.bytes);
            if ((deleted == null) || (Bytes.toBoolean(deleted))) {
                return false;
            } else {
                return true;
            }
        }
    }

    private Pair<SchemaId, Long> extractRecordType(Scope scope, Result result, Long version, Record record) {
        if (version == null) {
            // Get latest version
            byte[] idBytes = result.getValue(RecordCf.DATA.bytes,
                    recordTypeIdColumnNames.get(scope));
            byte[] versionBytes = result.getValue(RecordCf.DATA.bytes,
                    recordTypeVersionColumnNames.get(scope));
            if ((idBytes == null || idBytes.length == 0) || (versionBytes == null || versionBytes.length == 0))
                return null;
            return new Pair<SchemaId, Long>(new SchemaIdImpl(idBytes), Bytes.toLong(versionBytes));

        } else {
            // Get on version
            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> allVersionsMap = result.getMap();
            NavigableMap<byte[], NavigableMap<Long, byte[]>> versionableSystemCFversions = allVersionsMap
                    .get(RecordCf.DATA.bytes);
            return extractVersionRecordType(version, versionableSystemCFversions, scope);
        }
    }

    private Pair<SchemaId, Long> extractVersionRecordType(Long version,
            NavigableMap<byte[], NavigableMap<Long, byte[]>> versionableSystemCFversions, Scope scope) {
        byte[] recordTypeIdColumnName = recordTypeIdColumnNames.get(scope);
        byte[] recordTypeVersionColumnName = recordTypeVersionColumnNames.get(scope);
        Entry<Long, byte[]> idCeilingEntry = null;
        NavigableMap<Long, byte[]> recordTypeIdMap = versionableSystemCFversions.get(recordTypeIdColumnName);
        if (recordTypeIdMap != null) {
            idCeilingEntry = recordTypeIdMap.ceilingEntry(version);
        }
        SchemaId recordTypeId = null;
        if (idCeilingEntry == null)
            return null;
        recordTypeId = new SchemaIdImpl(idCeilingEntry.getValue());
        
        Long recordTypeVersion = null;
        Entry<Long, byte[]> versionCeilingEntry = null;
        NavigableMap<Long, byte[]> recordTypeVersionMap = versionableSystemCFversions.get(recordTypeVersionColumnName);
        if (recordTypeVersionMap != null) {
            versionCeilingEntry = recordTypeVersionMap.ceilingEntry(version);
        }
        if (versionCeilingEntry == null)
            return null;
        recordTypeVersion = Bytes.toLong(versionCeilingEntry.getValue());
        Pair<SchemaId, Long> recordType = new Pair<SchemaId, Long>(recordTypeId, recordTypeVersion);
        return recordType;
    }

    private List<Pair<QName, Object>> extractFields(Long version, Result result, ReadContext context, FieldTypes fieldTypes)
            throws FieldTypeNotFoundException, RecordException, TypeException, InterruptedException {
        List<Pair<QName, Object>> fields = new ArrayList<Pair<QName, Object>>();
        NavigableMap<byte[], NavigableMap<Long, byte[]>> mapWithVersions = result.getMap().get(RecordCf.DATA.bytes);
        if (mapWithVersions != null) {
            for (Entry<byte[], NavigableMap<Long, byte[]>> columnWithAllVersions : mapWithVersions.entrySet()) {
                NavigableMap<Long, byte[]> allValueVersions = columnWithAllVersions.getValue();
                Entry<Long, byte[]> ceilingEntry = allValueVersions.ceilingEntry(version);
                if (ceilingEntry != null) {
                    byte[] key = columnWithAllVersions.getKey();
                    if (key[0] == RecordColumn.DATA_PREFIX) {
                        Pair<QName, Object> field = extractField(key, ceilingEntry.getValue(),
                                context, fieldTypes);
                        if (field != null) {
                            fields.add(field);
                        }
                    }
                }
            }
        }
        return fields;
    }

    private Pair<QName, Object> extractField(byte[] key, byte[] prefixedValue, ReadContext context, FieldTypes fieldTypes)
            throws FieldTypeNotFoundException, RecordException, TypeException, InterruptedException {
        if (EncodingUtil.isDeletedField(prefixedValue)) {
            return null;
        }
        FieldType fieldType = fieldTypes.getFieldTypeById(new SchemaIdImpl(Bytes.tail(key, key.length-1)));
        context.addFieldType(fieldType);
        ValueType valueType = fieldType.getValueType();
        Object value = valueType.read(new DataInputImpl(EncodingUtil.stripPrefix(prefixedValue)));
        return new Pair<QName, Object>(fieldType.getName(), value);
    }

    private void addFieldsToGet(Get get, List<FieldType> fields) {
        if (fields != null && (!fields.isEmpty())) {
            for (FieldType field : fields) {
                get.addColumn(RecordCf.DATA.bytes, ((FieldTypeImpl)field).getQualifier());
            }
            addSystemColumnsToGet(get);
        } else {
            // Retrieve everything
            get.addFamily(RecordCf.DATA.bytes);
        }
    }

    private void addSystemColumnsToGet(Get get) {
        get.addColumn(RecordCf.DATA.bytes, RecordColumn.DELETED.bytes);
        get.addColumn(RecordCf.DATA.bytes, RecordColumn.VERSION.bytes);
        get.addColumn(RecordCf.DATA.bytes, RecordColumn.NON_VERSIONED_RT_ID.bytes);
        get.addColumn(RecordCf.DATA.bytes, RecordColumn.NON_VERSIONED_RT_VERSION.bytes);
        get.addColumn(RecordCf.DATA.bytes, RecordColumn.VERSIONED_RT_ID.bytes);
        get.addColumn(RecordCf.DATA.bytes, RecordColumn.VERSIONED_RT_VERSION.bytes);
        get.addColumn(RecordCf.DATA.bytes, RecordColumn.VERSIONED_MUTABLE_RT_ID.bytes);
        get.addColumn(RecordCf.DATA.bytes, RecordColumn.VERSIONED_MUTABLE_RT_VERSION.bytes);
    }

    private boolean extractFieldsAndRecordTypes(Result result, Long version, Record record, ReadContext context, FieldTypes fieldTypes)
            throws RecordTypeNotFoundException, RecordException, FieldTypeNotFoundException, TypeException,
            InterruptedException {
        List<Pair<QName, Object>> fields;
        if (version == null) {
            // All non-versioned fields are stored at version 1
            fields = extractFields(1L, result, context, fieldTypes);
        } else {
            fields = extractFields(version, result, context, fieldTypes);
        }
        if (fields.isEmpty()) 
            return false;
        
        for (Pair<QName, Object> field : fields) {
            record.setField(field.getV1(), field.getV2());
        }
        for (Scope scope : Scope.values()) {
            Pair<SchemaId, Long> recordTypePair = extractRecordType(scope, result, version, record);
            if (recordTypePair != null) {
                RecordType recordType = typeManager.getRecordTypeById(recordTypePair.getV1(), recordTypePair.getV2());
                record.setRecordType(scope, recordType.getName(), recordType.getVersion());
                context.setRecordTypeId(scope, recordType);
            }
        }
        return true;
    }

    public void delete(RecordId recordId) throws RecordException, RecordNotFoundException, RecordLockedException {
        ArgumentValidator.notNull(recordId, "recordId");
        long before = System.currentTimeMillis();
        RowLock rowLock = null;
        byte[] rowId = recordId.toBytes();
        try {
            // Take Custom Lock
            rowLock = lockRow(recordId);

            if (recordExists(rowId)) { // Check if the record exists in the first place
                Put put = new Put(rowId);
                // Mark the record as deleted
                put.add(RecordCf.DATA.bytes, RecordColumn.DELETED.bytes, 1L, Bytes.toBytes(true));
                RecordEvent recordEvent = new RecordEvent();
                recordEvent.setType(Type.DELETE);
                RowLogMessage walMessage = wal.putMessage(recordId.toBytes(), null, recordEvent.toJsonBytes(), put);
                if (!rowLocker.put(put, rowLock)) {
                    throw new RecordException("Exception occurred while deleting record <" + recordId + "> on HBase table", null);
                }
                
                // Clear the old data and delete any referenced blobs
                clearData(recordId);
    
                if (walMessage != null) {
                    try {
                        wal.processMessage(walMessage, rowLock);
                    } catch (RowLogException e) {
                        // Processing the message failed, it will be retried later.
                    }
                }
            } else {
                throw new RecordNotFoundException(recordId);
            }
        } catch (RowLogException e) {
            throw new RecordException("Exception occurred while deleting record <" + recordId
                    + "> on HBase table", e);

        } catch (IOException e) {
            throw new RecordException("Exception occurred while deleting record <" + recordId + "> on HBase table",
                    e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RecordException("Exception occurred while deleting record <" + recordId + "> on HBase table",
                    e);
        } finally {
            unlockRow(rowLock);
            long after = System.currentTimeMillis();
            metrics.report(Action.DELETE, (after-before));
        }
    }
    
    // Clear all data of the recordId until the latest record version (included)
    // And delete any referred blobs
    private void clearData(RecordId recordId) throws IOException, InterruptedException {
        Get get = new Get(recordId.toBytes());
        get.addFamily(RecordCf.DATA.bytes);
        get.setFilter(new ColumnPrefixFilter(new byte[]{RecordColumn.DATA_PREFIX}));
        get.setMaxVersions();
        Result result = recordTable.get(get);
        
        if (result != null && !result.isEmpty()) {
            boolean dataToDelete = false; 
            Delete delete = new Delete(recordId.toBytes());
            Set<BlobReference> blobsToDelete = new HashSet<BlobReference>();

            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result.getMap();
            Set<Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>> familiesSet = map.entrySet();
            for (Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> family : familiesSet) {
                if (Arrays.equals(RecordCf.DATA.bytes, family.getKey())) {
                    NavigableMap<byte[], NavigableMap<Long, byte[]>> columnsSet = family.getValue();
                    for (Entry<byte[], NavigableMap<Long, byte[]>> column : columnsSet.entrySet()) {
                        try {
                            byte[] columnQualifier = column.getKey();
                            FieldType fieldType = typeManager.getFieldTypeById(new SchemaIdImpl(Bytes.tail(columnQualifier, columnQualifier.length-1)));
                            ValueType valueType = fieldType.getValueType();
                            NavigableMap<Long,byte[]> cells = column.getValue();
                            Set<Entry<Long, byte[]>> cellsSet = cells.entrySet();
                            for (Entry<Long, byte[]> cell : cellsSet) {
                                // Get blobs to delete
                                if (valueType.getPrimitive() instanceof BlobValueType) {
                                    byte[] value = cell.getValue();
                                    if (!isDeleteMarker(value)) {
                                        Object valueObject = valueType.read(new DataInputImpl(EncodingUtil.stripPrefix(value)));
                                        try {
                                            Set<BlobReference> referencedBlobs = getReferencedBlobs((FieldTypeImpl)fieldType, valueObject);
                                            blobsToDelete.addAll(referencedBlobs);
                                        } catch (BlobException e) {
                                            log.warn("Failure occured while clearing blob data", e);
                                            // We do a best effort here
                                        }
                                    }
                                }
                                // Get cells to delete
                                delete.deleteColumn(RecordCf.DATA.bytes, columnQualifier, cell.getKey());
                                dataToDelete = true;
                            }
                        } catch (FieldTypeNotFoundException e) {
                            log.warn("Failure occured while clearing blob data", e);
                            // We do a best effort here
                        } catch (TypeException e) {
                            log.warn("Failure occured while clearing blob data", e);
                            // We do a best effort here
                        }
                    }
                } else {
                    //skip
                }
            }
            // Delete the blobs
            blobManager.handleBlobReferences(recordId, null, blobsToDelete);
            
            // Delete data
            if (dataToDelete) { // Avoid a delete action when no data was found to delete
                delete.deleteColumn(RecordCf.DATA.bytes, RecordColumn.NON_VERSIONED_RT_ID.bytes);
                delete.deleteColumn(RecordCf.DATA.bytes, RecordColumn.NON_VERSIONED_RT_VERSION.bytes);
                delete.deleteColumn(RecordCf.DATA.bytes, RecordColumn.VERSIONED_RT_ID.bytes);
                delete.deleteColumn(RecordCf.DATA.bytes, RecordColumn.VERSIONED_RT_VERSION.bytes);
                delete.deleteColumn(RecordCf.DATA.bytes, RecordColumn.VERSIONED_MUTABLE_RT_ID.bytes);
                delete.deleteColumn(RecordCf.DATA.bytes, RecordColumn.VERSIONED_MUTABLE_RT_VERSION.bytes);
                recordTable.delete(delete);
            }
        }
    }
    
    private void unlockRow(RowLock rowLock) {
        if (rowLock != null) {
            try {
                rowLocker.unlockRow(rowLock);
            } catch (IOException e) {
                log.warn("Exception while unlocking row <" + rowLock.getRowKey()+ ">", e);
            }
        }
    }

    private RowLock lockRow(RecordId recordId) throws IOException,
            RecordLockedException {
        RowLock rowLock = rowLocker.lockRow(recordId.toBytes());
        if (rowLock == null)
            throw new RecordLockedException(recordId);
        return rowLock;
    }

    private Set<BlobReference> getReferencedBlobs(FieldTypeImpl fieldType, Object value) throws BlobException {
        HashSet<BlobReference> referencedBlobs = new HashSet<BlobReference>();
        ValueType valueType = fieldType.getValueType();
        PrimitiveValueType primitiveValueType = valueType.getPrimitive();
        if ((primitiveValueType instanceof BlobValueType) && ! isDeleteMarker(value)) {
            Set<Object> values = valueType.getValues(value);
            for (Object object : values) {
                referencedBlobs.add(new BlobReference((Blob)object, null, fieldType));
            }
        }
        return referencedBlobs;
    }

    private void reserveBlobs(RecordId recordId, Set<BlobReference> referencedBlobs) throws IOException,
            InvalidRecordException {
        if (!referencedBlobs.isEmpty()) {
            // Check if the blob is newly uploaded
            Set<BlobReference> failedReservations = blobManager.reserveBlobs(referencedBlobs);
            // If not, filter those that are already used by the record
            failedReservations = filterReferencedBlobs(recordId, failedReservations, null);
            if (!failedReservations.isEmpty())
            {
                throw new InvalidRecordException("Record references blobs which are not available for use", recordId);
            }
        }
    }
    
    // Checks the set of blobs and returns a subset of those blobs which are not referenced anymore
    private Set<BlobReference> filterReferencedBlobs(RecordId recordId, Set<BlobReference> blobs, Long ignoreVersion) throws IOException {
        if (recordId == null)
            return blobs;
        Set<BlobReference> unReferencedBlobs = new HashSet<BlobReference>();
        for (BlobReference blobReference : blobs) {
            FieldTypeImpl fieldType = (FieldTypeImpl)blobReference.getFieldType();
            byte[] recordIdBytes = recordId.toBytes();
            ValueType valueType = fieldType.getValueType();

            Get get = new Get(recordIdBytes);
            get.addColumn(RecordCf.DATA.bytes, fieldType.getQualifier());
            byte[] valueToCompare;
            if (valueType.isMultiValue() && valueType.isHierarchical()) {
                valueToCompare = Bytes.toBytes(2);
            } else if (valueType.isMultiValue() || valueType.isHierarchical()) {
                valueToCompare = Bytes.toBytes(1);
            } else {
                valueToCompare = Bytes.toBytes(0);
            }
            valueToCompare = Bytes.add(valueToCompare, blobReference.getBlob().getValue());
            WritableByteArrayComparable valueComparator = new ContainsValueComparator(valueToCompare);
            Filter filter = new SingleColumnValueFilter(RecordCf.DATA.bytes, fieldType.getQualifier(), CompareOp.EQUAL, valueComparator);
            get.setFilter(filter);
            Result result = recordTable.get(get);
            
            if (result.isEmpty()) {
                unReferencedBlobs.add(blobReference);
            } else {
                if (ignoreVersion != null) {
                    boolean stillReferenced = false;
                    List<KeyValue> column = result.getColumn(RecordCf.DATA.bytes, fieldType.getQualifier());
                    for (KeyValue keyValue : column) {
                        if (keyValue.getTimestamp() != ignoreVersion) {
                            stillReferenced = true;
                            break;
                        }
                    }
                    if (!stillReferenced) {
                        unReferencedBlobs.add(blobReference);
                    }
                }
            }
        }
        return unReferencedBlobs;
    }

    public Set<RecordId> getVariants(RecordId recordId) throws RepositoryException {
        byte[] masterRecordIdBytes = recordId.getMaster().toBytes();
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        filterList.addFilter(new PrefixFilter(masterRecordIdBytes));
        filterList.addFilter(new SingleColumnValueFilter(RecordCf.DATA.bytes,
                RecordColumn.DELETED.bytes, CompareFilter.CompareOp.NOT_EQUAL, Bytes.toBytes(true)));

        Scan scan = new Scan(masterRecordIdBytes, filterList);
        scan.addColumn(RecordCf.DATA.bytes, RecordColumn.DELETED.bytes);

        Set<RecordId> recordIds = new HashSet<RecordId>();

        try {
            ResultScanner scanner = recordTable.getScanner(scan);
            Result result;
            while ((result = scanner.next()) != null) {
                RecordId id = idGenerator.fromBytes(result.getRow());
                recordIds.add(id);
            }
            Closer.close(scanner); // Not closed in finally block: avoid HBase contact when there could be connection problems.
        } catch (IOException e) {
            throw new RepositoryException("Error getting list of variants of record " + recordId.getMaster(), e);
        }

        return recordIds;
    }
    
    private void checkAndProcessOpenMessages(RecordId recordId, RowLock rowLock) throws WalProcessingException {
        byte[] rowKey = recordId.toBytes();
        try {
            List<RowLogMessage> messages = wal.getMessages(rowKey);
            if (messages.isEmpty())
               return;
            try {
                for (RowLogMessage rowLogMessage : messages) {
                    wal.processMessage(rowLogMessage, rowLock);
                }
                if (!(wal.getMessages(rowKey).isEmpty())) {
                    throw new WalProcessingException(recordId, "Not all messages were processed");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new WalProcessingException(recordId, e);
            }
        } catch (RowLogException e) {
            throw new WalProcessingException(recordId, e);
        }
    }

    private static class ReadContext {
        private Map<SchemaId, FieldType> fieldTypes = new HashMap<SchemaId, FieldType>();
        private Map<Scope, RecordType> recordTypes = new HashMap<Scope, RecordType>();
        private Set<Scope> scopes = new HashSet<Scope>();

        public void addFieldType(FieldType fieldType) {
            fieldTypes.put(fieldType.getId(), fieldType);
            scopes.add(fieldType.getScope());
        }

        public void setRecordTypeId(Scope scope, RecordType recordType) {
            recordTypes.put(scope, recordType);
        }

        public Map<Scope, RecordType> getRecordTypes() {
            return recordTypes;
        }

        public Map<SchemaId, FieldType> getFieldTypes() {
            return fieldTypes;
        }
        
        public Set<Scope> getScopes() {
            return scopes;
        }
    }
}
