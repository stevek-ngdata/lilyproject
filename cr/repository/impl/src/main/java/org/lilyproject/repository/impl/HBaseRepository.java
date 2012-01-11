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
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilyproject.bytes.api.DataOutput;
import org.lilyproject.bytes.impl.DataOutputImpl;
import org.lilyproject.hbaseext.ContainsValueComparator;
import org.lilyproject.repository.api.*;
import org.lilyproject.repository.impl.RepositoryMetrics.Action;
import org.lilyproject.repository.impl.valuetype.BlobValueType;
import org.lilyproject.repository.spi.RecordUpdateHook;
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
import org.lilyproject.util.repo.RecordEvent.Type;

/**
 * Repository implementation.
 * 
 */
public class HBaseRepository extends BaseRepository {
 
    private HTableInterface recordTable;
    private Map<Scope, byte[]> recordTypeIdColumnNames = new EnumMap<Scope, byte[]>(Scope.class);
    private Map<Scope, byte[]> recordTypeVersionColumnNames = new EnumMap<Scope, byte[]>(Scope.class);
    private RowLog wal;
    private RowLocker rowLocker;
    private List<RecordUpdateHook> updateHooks = Collections.emptyList();
    
    private Log log = LogFactory.getLog(getClass());
    private RepositoryMetrics metrics;

    public HBaseRepository(TypeManager typeManager, IdGenerator idGenerator, RowLog wal,
            HBaseTableFactory hbaseTableFactory, BlobManager blobManager, RowLocker rowLocker) throws IOException {
        super(typeManager, blobManager, idGenerator);

        this.wal = wal;

        recordTable = LilyHBaseSchema.getRecordTable(hbaseTableFactory);

        recordTypeIdColumnNames.put(Scope.NON_VERSIONED, RecordColumn.NON_VERSIONED_RT_ID.bytes);
        recordTypeIdColumnNames.put(Scope.VERSIONED, RecordColumn.VERSIONED_RT_ID.bytes);
        recordTypeIdColumnNames.put(Scope.VERSIONED_MUTABLE, RecordColumn.VERSIONED_MUTABLE_RT_ID.bytes);
        recordTypeVersionColumnNames.put(Scope.NON_VERSIONED, RecordColumn.NON_VERSIONED_RT_VERSION.bytes);
        recordTypeVersionColumnNames.put(Scope.VERSIONED, RecordColumn.VERSIONED_RT_VERSION.bytes);
        recordTypeVersionColumnNames.put(Scope.VERSIONED_MUTABLE, RecordColumn.VERSIONED_MUTABLE_RT_VERSION.bytes);

        this.rowLocker = rowLocker;
        metrics = new RepositoryMetrics("hbaserepository");
    }

    @Override
    public void close() throws IOException {
    }

    /**
     * Sets the record update hooks.
     */
    public void setRecordUpdateHooks(List<RecordUpdateHook> recordUpdateHooks) {
        this.updateHooks = recordUpdateHooks == null ?
                Collections.<RecordUpdateHook>emptyList() : recordUpdateHooks;
    }

    @Override
    public IdGenerator getIdGenerator() {
        return idGenerator;
    }

    @Override
    public Record createOrUpdate(Record record) throws RepositoryException, InterruptedException {
        return createOrUpdate(record, true);
    }

    @Override
    public Record createOrUpdate(Record record, boolean useLatestRecordType) throws RepositoryException,
            InterruptedException {

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

            byte[] deleted = getLatest(result, RecordCf.DATA.bytes, RecordColumn.DELETED.bytes);
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

    @Override
    public Record create(Record record) throws RepositoryException {

        long before = System.currentTimeMillis();
        try {
            checkCreatePreconditions(record);
            
            Record newRecord = record.cloneRecord();
    
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
                        // occurred before executing the clearData
                        // If this was already done, this is a no-op
                        clearData(recordId, null);
                    }
                }
                
                Record dummyOriginalRecord = newRecord();
                Put put = new Put(newRecord.getId().toBytes());
                put.add(RecordCf.DATA.bytes, RecordColumn.DELETED.bytes, 1L, Bytes.toBytes(false));
                RecordEvent recordEvent = new RecordEvent();
                recordEvent.setType(Type.CREATE);
                Set<BlobReference> referencedBlobs = new HashSet<BlobReference>();
                Set<BlobReference> unReferencedBlobs = new HashSet<BlobReference>();
                
                calculateRecordChanges(newRecord, dummyOriginalRecord, version, put, recordEvent, referencedBlobs,
                        unReferencedBlobs, false, fieldTypes);

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
                throw new RecordException("Exception occurred while creating record '" + recordId + "' in HBase table",
                        e);
            } catch (RowLogException e) {
                throw new RecordException("Exception occurred while creating record '" + recordId + "' in HBase table",
                        e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RecordException("Exception occurred while creating record '" + recordId + "' in HBase table",
                        e);
            } catch (BlobException e) {
                throw new RecordException("Exception occurred while creating record '" + recordId + "'",
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

    @Override
    public Record update(Record record) throws RepositoryException, InterruptedException {
        return update(record, false, true);
    }

    @Override
    public Record update(Record record, List<MutationCondition> conditions) throws RepositoryException,
            InterruptedException {
        return update(record, false, true, conditions);
    }

    @Override
    public Record update(Record record, boolean updateVersion, boolean useLatestRecordType) throws RepositoryException,
            InterruptedException {
        return update(record, updateVersion, useLatestRecordType, null);
    }

    @Override
    public Record update(Record record, boolean updateVersion, boolean useLatestRecordType,
            List<MutationCondition> conditions) throws RepositoryException, InterruptedException {

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
                    return updateMutableFields(record, useLatestRecordType, conditions, rowLock, fieldTypes);
                } catch (BlobException e) {
                    throw new RecordException("Exception occurred while updating record '" + record.getId() + "'",
                            e);
                }
            } else {
                return updateRecord(record, useLatestRecordType, conditions, rowLock, fieldTypes);
            }
        } catch (IOException e) {
            throw new RecordException("Exception occurred while updating record '" + recordId + ">' on HBase table",
                    e); 
        } finally {
            unlockRow(rowLock);
            metrics.report(Action.UPDATE, System.currentTimeMillis() - before);
        }
    }
    
    
    private Record updateRecord(Record record, boolean useLatestRecordType, List<MutationCondition> conditions,
            RowLock rowLock, FieldTypes fieldTypes) throws RepositoryException {

        RecordId recordId = record.getId();

        try {
            Record originalRecord = new UnmodifiableRecord(read(record.getId(), null, null, null, fieldTypes));

            for (RecordUpdateHook hook : updateHooks) {
                hook.beforeUpdate(record, originalRecord, this, fieldTypes);
            }

            Record newRecord = record.cloneRecord();

            Put put = new Put(newRecord.getId().toBytes());
            Set<BlobReference> referencedBlobs = new HashSet<BlobReference>();
            Set<BlobReference> unReferencedBlobs = new HashSet<BlobReference>();
            RecordEvent recordEvent = new RecordEvent();
            recordEvent.setType(Type.UPDATE);
            long newVersion = originalRecord.getVersion() == null ? 1 : originalRecord.getVersion() + 1;
                
            if (calculateRecordChanges(newRecord, originalRecord, newVersion, put, recordEvent, referencedBlobs,
                    unReferencedBlobs, useLatestRecordType, fieldTypes)) {

                // Check the conditions after establishing that the record really needs updating, this makes the
                // conditional update operation idempotent.
                Record conditionsResponse = MutationConditionVerifier.checkConditions(originalRecord, conditions, this,
                        record);
                if (conditionsResponse != null) {
                    return conditionsResponse;
                }

                // Reserve blobs so no other records can use them
                reserveBlobs(record.getId(), referencedBlobs);
                putRowWithWalProcessing(recordId, rowLock, put, recordEvent);
                // Remove the used blobs from the blobIncubator and delete unreferenced blobs from the blobstore
                blobManager.handleBlobReferences(recordId, referencedBlobs, unReferencedBlobs);
                newRecord.setResponseStatus(ResponseStatus.UPDATED);
            } else {
                newRecord.setResponseStatus(ResponseStatus.UP_TO_DATE);
            }

            newRecord.getFieldsToDelete().clear();
            return newRecord;

        } catch (RowLogException e) {
            throw new RecordException("Exception occurred while putting updated record '" + recordId
                    + "' on HBase table", e);

        } catch (IOException e) {
            throw new RecordException("Exception occurred while updating record '" + recordId + "' on HBase table",
                    e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RecordException("Exception occurred while updating record '" + recordId + "' on HBase table",
                    e);
        } catch (BlobException e) {
            throw new RecordException("Exception occurred while putting updated record '" + recordId
                    + "' on HBase table", e);
        }
    }

    // This method takes a put object containing the row's data to be updated
    // A wal message is added to this put object
    // The rowLocker is asked to put the data and message on the record table using the given rowlock
    // Finally the wal is asked to process the message
    private void putRowWithWalProcessing(RecordId recordId, RowLock rowLock, Put put, RecordEvent recordEvent)
            throws InterruptedException, RowLogException, IOException, RecordException {
        RowLogMessage walMessage;
        walMessage = wal.putMessage(recordId.toBytes(), null, recordEvent.toJsonBytes(), put);
        if (!rowLocker.put(put, rowLock)) {
            throw new RecordException("Invalid or expired lock trying to put record '" + recordId + "' on HBase table");
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
            RecordEvent recordEvent, Set<BlobReference> referencedBlobs, Set<BlobReference> unReferencedBlobs,
            boolean useLatestRecordType, FieldTypes fieldTypes) throws InterruptedException, RepositoryException {
        QName recordTypeName = record.getRecordTypeName();
        Long recordTypeVersion = null;
        if (recordTypeName == null) {
            recordTypeName = originalRecord.getRecordTypeName();
        } else {
            recordTypeVersion = useLatestRecordType ? null : record.getRecordTypeVersion();
        }

        RecordType recordType = typeManager.getRecordTypeByName(recordTypeName, recordTypeVersion);
        
        // Check which fields have changed
        Set<Scope> changedScopes = calculateChangedFields(record, originalRecord, recordType, version, put, recordEvent,
                referencedBlobs, unReferencedBlobs, fieldTypes);

        // If no versioned fields have changed, keep the original version
        boolean versionedFieldsHaveChanged = changedScopes.contains(Scope.VERSIONED)
                || changedScopes.contains(Scope.VERSIONED_MUTABLE);
        if (!versionedFieldsHaveChanged) {
            version = originalRecord.getVersion();
        }

        boolean fieldsHaveChanged = !changedScopes.isEmpty();
        if (fieldsHaveChanged) {
            // The provided recordTypeVersion could have been null, so the latest version of the recordType was taken
            // and we need to know which version that is
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
            }
            validateRecord(record, originalRecord, recordType, fieldTypes);

        }

        setRecordTypesAfterUpdate(record, originalRecord, changedScopes);

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

    private void setRecordTypesAfterUpdate(Record record, Record originalRecord, Set<Scope> changedScopes) {
        // The returned record object after an update should always contain complete record type information for
        // all the scopes
        for (Scope scope : Scope.values()) {
            // For any unchanged or non-existing scope, we reset the record type information to the one of the
            // original record, so that the returned record object corresponds to the repository state (= same
            // as when one would do a fresh read)
            //
            // Copy over the original record type of a scope if:
            //   - the scope was unchanged. If it was changed, the record type will already have been filled in
            //     by calculateRecordChanges.
            //   - for the non-versioned scope, only copy it over if none of the scopes changed, because the
            //     record type of the non-versioned scope is always brought up to date in case any scope is changed
            if (!changedScopes.contains(scope) && (scope != Scope.NON_VERSIONED || changedScopes.isEmpty())) {
                record.setRecordType(scope, originalRecord.getRecordTypeName(scope),
                        originalRecord.getRecordTypeVersion(scope));
            }
        }
    }
    
    private void validateRecord(Record record, Record originalRecord, RecordType recordType, FieldTypes fieldTypes)
            throws TypeException, InvalidRecordException, InterruptedException {
        // Check mandatory fields
        Collection<FieldTypeEntry> fieldTypeEntries = recordType.getFieldTypeEntries();
        List<QName> fieldsToDelete = record.getFieldsToDelete();
        for (FieldTypeEntry fieldTypeEntry : fieldTypeEntries) {
            if (fieldTypeEntry.isMandatory()) {
                FieldType fieldType = fieldTypes.getFieldType(fieldTypeEntry.getFieldTypeId());
                QName fieldName = fieldType.getName();
                if (fieldsToDelete.contains(fieldName)) {
                    throw new InvalidRecordException("Field: '" + fieldName + "' is mandatory.", record.getId());
                }
                if (!record.hasField(fieldName) && !originalRecord.hasField(fieldName)) {
                    throw new InvalidRecordException("Field: '" + fieldName + "' is mandatory.", record.getId());
                }
            }
        }
    }

    // Calculates which fields have changed and updates the record types of the scopes that have changed fields
    private Set<Scope> calculateChangedFields(Record record, Record originalRecord, RecordType recordType,
            Long version, Put put, RecordEvent recordEvent, Set<BlobReference> referencedBlobs,
            Set<BlobReference> unReferencedBlobs, FieldTypes fieldTypes)
            throws InterruptedException, RepositoryException {

        Map<QName, Object> originalFields = originalRecord.getFields();
        Set<Scope> changedScopes = EnumSet.noneOf(Scope.class);
        
        Map<QName, Object> fields = getFieldsToUpdate(record);
        
        changedScopes.addAll(calculateUpdateFields(record, fields, originalFields, null, version, put, recordEvent,
                referencedBlobs, unReferencedBlobs, false, fieldTypes));
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
                put.add(RecordCf.DATA.bytes, recordTypeIdColumnNames.get(scope), versionOfRTField,
                        recordType.getId().getBytes());
                put.add(RecordCf.DATA.bytes, recordTypeVersionColumnNames.get(scope), versionOfRTField,
                        Bytes.toBytes(recordType.getVersion()));
            } else {
                RecordType originalScopeRecordType = typeManager.getRecordTypeByName(originalScopeRecordTypeName,
                        originalRecord.getRecordTypeVersion(scope));
                if (!recordType.getId().equals(originalScopeRecordType.getId())) {
                    put.add(RecordCf.DATA.bytes, recordTypeIdColumnNames.get(scope), versionOfRTField,
                            recordType.getId().getBytes());
                }
                if (!recordType.getVersion().equals(originalScopeRecordType.getVersion())) {
                    put.add(RecordCf.DATA.bytes, recordTypeVersionColumnNames.get(scope), versionOfRTField,
                            Bytes.toBytes(recordType.getVersion()));
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
    private Set<Scope> calculateUpdateFields(Record parentRecord, Map<QName, Object> fields,
            Map<QName, Object> originalFields,
            Map<QName, Object> originalNextFields, Long version, Put put, RecordEvent recordEvent,
            Set<BlobReference> referencedBlobs, Set<BlobReference> unReferencedBlobs, boolean mutableUpdate,
            FieldTypes fieldTypes) throws InterruptedException, TypeException, BlobException, RecordException, RepositoryException {
        Set<Scope> changedScopes = EnumSet.noneOf(Scope.class);
        for (Entry<QName, Object> field : fields.entrySet()) {
            QName fieldName = field.getKey();
            Object newValue = field.getValue();
            boolean fieldIsNewOrDeleted = !originalFields.containsKey(fieldName);
            Object originalValue = originalFields.get(fieldName);
            if (!(
                    ((newValue == null) && (originalValue == null))         // Don't update if both are null
                    || (isDeleteMarker(newValue) && fieldIsNewOrDeleted)    // Don't delete if it doesn't exist
                    || (newValue.equals(originalValue)))) {                 // Don't update if they didn't change
                FieldTypeImpl fieldType = (FieldTypeImpl)fieldTypes.getFieldType(fieldName);
                Scope scope = fieldType.getScope();
                
                // Check if the newValue contains blobs 
                Set<BlobReference> newReferencedBlobs = getReferencedBlobs(fieldType, newValue);
                referencedBlobs.addAll(newReferencedBlobs);

                byte[] encodedFieldValue = encodeFieldValue(parentRecord, fieldType, newValue);

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
                        copyValueToNextVersionIfNeeded(parentRecord, version, put, originalNextFields, fieldName,
                                originalValue, fieldTypes);
                    }
                }
                
                changedScopes.add(scope);

                recordEvent.addUpdatedField(fieldType.getId());
            }
        }
        return changedScopes;
    }
    
    private byte[] encodeFieldValue(Record parentRecord, FieldType fieldType, Object fieldValue)
            throws FieldTypeNotFoundException,
            RecordTypeNotFoundException, RecordException, RepositoryException, InterruptedException {
        if (isDeleteMarker(fieldValue))
            return DELETE_MARKER;
        ValueType valueType = fieldType.getValueType();

        DataOutput dataOutput = new DataOutputImpl();
        dataOutput.writeByte(EXISTS_FLAG);
        valueType.write(fieldValue, dataOutput, new IdentityRecordStack(parentRecord));
        return dataOutput.toByteArray();
    }

    private boolean isDeleteMarker(Object fieldValue) {
        return (fieldValue instanceof byte[]) && Arrays.equals(DELETE_MARKER, (byte[])fieldValue);
    }

    private Record updateMutableFields(Record record, boolean latestRecordType, List<MutationCondition> conditions,
            RowLock rowLock, FieldTypes fieldTypes) throws RepositoryException {

        Record newRecord = record.cloneRecord();

        RecordId recordId = record.getId();
        
        Long version = record.getVersion();
        if (version == null) {
            throw new InvalidRecordException("The version of the record cannot be null to update mutable fields",
                    record.getId());
        }

        try {
            Map<QName, Object> fields = getFieldsToUpdate(record);
            fields = filterMutableFields(fields, fieldTypes);

            Record originalRecord = new UnmodifiableRecord(read(recordId, version, null, null, fieldTypes));

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

            
            Set<Scope> changedScopes = calculateUpdateFields(record, fields, originalFields, originalNextFields,
                    version, put,
                    recordEvent, referencedBlobs, unReferencedBlobs, true, fieldTypes);
            for (BlobReference referencedBlob : referencedBlobs) {
                referencedBlob.setRecordId(recordId);
            }
            for (BlobReference unReferencedBlob : unReferencedBlobs) {
                unReferencedBlob.setRecordId(recordId);
            }
            
            if (!changedScopes.isEmpty()) {
                // Check the conditions after establishing that the record really needs updating, this makes the
                // conditional update operation idempotent.
                Record conditionsRecord = MutationConditionVerifier.checkConditions(originalRecord, conditions, this,
                        record);
                if (conditionsRecord != null) {
                    return conditionsRecord;
                }

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

            setRecordTypesAfterUpdate(record, originalRecord, changedScopes);
        } catch (RowLogException e) {
            throw new RecordException("Exception occurred while updating record '" + recordId+ "' on HBase table", e);
        } catch (IOException e) {
            throw new RecordException("Exception occurred while updating record '" + recordId + "' on HBase table",
                    e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RecordException("Exception occurred while updating record '" + recordId + "' on HBase table",
                    e);
        } finally {
            unlockRow(rowLock);
        }

        newRecord.getFieldsToDelete().clear();
        return newRecord;
    }

    private Map<QName, Object> filterMutableFields(Map<QName, Object> fields, FieldTypes fieldTypes)
            throws RecordException, TypeException, InterruptedException {
        Map<QName, Object> mutableFields = new HashMap<QName, Object>();
        for (Entry<QName, Object> field : fields.entrySet()) {
            FieldType fieldType = fieldTypes.getFieldType(field.getKey());
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
    private void copyValueToNextVersionIfNeeded(Record parentRecord, Long version, Put put,
            Map<QName, Object> originalNextFields,
            QName fieldName, Object originalValue, FieldTypes fieldTypes)
            throws RecordException, TypeException, RepositoryException, InterruptedException {
        Object originalNextValue = originalNextFields.get(fieldName);
        if ((originalValue == null && originalNextValue == null) || originalValue.equals(originalNextValue)) {
            FieldTypeImpl fieldType = (FieldTypeImpl)fieldTypes.getFieldType(fieldName);
            byte[] encodedValue = encodeFieldValue(parentRecord, fieldType, originalValue);
            put.add(RecordCf.DATA.bytes, fieldType.getQualifier(), version + 1, encodedValue);
        }
    }

    @Override
    public Record read(RecordId recordId, List<QName> fieldNames) throws RepositoryException, InterruptedException {
        return read(recordId, null, fieldNames == null ? null : fieldNames.toArray(new QName[fieldNames.size()]));
    }
    
    @Override
    public Record read(RecordId recordId, QName... fieldNames) throws RepositoryException, InterruptedException {
        return read(recordId, null, fieldNames);
    }
    
    @Override
    public List<Record> read(List<RecordId> recordIds, List<QName> fieldNames)
            throws RepositoryException, InterruptedException {
        return read(recordIds, fieldNames == null ? null : fieldNames.toArray(new QName[fieldNames.size()]));
    }
    
    @Override
    public List<Record> read(List<RecordId> recordIds, QName...fieldNames)
            throws RepositoryException, InterruptedException {
        FieldTypes fieldTypes = typeManager.getFieldTypesSnapshot();
        List<FieldType> fields = getFieldTypesFromNames(fieldTypes, fieldNames);
        
        return read(recordIds, fields, fieldTypes);
    }

    @Override
    public Record read(RecordId recordId, Long version, List<QName> fieldNames)
            throws RepositoryException, InterruptedException {
        return read(recordId, version , fieldNames == null ? null : fieldNames.toArray(new QName[fieldNames.size()]));
    }
    
    @Override
    public Record read(RecordId recordId, Long version, QName... fieldNames) throws RepositoryException, InterruptedException {
        FieldTypes fieldTypes = typeManager.getFieldTypesSnapshot();
        List<FieldType> fields = getFieldTypesFromNames(fieldTypes, fieldNames);
        
        return read(recordId, version, fields, null, fieldTypes);
    }

    private List<FieldType> getFieldTypesFromNames(FieldTypes fieldTypes, QName... fieldNames)
        throws TypeException, InterruptedException {
        List<FieldType> fields = null;
        if (fieldNames != null) {
            fields = new ArrayList<FieldType>();
            for (QName fieldName : fieldNames) {
                fields.add(fieldTypes.getFieldType(fieldName));
            }
        }
        return fields;
    }
    
    private List<FieldType> getFieldTypesFromIds(List<SchemaId> fieldIds, FieldTypes fieldTypes)
            throws TypeException, InterruptedException {
        List<FieldType> fields = null;
        if (fieldIds != null) {
            fields = new ArrayList<FieldType>(fieldIds.size());
            for (SchemaId fieldId : fieldIds) {
                fields.add(fieldTypes.getFieldType(fieldId));
            }
        }
        return fields;
    }
    
    @Override
    public List<Record> readVersions(RecordId recordId, Long fromVersion, Long toVersion, List<QName> fieldNames)
        throws RepositoryException, InterruptedException {
        return readVersions(recordId, fromVersion, toVersion, fieldNames == null ? null : fieldNames.toArray(new QName[fieldNames.size()]));
    }
    
    @Override
    public List<Record> readVersions(RecordId recordId, Long fromVersion, Long toVersion, QName... fieldNames)
            throws RepositoryException, InterruptedException {
        ArgumentValidator.notNull(recordId, "recordId");
        ArgumentValidator.notNull(fromVersion, "fromVersion");
        ArgumentValidator.notNull(toVersion, "toVersion");
        if (fromVersion > toVersion) {
            throw new IllegalArgumentException("fromVersion '" + fromVersion +
                    "' must be smaller or equal to toVersion '" + toVersion + "'");
        }

        FieldTypes fieldTypes = typeManager.getFieldTypesSnapshot();
        List<FieldType> fields = getFieldTypesFromNames(fieldTypes, fieldNames);

        int numberOfVersionsToRetrieve = (int)(toVersion - fromVersion + 1);
        Result result = getRow(recordId, toVersion, numberOfVersionsToRetrieve, fields);
        if (fromVersion < 1L)
            fromVersion = 1L; // Put the fromVersion to a sensible value
        Long latestVersion = getLatestVersion(result);
        if (latestVersion < toVersion)
            toVersion = latestVersion; // Limit the toVersion to the highest possible version
        List<Long> versionsToRead = new ArrayList<Long>(); 
        for (long version = fromVersion; version <= toVersion; version++) {
            versionsToRead.add(version);
        }
        return getRecordsFromRowResult(recordId, versionsToRead, result, fieldTypes);
    }
    
    @Override
    public List<Record> readVersions(RecordId recordId, List<Long> versions, List<QName> fieldNames)
    throws RepositoryException, InterruptedException {
        return readVersions(recordId, versions, fieldNames == null ? null : fieldNames.toArray(new QName[fieldNames.size()]));
    }
    
    @Override
    public List<Record> readVersions(RecordId recordId, List<Long> versions, QName... fieldNames)
            throws RepositoryException, InterruptedException {
        ArgumentValidator.notNull(recordId, "recordId");
        ArgumentValidator.notNull(versions, "versions");
        
        if (versions.isEmpty())
            return new ArrayList<Record>();

        Collections.sort(versions);
        
        FieldTypes fieldTypes = typeManager.getFieldTypesSnapshot();
        List<FieldType> fields = getFieldTypesFromNames(fieldTypes, fieldNames);
        
        Long lowestRequestedVersion = versions.get(0);
        Long highestRequestedVersion = versions.get(versions.size()-1);
        int numberOfVersionsToRetrieve = (int)(highestRequestedVersion - lowestRequestedVersion + 1);
        Result result = getRow(recordId, highestRequestedVersion, numberOfVersionsToRetrieve, fields);
        Long latestVersion = getLatestVersion(result);
        
        // Drop the versions that are higher than the latestVersion
        List<Long> validVersions = new ArrayList<Long>();
        for (Long version : versions) {
            if (version > latestVersion)
                break;
            validVersions.add(version);
        }
        return getRecordsFromRowResult(recordId, validVersions, result, fieldTypes);
    }
    
    @Override
    public IdRecord readWithIds(RecordId recordId, Long version, List<SchemaId> fieldIds)
            throws RepositoryException, InterruptedException {
        ReadContext readContext = new ReadContext();

        FieldTypes fieldTypes = typeManager.getFieldTypesSnapshot();
        List<FieldType> fields = getFieldTypesFromIds(fieldIds, fieldTypes);

        Record record = read(recordId, version, fields, readContext, fieldTypes);

        Map<SchemaId, QName> idToQNameMapping = new HashMap<SchemaId, QName>();
        for (FieldType fieldType : readContext.getFieldTypes().values()) {
            idToQNameMapping.put(fieldType.getId(), fieldType.getName());
        }

        Map<Scope, SchemaId> recordTypeIds = new EnumMap<Scope, SchemaId>(Scope.class);
        for (Map.Entry<Scope, RecordType> entry : readContext.getRecordTypes().entrySet()) {
            recordTypeIds.put(entry.getKey(), entry.getValue().getId());
        }

        return new IdRecordImpl(record, idToQNameMapping, recordTypeIds);
    }
    
    private Record read(RecordId recordId, Long requestedVersion, List<FieldType> fields, ReadContext readContext,
            FieldTypes fieldTypes) throws RepositoryException, InterruptedException {
        long before = System.currentTimeMillis();
        try {
            ArgumentValidator.notNull(recordId, "recordId");
    
            Result result = getRow(recordId, requestedVersion, 1, fields);
    
            Long latestVersion = getLatestVersion(result);
            if (requestedVersion == null) {
                // Latest version can still be null if there are only non-versioned fields in the record
                requestedVersion = latestVersion; 
            } else {
                if (latestVersion == null || latestVersion < requestedVersion ) {
                    // The requested version is higher than the highest existing version
                    throw new VersionNotFoundException(recordId, requestedVersion);
                }
            }
            return getRecordFromRowResult(recordId, requestedVersion, readContext, result, fieldTypes);
        } finally {
            metrics.report(Action.READ, System.currentTimeMillis() - before);
        }
    }
    
    private List<Record> read(List<RecordId> recordIds, List<FieldType> fields, FieldTypes fieldTypes) 
            throws RepositoryException, InterruptedException {
        long before = System.currentTimeMillis();
        try {
            ArgumentValidator.notNull(recordIds, "recordIds");
            List<Record> records = new ArrayList<Record>();
            if (recordIds.isEmpty())
                return records;
        
            Map<RecordId, Result> results = getRows(recordIds, fields);
        
            for (Entry<RecordId, Result> entry : results.entrySet()) {
                Long version = getLatestVersion(entry.getValue());
                records.add(getRecordFromRowResult(entry.getKey(), version, null, entry.getValue(), fieldTypes));
            }
            return records;
        } finally {
            metrics.report(Action.READ, System.currentTimeMillis() - before);
        }
    }

    private Long getLatestVersion(Result result) {
        byte[] latestVersionBytes = getLatest(result, RecordCf.DATA.bytes, RecordColumn.VERSION.bytes);
        Long latestVersion = latestVersionBytes != null ? Bytes.toLong(latestVersionBytes) : null;
        return latestVersion;
    }

    // Retrieves the row from the table and check if it exists and has not been flagged as deleted
    private Result getRow(RecordId recordId, Long version, int numberOfVersions, List<FieldType> fields)
            throws RecordException {
        Result result;
        Get get = new Get(recordId.toBytes());

        try {
            // Add the columns for the fields to get
            addFieldsToGet(get, fields);
            
            if (version != null)
                get.setTimeRange(0, version+1); // Only retrieve data within this timerange
            get.setMaxVersions(numberOfVersions); 
            
            // Retrieve the data from the repository
            result = recordTable.get(get);
            
            if (result == null || result.isEmpty())
                throw new RecordNotFoundException(recordId);
            
            // Check if the record was deleted
            byte[] deleted = getLatest(result, RecordCf.DATA.bytes, RecordColumn.DELETED.bytes);
            if ((deleted == null) || (Bytes.toBoolean(deleted))) {
                throw new RecordNotFoundException(recordId);
            }
        } catch (IOException e) {
            throw new RecordException("Exception occurred while retrieving record '" + recordId
                    + "' from HBase table", e);
        }
        return result;
    }

    /**
     * Gets the latest value for a family/qualifier from a Result object, using its
     * getMap(). Most of the time this will be more efficient than using
     * Result.getValue() since the map will need to built anyway when reading the
     * record.
     */
    private byte[] getLatest(Result result, byte[] family, byte[] qualifier) {
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result.getMap();
        if (map == null)
            return null;
        
        NavigableMap<byte[], NavigableMap<Long, byte[]>> qualifiers = map.get(family);
        if (qualifiers == null)
            return null;
        
        NavigableMap<Long, byte[]> timestamps = qualifiers.get(qualifier);
        if (timestamps == null)
            return null;
        
        Map.Entry<Long, byte[]> entry = timestamps.lastEntry();        
        return entry == null ? null : entry.getValue();
    }    
    
 // Retrieves the row from the table and check if it exists and has not been flagged as deleted
    private Map<RecordId, Result> getRows(List<RecordId> recordIds, List<FieldType> fields)
            throws RecordException {
        Map<RecordId, Result> results = new HashMap<RecordId, Result>();
        
        try {
            List<Get> gets = new ArrayList<Get>();
            for (RecordId recordId : recordIds) {
                Get get = new Get(recordId.toBytes());
                // Add the columns for the fields to get
                addFieldsToGet(get, fields);
                get.setMaxVersions(1); // Only retrieve the most recent version of each field
                gets.add(get);
            }

            // Retrieve the data from the repository
            int i = 0;
            for (Result result : recordTable.get(gets)) {
                if (result == null || result.isEmpty()) {
                    i++; // Skip this recordId (instead of throwing a RecordNotFoundException
                    continue;
                }
                // Check if the record was deleted
                byte[] deleted = getLatest(result, RecordCf.DATA.bytes, RecordColumn.DELETED.bytes);
                if ((deleted == null) || (Bytes.toBoolean(deleted))) {
                    i++; // Skip this recordId (instead of throwing a RecordNotFoundException
                    continue;
                }
                results.put(recordIds.get(i++), result);
            } 
        } catch (IOException e) {
            throw new RecordException("Exception occurred while retrieving records '" + recordIds
                    + "' from HBase table", e);
        }
        return results;
    }
    
    /**
     * Extracts the latest record type for a specific scope from the Result.
     */
    private Pair<SchemaId, Long> extractLatestRecordType(Scope scope, Result result) {
        byte[] idBytes = getLatest(result, RecordCf.DATA.bytes, recordTypeIdColumnNames.get(scope));
        byte[] versionBytes = getLatest(result, RecordCf.DATA.bytes, recordTypeVersionColumnNames.get(scope));
        if ((idBytes == null || idBytes.length == 0) || (versionBytes == null || versionBytes.length == 0))
            return null; // No record type was found
        return new Pair<SchemaId, Long>(new SchemaIdImpl(idBytes), Bytes.toLong(versionBytes));
    }

    /**
     * Extracts the record type for a specific version and a specific scope
     */
    private Pair<SchemaId, Long> extractVersionRecordType(Scope scope, Result result, Long version) {
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> allVersionsMap = result.getMap();
        NavigableMap<byte[], NavigableMap<Long, byte[]>> allColumnsAllVersionsMap = allVersionsMap
                .get(RecordCf.DATA.bytes);
        
        byte[] recordTypeIdColumnName = recordTypeIdColumnNames.get(scope);
        byte[] recordTypeVersionColumnName = recordTypeVersionColumnNames.get(scope);
        // Get recordTypeId
        Entry<Long, byte[]> idCeilingEntry = null;
        NavigableMap<Long, byte[]> recordTypeIdMap = allColumnsAllVersionsMap.get(recordTypeIdColumnName);
        if (recordTypeIdMap != null) {
            idCeilingEntry = recordTypeIdMap.ceilingEntry(version);
        }
        SchemaId recordTypeId;
        if (idCeilingEntry == null)
            return null; // No record type was found
        recordTypeId = new SchemaIdImpl(idCeilingEntry.getValue());
        
        // Get recordTypeVersion
        Long recordTypeVersion;
        Entry<Long, byte[]> versionCeilingEntry = null;
        NavigableMap<Long, byte[]> recordTypeVersionMap = allColumnsAllVersionsMap.get(recordTypeVersionColumnName);
        if (recordTypeVersionMap != null) {
            versionCeilingEntry = recordTypeVersionMap.ceilingEntry(version);
        }
        if (versionCeilingEntry == null)
            return null; // No record type was found, we should never get here: if there is an id there should also be a version
        recordTypeVersion = Bytes.toLong(versionCeilingEntry.getValue());
        Pair<SchemaId, Long> recordType = new Pair<SchemaId, Long>(recordTypeId, recordTypeVersion);
        return recordType;
    }

    /**
     *  Gets the requested version of the record (fields and recordTypes) from the Result object. 
     */
    private Record getRecordFromRowResult(RecordId recordId, Long requestedVersion, ReadContext readContext,
            Result result, FieldTypes fieldTypes) throws InterruptedException, RepositoryException {
        Record record = newRecord(recordId);
        record.setVersion(requestedVersion);
        Set<Scope> scopes = EnumSet.noneOf(Scope.class); // Set of scopes for which a field has been read

        // If the version is null, this means the record has no version an thus only contains non-versioned fields (if any)
        // All non-versioned fields are stored at version 1, so we extract the fields at version 1
        Long versionToRead = (requestedVersion == null) ? 1L : requestedVersion;

        // Get a map of all fields with their values for each (cell-)version
        NavigableMap<byte[], NavigableMap<Long, byte[]>> mapWithVersions = result.getMap().get(RecordCf.DATA.bytes); 
        if (mapWithVersions != null) {
            // Iterate over all columns
            for (Entry<byte[], NavigableMap<Long, byte[]>> columnWithAllVersions : mapWithVersions.entrySet()) {
                // Check if the retrieved column is from a data field, and not a system field
                byte[] key = columnWithAllVersions.getKey();
                if (key[0] == RecordColumn.DATA_PREFIX) {
                    NavigableMap<Long, byte[]> allValueVersions = columnWithAllVersions.getValue();
                    // Get the entry for the version (can be a cell with a lower version number if the field was not changed)
                    Entry<Long, byte[]> ceilingEntry = allValueVersions.ceilingEntry(versionToRead);
                    if (ceilingEntry != null) {
                        // Extract and decode the value of the field
                        Pair<FieldType, Object> field = extractField(key, ceilingEntry.getValue(), readContext, fieldTypes);
                        if (field != null) {
                            record.setField(field.getV1().getName(), field.getV2());
                            scopes.add(field.getV1().getScope());
                        }
                    }
                }
            }
        }
        
        // We're only adding the record types if any fields were read.
        if (!scopes.isEmpty()) {
            if (requestedVersion == null) {
                // There can only be non-versioned fields, so only read the non-versioned record type
                Pair<SchemaId, Long> recordTypePair = extractLatestRecordType(Scope.NON_VERSIONED, result);
                if (recordTypePair != null) {
                    RecordType recordType = typeManager.getRecordTypeById(recordTypePair.getV1(), recordTypePair.getV2());
                    record.setRecordType(recordType.getName(), recordType.getVersion());
                    if (readContext != null)
                        readContext.setRecordTypeId(Scope.NON_VERSIONED, recordType);
                }            
            } else {
                // Get the record type for each scope 
                // At least the non-versioned record type should be read since that is also the record type of the whole record 
                scopes.add(Scope.NON_VERSIONED); 
                for (Scope scope : scopes) {
                    Pair<SchemaId, Long> recordTypePair = extractVersionRecordType(scope, result, requestedVersion);
                    if (recordTypePair != null) {
                        RecordType recordType = typeManager.getRecordTypeById(recordTypePair.getV1(), recordTypePair.getV2());
                        record.setRecordType(scope, recordType.getName(), recordType.getVersion());
                        if (readContext != null)
                            readContext.setRecordTypeId(scope, recordType);
                    }
                }
            }
        }
        
        return record;
    }
    
    /**
     *  Gets the requested version of the record (fields and recordTypes) from the Result object.
     *  This method is optimized for reading multiple versions.
     */
    private List<Record> getRecordsFromRowResult(RecordId recordId, List<Long> requestedVersions, Result result,
            FieldTypes fieldTypes) throws InterruptedException, RepositoryException{
        Map<Long, Record> records = new HashMap<Long, Record>(requestedVersions.size());
        Map<Long, Set<Scope>> scopes = new HashMap<Long, Set<Scope>>(requestedVersions.size());
        for (Long requestedVersion : requestedVersions) {
            Record record = newRecord(recordId);
            record.setVersion(requestedVersion);
            records.put(requestedVersion, record);
            scopes.put(requestedVersion, EnumSet.noneOf(Scope.class));
        }

        // Get a map of all fields with their values for each (cell-)version
        NavigableMap<byte[], NavigableMap<Long, byte[]>> mapWithVersions = result.getMap().get(RecordCf.DATA.bytes); 
        if (mapWithVersions != null) {
            
            // Iterate over all columns
            for (Entry<byte[], NavigableMap<Long, byte[]>> columnWithAllVersions : mapWithVersions.entrySet()) {
            
                // Check if the retrieved column is from a data field, and not a system field
                byte[] key = columnWithAllVersions.getKey();
                if (key[0] == RecordColumn.DATA_PREFIX) {
                    NavigableMap<Long, byte[]> allValueVersions = columnWithAllVersions.getValue();
                
                    // Keep the last decoded field value, to avoid decoding the same value again and again if unchanged
                    // between versions (sparse storage). Note that lastDecodedField can be null, in case of a field
                    // deletion marker
                    Long lastDecodedFieldVersion = null;
                    Pair<FieldType, Object> lastDecodedField = null;
                    for (Long versionToRead : requestedVersions) {
                        Record record = records.get(versionToRead);
                        // Get the entry for the version (can be a cell with a lower version number if the field was
                        // not changed)
                        Entry<Long, byte[]> ceilingEntry = allValueVersions.ceilingEntry(versionToRead);
                        if (ceilingEntry != null) {
                            if (lastDecodedFieldVersion == null ||
                                    !lastDecodedFieldVersion.equals(ceilingEntry.getKey())) {
                                // Not yet decoded, do it now
                                lastDecodedFieldVersion = ceilingEntry.getKey();
                                lastDecodedField = extractField(key, ceilingEntry.getValue(), null, fieldTypes);
                            }
                            if (lastDecodedField != null) {
                                record.setField(lastDecodedField.getV1().getName(), lastDecodedField.getV2());
                                scopes.get(versionToRead).add(lastDecodedField.getV1().getScope());
                            }
                        }
                    }
                }
            }
        }
        
        // Add the record types to the records
        for (Entry<Long, Record> recordEntry : records.entrySet()) {
            Set<Scope> scopesForVersion = scopes.get(recordEntry.getKey());
            
            // We're only adding the record types if any fields were read.
            if (!scopesForVersion.isEmpty()) {
                // Get the record type for each scope 

                // At least the non-versioned record type should be read since that is also the record type of the whole record 
                scopesForVersion.add(Scope.NON_VERSIONED); 
                for (Scope scope : scopesForVersion) {
                    Pair<SchemaId, Long> recordTypePair = extractVersionRecordType(scope, result, recordEntry.getKey());
                    if (recordTypePair != null) {
                        RecordType recordType = typeManager.getRecordTypeById(recordTypePair.getV1(), recordTypePair.getV2());
                        recordEntry.getValue().setRecordType(scope, recordType.getName(), recordType.getVersion());
                    }
                }
            }
        }
        
        return new ArrayList<Record>(records.values());
    }
    
    private Pair<FieldType, Object> extractField(byte[] key, byte[] prefixedValue, ReadContext context,
            FieldTypes fieldTypes) throws RecordException, TypeException, RepositoryException, InterruptedException {
        byte prefix = prefixedValue[0];
        if (LilyHBaseSchema.DELETE_FLAG == prefix) {
            return null;
        }
        FieldType fieldType = fieldTypes.getFieldType(new SchemaIdImpl(Bytes.tail(key, key.length-1)));
        if (context != null) 
            context.addFieldType(fieldType);
        ValueType valueType = fieldType.getValueType();
        Object value = valueType.read(EncodingUtil.stripPrefix(prefixedValue));
        return new Pair<FieldType, Object>(fieldType, value);
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

    @Override
    public void delete(RecordId recordId) throws RepositoryException {
        delete(recordId, null);
    }

    @Override
    public Record delete(RecordId recordId, List<MutationCondition> conditions)
            throws RepositoryException {
        ArgumentValidator.notNull(recordId, "recordId");
        long before = System.currentTimeMillis();
        RowLock rowLock = null;
        byte[] rowId = recordId.toBytes();
        try {
            // Take Custom Lock
            rowLock = lockRow(recordId);

            // We need to read the original record in order to put the delete marker in the non-versioned fields.
            // Throw RecordNotFoundException if there is no record to be deleted
            FieldTypes fieldTypes = typeManager.getFieldTypesSnapshot();            
            Record originalRecord = read(recordId, null, null, null, fieldTypes); 

            if (conditions != null) {
                Record conditionsRecord = MutationConditionVerifier.checkConditions(originalRecord, conditions, this,
                        null);
                if (conditionsRecord != null) {
                    return conditionsRecord;
                }
            }

            Put put = new Put(rowId);
            // Mark the record as deleted
            put.add(RecordCf.DATA.bytes, RecordColumn.DELETED.bytes, 1L, Bytes.toBytes(true));
            
            // Put the delete marker in the non-versioned fields instead of deleting their columns in the clearData call
            // This is needed to avoid non-versioned fields to be lost due to the hbase delete thombstone
            // See trac ticket http://dev.outerthought.org/trac/outerthought_lilyproject/ticket/297
            Map<QName, Object> fields = originalRecord.getFields();
            for (Entry<QName, Object> fieldEntry : fields.entrySet()) {
                FieldTypeImpl fieldType = (FieldTypeImpl)fieldTypes.getFieldType(fieldEntry.getKey());
                if (Scope.NON_VERSIONED == fieldType.getScope()) {
                    put.add(RecordCf.DATA.bytes, fieldType.getQualifier(), 1L, DELETE_MARKER);
                }
                
            }
            
            RecordEvent recordEvent = new RecordEvent();
            recordEvent.setType(Type.DELETE);
            RowLogMessage walMessage = wal.putMessage(recordId.toBytes(), null, recordEvent.toJsonBytes(), put);
            if (!rowLocker.put(put, rowLock)) {
                throw new RecordException("Exception occurred while deleting record '" + recordId + "' on HBase table");
            }

            // Clear the old data and delete any referenced blobs
            clearData(recordId, originalRecord);

            if (walMessage != null) {
                try {
                    wal.processMessage(walMessage, rowLock);
                } catch (RowLogException e) {
                    // Processing the message failed, it will be retried later.
                }
            }
        } catch (RowLogException e) {
            throw new RecordException("Exception occurred while deleting record '" + recordId
                    + "' on HBase table", e);

        } catch (IOException e) {
            throw new RecordException("Exception occurred while deleting record '" + recordId + "' on HBase table",
                    e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RecordException("Exception occurred while deleting record '" + recordId + "' on HBase table",
                    e);
        } finally {
            unlockRow(rowLock);
            long after = System.currentTimeMillis();
            metrics.report(Action.DELETE, (after-before));
        }

        return null;
    }

    // Clear all data of the recordId until the latest record version (included)
    // And delete any referred blobs
    private void clearData(RecordId recordId, Record originalRecord) throws IOException, RepositoryException, InterruptedException {
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
                            SchemaId schemaId = new SchemaIdImpl(Bytes.tail(columnQualifier, columnQualifier.length - 1));
                            FieldType fieldType = typeManager.getFieldTypeById(schemaId);
                            ValueType valueType = fieldType.getValueType();
                            NavigableMap<Long,byte[]> cells = column.getValue();
                            Set<Entry<Long, byte[]>> cellsSet = cells.entrySet();
                            for (Entry<Long, byte[]> cell : cellsSet) {
                                // Get blobs to delete
                                if (valueType.getDeepestValueType() instanceof BlobValueType) {
                                    Object blobValue = null;
                                    if (fieldType.getScope() == Scope.NON_VERSIONED) {
                                        // Read the blob value from the original record, 
                                        // since the delete marker has already been put in the field by the delete call
                                        if (originalRecord != null)
                                            blobValue = originalRecord.getField(fieldType.getName());
                                    } else {
                                        byte[] value = cell.getValue();
                                        if (!isDeleteMarker(value)) {
                                            blobValue = valueType.read(EncodingUtil.stripPrefix(value));
                                        }
                                    }
                                    try {
                                        if (blobValue != null)
                                            blobsToDelete.addAll(getReferencedBlobs((FieldTypeImpl)fieldType, blobValue));
                                    } catch (BlobException e) {
                                        log.warn("Failure occured while clearing blob data", e);
                                        // We do a best effort here
                                    }
                                }
                                // Get cells to delete
                                // Only delete if in NON_VERSIONED scope
                                // The NON_VERSIONED fields will get filled in with a delete marker
                                // This is needed to avoid non-versioned fields to be lost due to the hbase delete thombstone
                                // See trac ticket http://dev.outerthought.org/trac/outerthought_lilyproject/ticket/297
                                if (fieldType.getScope() != Scope.NON_VERSIONED) { 
                                    delete.deleteColumn(RecordCf.DATA.bytes, columnQualifier, cell.getKey());
                                }
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
                // Do not delete the NON-VERSIONED record type column.
                // If the thombstone was not processed yet (major compaction)
                // a re-creation of the record would then loose its record type since the NON-VERSIONED 
                // field is always stored at timestamp 1L
                // Re-creating the record will always overwrite the (NON-VERSIONED) record type.
                // So, there is no risk of old record type information ending up in the new record.
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
                log.warn("Exception while unlocking row '" + Bytes.toStringBinary(rowLock.getRowKey()) + "'", e);
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
        if ((valueType.getDeepestValueType() instanceof BlobValueType) && ! isDeleteMarker(value)) {
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
    private Set<BlobReference> filterReferencedBlobs(RecordId recordId, Set<BlobReference> blobs, Long ignoreVersion)
            throws IOException {
        if (recordId == null)
            return blobs;
        Set<BlobReference> unReferencedBlobs = new HashSet<BlobReference>();
        for (BlobReference blobReference : blobs) {
            FieldTypeImpl fieldType = (FieldTypeImpl)blobReference.getFieldType();
            byte[] recordIdBytes = recordId.toBytes();
            ValueType valueType = fieldType.getValueType();

            Get get = new Get(recordIdBytes);
            get.addColumn(RecordCf.DATA.bytes, fieldType.getQualifier());
            byte[] valueToCompare = Bytes.toBytes(valueType.getNestingLevel());
            
            // Note, if a encoding of the BlobValueType is added, this might have to change.
            valueToCompare = Bytes.add(valueToCompare, blobReference.getBlob().getValue());
            WritableByteArrayComparable valueComparator = new ContainsValueComparator(valueToCompare);
            Filter filter = new SingleColumnValueFilter(RecordCf.DATA.bytes, fieldType.getQualifier(), CompareOp.EQUAL,
                    valueComparator);
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

    @Override
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
        private Map<Scope, RecordType> recordTypes = new EnumMap<Scope, RecordType>(Scope.class);
        private Set<Scope> scopes = EnumSet.noneOf(Scope.class);

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
    
    @Override
    public RecordBuilder recordBuilder() throws RecordException {
        return new RecordBuilderImpl(this);
    }
}
