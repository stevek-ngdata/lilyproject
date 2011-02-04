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
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilyproject.repository.api.Blob;
import org.lilyproject.repository.api.BlobException;
import org.lilyproject.repository.api.BlobInputStream;
import org.lilyproject.repository.api.BlobManager;
import org.lilyproject.repository.api.BlobNotFoundException;
import org.lilyproject.repository.api.BlobReference;
import org.lilyproject.repository.api.BlobStoreAccess;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.FieldTypeEntry;
import org.lilyproject.repository.api.FieldTypeNotFoundException;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.IdRecord;
import org.lilyproject.repository.api.InvalidRecordException;
import org.lilyproject.repository.api.PrimitiveValueType;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordException;
import org.lilyproject.repository.api.RecordExistsException;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.RecordLockedException;
import org.lilyproject.repository.api.RecordNotFoundException;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.RecordTypeNotFoundException;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.ResponseStatus;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.TypeException;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.api.ValueType;
import org.lilyproject.repository.api.VersionNotFoundException;
import org.lilyproject.repository.impl.RepositoryMetrics.Action;
import org.lilyproject.repository.impl.lock.RowLock;
import org.lilyproject.repository.impl.lock.RowLocker;
import org.lilyproject.repository.impl.primitivevaluetype.BlobValueType;
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
public class HBaseRepository implements Repository {
 
    private HTableInterface recordTable;
    private final TypeManager typeManager;
    private final IdGenerator idGenerator;
    private byte[] dataColumnFamily = RecordCf.DATA.bytes;
    private byte[] systemColumnFamily = RecordCf.SYSTEM.bytes;
    private Map<Scope, byte[]> recordTypeIdColumnNames = new HashMap<Scope, byte[]>();
    private Map<Scope, byte[]> recordTypeVersionColumnNames = new HashMap<Scope, byte[]>();
    private RowLog wal;
    private RowLocker rowLocker;
    
    private Log log = LogFactory.getLog(getClass());
    private RepositoryMetrics metrics;
    private BlobManager blobManager;

    public HBaseRepository(TypeManager typeManager, IdGenerator idGenerator,
            RowLog wal, Configuration configuration, HBaseTableFactory hbaseTableFactory, BlobManager blobManager) throws IOException {
        this.typeManager = typeManager;
        this.idGenerator = idGenerator;
        this.wal = wal;
        this.blobManager = blobManager;

        recordTable = LilyHBaseSchema.getRecordTable(hbaseTableFactory);

        recordTypeIdColumnNames.put(Scope.NON_VERSIONED, RecordColumn.NON_VERSIONED_RT_ID.bytes);
        recordTypeIdColumnNames.put(Scope.VERSIONED, RecordColumn.VERSIONED_RT_ID.bytes);
        recordTypeIdColumnNames.put(Scope.VERSIONED_MUTABLE, RecordColumn.VERSIONED_MUTABLE_RT_ID.bytes);
        recordTypeVersionColumnNames.put(Scope.NON_VERSIONED, RecordColumn.NON_VERSIONED_RT_VERSION.bytes);
        recordTypeVersionColumnNames.put(Scope.VERSIONED, RecordColumn.VERSIONED_RT_VERSION.bytes);
        recordTypeVersionColumnNames.put(Scope.VERSIONED_MUTABLE, RecordColumn.VERSIONED_MUTABLE_RT_VERSION.bytes);

        rowLocker = new RowLocker(recordTable, RecordCf.SYSTEM.bytes, RecordColumn.LOCK.bytes, 10000);
        metrics = new RepositoryMetrics("hbaserepository");
    }

    public void close() throws IOException {
    }

    public IdGenerator getIdGenerator() {
        return idGenerator;
    }

    public TypeManager getTypeManager() {
        return typeManager;
    }

    public Record newRecord() {
        return new RecordImpl();
    }

    public Record newRecord(RecordId recordId) {
        ArgumentValidator.notNull(recordId, "recordId");
        return new RecordImpl(recordId);
    }

    public Record createOrUpdate(Record record) throws FieldTypeNotFoundException, RecordException,
            RecordTypeNotFoundException, InvalidRecordException, TypeException,
            VersionNotFoundException, RecordLockedException {
        return createOrUpdate(record, true);
    }

    public Record createOrUpdate(Record record, boolean useLatestRecordType) throws FieldTypeNotFoundException,
            RecordException, RecordTypeNotFoundException, InvalidRecordException, TypeException,
            VersionNotFoundException, RecordLockedException {

        if (record.getId() == null) {
            // While we could generate an ID ourselves in this case, this would defeat partly the purpose of
            // createOrUpdate, which is that clients would be able to retry the operation (in case of IO exceptions)
            // without having to worry that more than one record might be created.
            throw new RecordException("Record ID is mandatory when using create-or-update.");
        }

        byte[] rowId = record.getId().toBytes();
        Get get = new Get(rowId);
        get.addColumn(RecordCf.SYSTEM.bytes, RecordColumn.DELETED.bytes);

        int attempts;
        
        for (attempts = 0; attempts < 3; attempts++) {            
            Result result;
            try {
                result = recordTable.get(get);
            } catch (IOException e) {
                throw new RecordException("Error reading record row for record id " + record.getId());
            }

            byte[] deleted = result.getValue(systemColumnFamily, RecordColumn.DELETED.bytes);
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
                // Lock the row
                rowLock = lockRow(recordId);

                long version = 1L;
                // If the record existed it would have been deleted.
                // The version numbering continues from where it has been deleted.
                Get get = new Get(rowId);
                get.addColumn(systemColumnFamily, RecordColumn.VERSION.bytes);
                Result result = recordTable.get(get);
                if (!result.isEmpty()) {
	                byte[] oldVersion = result.getValue(systemColumnFamily, RecordColumn.VERSION.bytes);
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
                put.add(systemColumnFamily, RecordColumn.DELETED.bytes, 1L, Bytes.toBytes(false));
                RecordEvent recordEvent = new RecordEvent();
                recordEvent.setType(Type.CREATE);
                Set<BlobReference> referencedBlobs = new HashSet<BlobReference>();
                Set<BlobReference> unReferencedBlobs = new HashSet<BlobReference>();
                
                calculateRecordChanges(newRecord, dummyOriginalRecord, version, put, recordEvent, referencedBlobs, unReferencedBlobs, false);

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

    public Record update(Record record, boolean updateVersion, boolean useLatestRecordType)
            throws InvalidRecordException, RecordNotFoundException, RecordTypeNotFoundException,
            FieldTypeNotFoundException, RecordException, VersionNotFoundException, TypeException,
            RecordLockedException {

        long before = System.currentTimeMillis();
        try {
            if (record.getId() == null) {
                throw new InvalidRecordException("The recordId cannot be null for a record to be updated.", record.getId());
            }
            try {
                if (!checkAndProcessOpenMessages(record.getId())) {
                    throw new RecordException("Record <"+ record.getId()+"> update could not be performed due to remaining messages on the WAL");
                }
            }  catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RecordException("Exception occurred while updating record <" + record.getId() + ">",
                        e);
            }
            
            if (updateVersion) {
                try {
                    return updateMutableFields(record, useLatestRecordType);
                } catch (BlobException e) {
                    throw new RecordException("Exception occurred while updating record <" + record.getId() + ">",
                            e);
                }
            } else {
                return updateRecord(record, useLatestRecordType);
            }
        } finally {
            metrics.report(Action.UPDATE, System.currentTimeMillis() - before);
        }
    }
    
    public Record update(Record record) throws InvalidRecordException, RecordNotFoundException,
            RecordTypeNotFoundException, FieldTypeNotFoundException, RecordException, VersionNotFoundException,
            TypeException, RecordLockedException {
        return update(record, false, true);
    }
    
    private Record updateRecord(Record record, boolean useLatestRecordType) throws RecordNotFoundException,
            InvalidRecordException, RecordTypeNotFoundException, FieldTypeNotFoundException, RecordException,
            VersionNotFoundException, TypeException, RecordLockedException {
        Record newRecord = record.clone();

        RecordId recordId = record.getId();
        org.lilyproject.repository.impl.lock.RowLock rowLock = null;
        try {
            // Take Custom Lock
            rowLock = lockRow(recordId);

            Record originalRecord = read(newRecord.getId(), null, null, new ReadContext());

            Put put = new Put(newRecord.getId().toBytes());
            Set<BlobReference> referencedBlobs = new HashSet<BlobReference>();
            Set<BlobReference> unReferencedBlobs = new HashSet<BlobReference>();
            RecordEvent recordEvent = new RecordEvent();
            recordEvent.setType(Type.UPDATE);
            long newVersion = originalRecord.getVersion() == null ? 1 : originalRecord.getVersion() + 1;
                
                if (calculateRecordChanges(newRecord, originalRecord, newVersion, put, recordEvent, referencedBlobs, unReferencedBlobs, useLatestRecordType)) {
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
        } finally {
            unlockRow(rowLock);
        }

        newRecord.getFieldsToDelete().clear();
        return newRecord;
    }

    private void putRowWithWalProcessing(RecordId recordId, org.lilyproject.repository.impl.lock.RowLock rowLock,
            Put put, RecordEvent recordEvent) throws InterruptedException, RowLogException, IOException, RecordException {
        RowLogMessage walMessage;
        walMessage = wal.putMessage(recordId.toBytes(), null, recordEvent.toJsonBytes(), put);
        if (!rowLocker.put(put, rowLock)) {
            throw new RecordException("Exception occurred while putting updated record <" + recordId
                    + "> on HBase table", null);
        }

        if (walMessage != null) {
            try {
                wal.processMessage(walMessage);
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
            RecordEvent recordEvent, Set<BlobReference> referencedBlobs, Set<BlobReference> unReferencedBlobs, boolean useLatestRecordType) throws InterruptedException, RecordTypeNotFoundException, TypeException, FieldTypeNotFoundException, BlobException, RecordException, InvalidRecordException {
        QName recordTypeName = record.getRecordTypeName();
        Long recordTypeVersion = null;
        if (recordTypeName == null) {
            recordTypeName = originalRecord.getRecordTypeName();
        } else {
            recordTypeVersion = useLatestRecordType ? null : record.getRecordTypeVersion();
        }

        RecordType recordType = typeManager.getRecordTypeByName(recordTypeName, recordTypeVersion);
        
        // Check which fields have changed
        Set<Scope> changedScopes = calculateChangedFields(record, originalRecord, recordType, version, put, recordEvent, referencedBlobs, unReferencedBlobs);

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
                put.add(systemColumnFamily, RecordColumn.NON_VERSIONED_RT_ID.bytes, 1L, Bytes
                        .toBytes(recordType.getId()));
                put.add(systemColumnFamily, RecordColumn.NON_VERSIONED_RT_VERSION.bytes, 1L,
                        Bytes.toBytes(recordTypeVersion));
            }
            // Always set the record type on the record since the requested
            // record type could have been given without a version number
            record.setRecordType(recordTypeName, recordTypeVersion);
            if (version != null) {
                byte[] versionBytes = Bytes.toBytes(version);
                put.add(systemColumnFamily, RecordColumn.VERSION.bytes, 1L, versionBytes);
                if (VersionTag.hasLastVTag(recordType, typeManager) || VersionTag.hasLastVTag(record, typeManager) || VersionTag.hasLastVTag(originalRecord, typeManager)) {
                    FieldTypeImpl lastVTagType = (FieldTypeImpl)VersionTag.getLastVTagType(typeManager);
                    put.add(dataColumnFamily, lastVTagType.getIdBytes(), 1L, encodeFieldValue(lastVTagType, version));
                    record.setField(lastVTagType.getName(), version);
                }
            }
            validateRecord(record, originalRecord, recordType);

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
    
    private void validateRecord(Record record, Record originalRecord, RecordType recordType) throws
            FieldTypeNotFoundException, TypeException, InvalidRecordException, InterruptedException {
        // Check mandatory fields
        Collection<FieldTypeEntry> fieldTypeEntries = recordType.getFieldTypeEntries();
        List<QName> fieldsToDelete = record.getFieldsToDelete();
        for (FieldTypeEntry fieldTypeEntry : fieldTypeEntries) {
            if (fieldTypeEntry.isMandatory()) {
                FieldType fieldType = typeManager.getFieldTypeById(fieldTypeEntry.getFieldTypeId());
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

    private Set<Scope> calculateChangedFields(Record record, Record originalRecord, RecordType recordType,
            Long version, Put put, RecordEvent recordEvent, Set<BlobReference> referencedBlobs, Set<BlobReference> unReferencedBlobs) throws InterruptedException, FieldTypeNotFoundException, TypeException, BlobException, RecordTypeNotFoundException, RecordException {
        Map<QName, Object> originalFields = originalRecord.getFields();
        Set<Scope> changedScopes = new HashSet<Scope>();
        
        Map<QName, Object> fields = getFieldsToUpdate(record);
        
        changedScopes.addAll(calculateUpdateFields(fields, originalFields, null, version, put, recordEvent, referencedBlobs, unReferencedBlobs, false));
        for (BlobReference referencedBlob : referencedBlobs) {
            referencedBlob.setRecordId(record.getId());
        }
        for (BlobReference unReferencedBlob : unReferencedBlobs) {
            unReferencedBlob.setRecordId(record.getId());
        }
        // Update record types
        for (Scope scope : changedScopes) {
            if (Scope.NON_VERSIONED.equals(scope)) {
                put.add(systemColumnFamily, recordTypeIdColumnNames.get(scope), 1L, Bytes.toBytes(recordType
                        .getId()));
                put.add(systemColumnFamily, recordTypeVersionColumnNames.get(scope), 1L, Bytes
                        .toBytes(recordType.getVersion()));
            } else {
                put.add(systemColumnFamily, recordTypeIdColumnNames.get(scope), version, Bytes
                        .toBytes(recordType.getId()));
                put.add(systemColumnFamily, recordTypeVersionColumnNames.get(scope), version, Bytes
                        .toBytes(recordType.getVersion()));

            }
            record.setRecordType(scope, recordType.getName(), recordType.getVersion());
        }
        return changedScopes;
    }

    private Map<QName, Object> getFieldsToUpdate(Record record) {
        // Work with a copy of the map
        Map<QName, Object> fields = new HashMap<QName, Object>();
        fields.putAll(record.getFields());
        for (QName qName : record.getFieldsToDelete()) {
            fields.put(qName, DELETE_MARKER);
        }
        return fields;
    }

    private Set<Scope> calculateUpdateFields(Map<QName, Object> fields, Map<QName, Object> originalFields, Map<QName, Object> originalNextFields,
            Long version, Put put, RecordEvent recordEvent, Set<BlobReference> referencedBlobs, Set<BlobReference> unReferencedBlobs, boolean mutableUpdate) throws InterruptedException, FieldTypeNotFoundException, TypeException, BlobException, RecordTypeNotFoundException, RecordException {
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
                FieldTypeImpl fieldType = (FieldTypeImpl)typeManager.getFieldTypeByName(fieldName);
                Scope scope = fieldType.getScope();
                byte[] fieldIdAsBytes = fieldType.getIdBytes();
                
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
                    put.add(dataColumnFamily, fieldIdAsBytes, 1L, encodedFieldValue);
                } else {
                    put.add(dataColumnFamily, fieldIdAsBytes, version, encodedFieldValue);
                    if (originalNextFields != null && !fieldIsNewOrDeleted && originalNextFields.containsKey(fieldName)) {
                        copyValueToNextVersionIfNeeded(version, put, originalNextFields, fieldName, originalValue);
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

    private Record updateMutableFields(Record record, boolean latestRecordType) throws InvalidRecordException,
            RecordNotFoundException, RecordTypeNotFoundException, FieldTypeNotFoundException, RecordException,
            VersionNotFoundException, TypeException, RecordLockedException, BlobException {

        Record newRecord = record.clone();

        RecordId recordId = record.getId();
        org.lilyproject.repository.impl.lock.RowLock rowLock = null;
        
        Long version = record.getVersion();
        if (version == null) {
            throw new InvalidRecordException("The version of the record cannot be null to update mutable fields",
                    record.getId());
        }

        try {
            // Take Custom Lock
            rowLock = lockRow(recordId);
            
            Record originalRecord = read(recordId, version, null, new ReadContext());

            // Update the mutable fields
            Put put = new Put(recordId.toBytes());
            Map<QName, Object> fields = getFieldsToUpdate(record);
            fields = filterMutableFields(fields);
            Map<QName, Object> originalFields = filterMutableFields(originalRecord.getFields());
            Set<BlobReference> referencedBlobs = new HashSet<BlobReference>();
            Set<BlobReference> unReferencedBlobs = new HashSet<BlobReference>();
            RecordEvent recordEvent = new RecordEvent();
            recordEvent.setType(Type.UPDATE);
            recordEvent.setVersionUpdated(version);

            Set<Scope> changedScopes = calculateUpdateFields(fields, originalFields, getOriginalNextFields(recordId, version), version, put, recordEvent, referencedBlobs, unReferencedBlobs, true);
            for (BlobReference referencedBlob : referencedBlobs) {
                referencedBlob.setRecordId(recordId);
            }
            for (BlobReference unReferencedBlob : unReferencedBlobs) {
                unReferencedBlob.setRecordId(recordId);
            }
            
            if (!changedScopes.isEmpty()) {
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
                
                // Update the mutable record type
                Scope scope = Scope.VERSIONED_MUTABLE;
                newRecord.setRecordType(scope, recordType.getName(), recordType.getVersion());
                
                validateRecord(newRecord, originalRecord, recordType);

                put.add(systemColumnFamily, recordTypeIdColumnNames.get(scope), version, Bytes
                        .toBytes(recordType.getId()));
                put.add(systemColumnFamily, recordTypeVersionColumnNames.get(scope), version, Bytes
                        .toBytes(recordType.getVersion()));

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

    private Map<QName, Object> filterMutableFields(Map<QName, Object> fields) throws FieldTypeNotFoundException,
            RecordTypeNotFoundException, RecordException, TypeException, InterruptedException {
        Map<QName, Object> mutableFields = new HashMap<QName, Object>();
        for (Entry<QName, Object> field : fields.entrySet()) {
            FieldType fieldType = typeManager.getFieldTypeByName(field.getKey());
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
            QName fieldName, Object originalValue)
            throws FieldTypeNotFoundException, RecordTypeNotFoundException, RecordException, TypeException,
            InterruptedException {
        Object originalNextValue = originalNextFields.get(fieldName);
        if ((originalValue == null && originalNextValue == null) || originalValue.equals(originalNextValue)) {
            FieldTypeImpl fieldType = (FieldTypeImpl)typeManager.getFieldTypeByName(fieldName);
            byte[] encodedValue = encodeFieldValue(fieldType, originalValue);
            put.add(dataColumnFamily, fieldType.getIdBytes(), version + 1, encodedValue);
        }
    }

    private Map<QName, Object> getOriginalNextFields(RecordId recordId, Long version)
            throws RecordTypeNotFoundException, FieldTypeNotFoundException, RecordException,
            TypeException, RecordNotFoundException, InterruptedException {
        try {
            Record originalNextRecord = read(recordId, version + 1, null, new ReadContext());
            return filterMutableFields(originalNextRecord.getFields());
        } catch (VersionNotFoundException exception) {
            // There is no next record
            return null;
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
        List<FieldType> fields = getFieldTypesFromNames(fieldNames);

        return read(recordId, version, fields, new ReadContext());
    }

    private List<FieldType> getFieldTypesFromNames(List<QName> fieldNames) throws FieldTypeNotFoundException,
            TypeException, InterruptedException {
        List<FieldType> fields = null;
        if (fieldNames != null) {
            fields = new ArrayList<FieldType>();
            for (QName fieldName : fieldNames) {
                fields.add(typeManager.getFieldTypeByName(fieldName));
            }
        }
        return fields;
    }
    
    private List<FieldType> getFieldTypesFromIds(List<String> fieldIds) throws FieldTypeNotFoundException,
            TypeException, InterruptedException {
        List<FieldType> fields = null;
        if (fieldIds != null) {
            fields = new ArrayList<FieldType>(fieldIds.size());
            for (String fieldId : fieldIds) {
                fields.add(typeManager.getFieldTypeById(fieldId));
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

        List<FieldType> fields = getFieldTypesFromNames(fieldNames);

        Result result = getRow(recordId, toVersion, true, fields);
        if (fromVersion < 1L)
            fromVersion = 1L;
        Long latestVersion = getLatestVersion(result);
        if (latestVersion < toVersion)
            toVersion = latestVersion;
        List<Record> records = new ArrayList<Record>();
        for (long version = fromVersion; version <= toVersion; version++) {
            records.add(getRecordFromRowResult(recordId, version, new ReadContext(), result));
        }
        return records;
    }
    
    public IdRecord readWithIds(RecordId recordId, Long version, List<String> fieldIds) throws RecordNotFoundException,
            RecordTypeNotFoundException, FieldTypeNotFoundException, RecordException, VersionNotFoundException,
            TypeException, InterruptedException {
        ReadContext readContext = new ReadContext();

        List<FieldType> fields = getFieldTypesFromIds(fieldIds);

        Record record = read(recordId, version, fields, readContext);

        Map<String, QName> idToQNameMapping = new HashMap<String, QName>();
        for (FieldType fieldType : readContext.getFieldTypes().values()) {
            idToQNameMapping.put(fieldType.getId(), fieldType.getName());
        }

        Map<Scope, String> recordTypeIds = new HashMap<Scope, String>();
        for (Map.Entry<Scope, RecordType> entry : readContext.getRecordTypes().entrySet()) {
            recordTypeIds.put(entry.getKey(), entry.getValue().getId());
        }

        return new IdRecordImpl(record, idToQNameMapping, recordTypeIds);
    }
    
    private Record read(RecordId recordId, Long requestedVersion, List<FieldType> fields, ReadContext readContext)
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
            return getRecordFromRowResult(recordId, requestedVersion, readContext, result);
        } finally {
            metrics.report(Action.READ, System.currentTimeMillis() - before);
        }
    }

    private Record getRecordFromRowResult(RecordId recordId, Long requestedVersion, ReadContext readContext,
            Result result) throws VersionNotFoundException, RecordTypeNotFoundException, FieldTypeNotFoundException,
            RecordException, TypeException, InterruptedException {
        Record record = newRecord(recordId);
        record.setVersion(requestedVersion);

        // Extract the actual fields from the retrieved data
        if (extractFieldsAndRecordTypes(result, requestedVersion, record, readContext)) {
            // Set the recordType explicitly in case only versioned fields were
            // extracted
            Pair<String, Long> recordTypePair = extractRecordType(Scope.NON_VERSIONED, result, null, record);
            RecordType recordType = typeManager.getRecordTypeById(recordTypePair.getV1(), recordTypePair.getV2());
            record.setRecordType(recordType.getName(), recordType.getVersion());
            readContext.setRecordTypeId(Scope.NON_VERSIONED, recordType);
        }
        return record;
    }

    private Long getLatestVersion(Result result) {
        byte[] latestVersionBytes = result.getValue(systemColumnFamily, RecordColumn.VERSION.bytes);
        Long latestVersion = latestVersionBytes != null ? Bytes.toLong(latestVersionBytes) : null;
        return latestVersion;
    }

    // Retrieves the row from the table and check if it exists and has not been flagged as deleted
    private Result getRow(RecordId recordId, Long version, boolean multipleVersions, List<FieldType> fields)
            throws RecordNotFoundException, RecordException {
        Result result;
        Get get = new Get(recordId.toBytes());
        get.addFamily(dataColumnFamily);
        get.addFamily(systemColumnFamily);
        
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
            byte[] deleted = result.getValue(systemColumnFamily, RecordColumn.DELETED.bytes);
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
        
        get.addColumn(systemColumnFamily, RecordColumn.DELETED.bytes);
        Result result = recordTable.get(get);
        if (result == null || result.isEmpty()) {
            return false;
        } else {
            byte[] deleted = result.getValue(systemColumnFamily, RecordColumn.DELETED.bytes);
            if ((deleted == null) || (Bytes.toBoolean(deleted))) {
                return false;
            } else {
                return true;
            }
        }
    }

    private Pair<String, Long> extractRecordType(Scope scope, Result result, Long version, Record record) {
        if (version == null) {
            // Get latest version
            return new Pair<String, Long>(Bytes.toString(result.getValue(systemColumnFamily,
                    recordTypeIdColumnNames.get(scope))), Bytes.toLong(result.getValue(systemColumnFamily,
                    recordTypeVersionColumnNames.get(scope))));

        } else {
            // Get on version
            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> allVersionsMap = result.getMap();
            NavigableMap<byte[], NavigableMap<Long, byte[]>> versionableSystemCFversions = allVersionsMap
                    .get(systemColumnFamily);
            return extractVersionRecordType(version, versionableSystemCFversions, scope);
        }
    }

    private Pair<String, Long> extractVersionRecordType(Long version,
            NavigableMap<byte[], NavigableMap<Long, byte[]>> versionableSystemCFversions, Scope scope) {
        byte[] recordTypeIdColumnName = recordTypeIdColumnNames.get(scope);
        byte[] recordTypeVersionColumnName = recordTypeVersionColumnNames.get(scope);
        Entry<Long, byte[]> ceilingEntry = versionableSystemCFversions.get(recordTypeIdColumnName)
                .ceilingEntry(version);
        String recordTypeId = null;
        if (ceilingEntry != null) {
            recordTypeId = Bytes.toString(ceilingEntry.getValue());
        }
        Long recordTypeVersion = null;
        ceilingEntry = versionableSystemCFversions.get(recordTypeVersionColumnName).ceilingEntry(version);
        if (ceilingEntry != null) {
            recordTypeVersion = Bytes.toLong(ceilingEntry.getValue());
        }
        Pair<String, Long> recordType = new Pair<String, Long>(recordTypeId, recordTypeVersion);
        return recordType;
    }

    private List<Pair<QName, Object>> extractFields(Long version, Result result, ReadContext context)
            throws FieldTypeNotFoundException, RecordException, TypeException, InterruptedException {
        List<Pair<QName, Object>> fields = new ArrayList<Pair<QName, Object>>();
        NavigableMap<byte[], NavigableMap<Long, byte[]>> mapWithVersions = result.getMap().get(dataColumnFamily);
        if (mapWithVersions != null) {
            for (Entry<byte[], NavigableMap<Long, byte[]>> columnWithAllVersions : mapWithVersions.entrySet()) {
                NavigableMap<Long, byte[]> allValueVersions = columnWithAllVersions.getValue();
                Entry<Long, byte[]> ceilingEntry = allValueVersions.ceilingEntry(version);
                if (ceilingEntry != null) {
                    Pair<QName, Object> field = extractField(columnWithAllVersions.getKey(), ceilingEntry.getValue(),
                            context);
                    if (field != null) {
                        fields.add(field);
                    }
                }
            }
        }
        return fields;
    }

    private Pair<QName, Object> extractField(byte[] key, byte[] prefixedValue, ReadContext context)
            throws FieldTypeNotFoundException, RecordException, TypeException, InterruptedException {
        if (EncodingUtil.isDeletedField(prefixedValue)) {
            return null;
        }
        FieldType fieldType = typeManager.getFieldTypeById(key);
        context.addFieldType(fieldType);
        ValueType valueType = fieldType.getValueType();
        Object value = valueType.fromBytes(EncodingUtil.stripPrefix(prefixedValue));
        return new Pair<QName, Object>(fieldType.getName(), value);
    }

    private void addFieldsToGet(Get get, List<FieldType> fields) {
        boolean added = false;
        if (fields != null) {
            for (FieldType field : fields) {
                get.addColumn(dataColumnFamily, ((FieldTypeImpl)field).getIdBytes());
                added = true;
            }
        }
        if (added) {
            // Add system columns explicitly to get since we're not retrieving
            // all columns
            addSystemColumnsToGet(get);
        }
    }

    private void addSystemColumnsToGet(Get get) {
        get.addColumn(systemColumnFamily, RecordColumn.DELETED.bytes);
        get.addColumn(systemColumnFamily, RecordColumn.VERSION.bytes);
        get.addColumn(systemColumnFamily, RecordColumn.NON_VERSIONED_RT_ID.bytes);
        get.addColumn(systemColumnFamily, RecordColumn.NON_VERSIONED_RT_VERSION.bytes);
        get.addColumn(systemColumnFamily, RecordColumn.VERSIONED_RT_ID.bytes);
        get.addColumn(systemColumnFamily, RecordColumn.VERSIONED_RT_VERSION.bytes);
        get.addColumn(systemColumnFamily, RecordColumn.VERSIONED_MUTABLE_RT_ID.bytes);
        get.addColumn(systemColumnFamily, RecordColumn.VERSIONED_MUTABLE_RT_VERSION.bytes);
    }

    private boolean extractFieldsAndRecordTypes(Result result, Long version, Record record, ReadContext context)
            throws RecordTypeNotFoundException, RecordException, FieldTypeNotFoundException, TypeException,
            InterruptedException {
        List<Pair<QName, Object>> fields;
        if (version == null) {
            // All non-versioned fields are stored at version 1
            fields = extractFields(1L, result, context);
        } else {
            fields = extractFields(version, result, context);
        }
        if (fields.isEmpty()) 
            return false;
        
        for (Pair<QName, Object> field : fields) {
            record.setField(field.getV1(), field.getV2());
        }
        for (Scope readScope : context.getScopes()) {
            Pair<String, Long> recordTypePair = extractRecordType(readScope, result, version, record);
            RecordType recordType = typeManager.getRecordTypeById(recordTypePair.getV1(), recordTypePair.getV2());
            record.setRecordType(readScope, recordType.getName(), recordType.getVersion());
            context.setRecordTypeId(readScope, recordType);
        }
        return true;
    }

    public void delete(RecordId recordId) throws RecordException, RecordNotFoundException, RecordLockedException {
        ArgumentValidator.notNull(recordId, "recordId");
        long before = System.currentTimeMillis();
        org.lilyproject.repository.impl.lock.RowLock rowLock = null;
        byte[] rowId = recordId.toBytes();
        try {
            // Take Custom Lock
            rowLock = lockRow(recordId);

            if (recordExists(rowId)) { // Check if the record exists in the first place
                Put put = new Put(rowId);
                // Mark the record as deleted
                put.add(systemColumnFamily, RecordColumn.DELETED.bytes, 1L, Bytes.toBytes(true));
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
                        wal.processMessage(walMessage);
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
        get.addFamily(dataColumnFamily);
        get.addColumn(systemColumnFamily, RecordColumn.VERSION.bytes);
        get.setMaxVersions();
        Result result = recordTable.get(get);
        
        if (result != null && !result.isEmpty()) {
            boolean dataToDelete = false; 
            Delete delete = new Delete(recordId.toBytes());
            Set<BlobReference> blobsToDelete = new HashSet<BlobReference>();

            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result.getMap();
            Set<Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>> familiesSet = map.entrySet();
            for (Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> family : familiesSet) {
                if (Arrays.equals(dataColumnFamily,family.getKey())) {
                    NavigableMap<byte[], NavigableMap<Long, byte[]>> columnsSet = family.getValue();
                    for (Entry<byte[], NavigableMap<Long, byte[]>> column : columnsSet.entrySet()) {
                        try {
                            byte[] columnQualifier = column.getKey();
                            FieldType fieldType = typeManager.getFieldTypeById(columnQualifier);
                            ValueType valueType = fieldType.getValueType();
                            NavigableMap<Long,byte[]> cells = column.getValue();
                            Set<Entry<Long, byte[]>> cellsSet = cells.entrySet();
                            for (Entry<Long, byte[]> cell : cellsSet) {
                                // Get blobs to delete
                                if (valueType.getPrimitive() instanceof BlobValueType) {
                                    byte[] value = cell.getValue();
                                    if (!isDeleteMarker(value)) {
                                        Object valueObject = valueType.fromBytes(EncodingUtil.stripPrefix(value));
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
                                delete.deleteColumn(dataColumnFamily, columnQualifier, cell.getKey());
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
                recordTable.delete(delete);
            }
        }
    }
    
    private void unlockRow(org.lilyproject.repository.impl.lock.RowLock rowLock) {
        if (rowLock != null) {
            try {
                rowLocker.unlockRow(rowLock);
            } catch (IOException e) {
                log.warn("Exception while unlocking row <" + rowLock.getRowKey()+ ">", e);
            }
        }
    }

    private org.lilyproject.repository.impl.lock.RowLock lockRow(RecordId recordId) throws IOException,
            RecordLockedException {
        RowLock rowLock = rowLocker.lockRow(recordId.toBytes());
        if (rowLock == null)
            throw new RecordLockedException(recordId);
        return rowLock;
    }

    public void registerBlobStoreAccess(BlobStoreAccess blobStoreAccess) {
        blobManager.register(blobStoreAccess);
    }

    public OutputStream getOutputStream(Blob blob) throws BlobException {
        return blobManager.getOutputStream(blob);
    }
    
    public BlobInputStream getInputStream(RecordId recordId, Long version, QName fieldName, Integer multivalueIndex, Integer hierarchyIndex) throws BlobNotFoundException, BlobException, RecordNotFoundException, RecordTypeNotFoundException, FieldTypeNotFoundException, RecordException, VersionNotFoundException, TypeException, InterruptedException {
        Record record = read(recordId, version, Arrays.asList(new QName[]{fieldName}));
        FieldType fieldType = typeManager.getFieldTypeByName(fieldName);
        return blobManager.getInputStream(record, fieldName, multivalueIndex, hierarchyIndex, fieldType);
    }
    
    public BlobInputStream getInputStream(RecordId recordId, QName fieldName) throws BlobNotFoundException, BlobException, RecordNotFoundException, RecordTypeNotFoundException, FieldTypeNotFoundException, RecordException, VersionNotFoundException, TypeException, InterruptedException {
        return getInputStream(recordId, null, fieldName, null, null);
    }

    private Set<BlobReference> getReferencedBlobs(FieldTypeImpl fieldType, Object value) throws BlobException {
        fieldType.getValueType().getValues(value);
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
            byte[] fieldTypeIdBytes = fieldType.getIdBytes();
            byte[] recordIdBytes = recordId.toBytes();
            ValueType valueType = fieldType.getValueType();

            Get get = new Get(recordIdBytes);
            get.addColumn(dataColumnFamily, fieldTypeIdBytes);
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
            Filter filter = new ValueFilter(CompareOp.EQUAL, valueComparator);
            get.setFilter(filter);
            Result result = recordTable.get(get);
            
            if (result.isEmpty()) {
                unReferencedBlobs.add(blobReference);
            } else {
                if (ignoreVersion != null) {
                    boolean stillReferenced = false;
                    List<KeyValue> column = result.getColumn(dataColumnFamily, fieldType.getIdBytes());
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
        filterList.addFilter(new SingleColumnValueFilter(systemColumnFamily,
                RecordColumn.DELETED.bytes, CompareFilter.CompareOp.NOT_EQUAL, Bytes.toBytes(true)));

        Scan scan = new Scan(masterRecordIdBytes, filterList);
        scan.addColumn(systemColumnFamily, RecordColumn.DELETED.bytes);

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
    
    private boolean checkAndProcessOpenMessages(RecordId recordId) throws RecordException, InterruptedException {
        byte[] rowKey = recordId.toBytes();
        try {
            List<RowLogMessage> messages = wal.getMessages(rowKey);
            if (messages.isEmpty())
               return true;
            for (RowLogMessage rowLogMessage : messages) {
                wal.processMessage(rowLogMessage);
            }
            return (wal.getMessages(rowKey).isEmpty());
        } catch (RowLogException e) {
            throw new RecordException("Failed to check for open WAL message for record <" + recordId + ">", e);
        }
    }

    private static class ReadContext {
        private Map<String, FieldType> fieldTypes = new HashMap<String, FieldType>();
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

        public Map<String, FieldType> getFieldTypes() {
            return fieldTypes;
        }
        
        public Set<Scope> getScopes() {
            return scopes;
        }
    }
}
