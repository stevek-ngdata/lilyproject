package org.lilyproject.repotestfw;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.lilyproject.repository.api.*;
import org.lilyproject.repository.impl.IdRecordImpl;
import org.lilyproject.repository.impl.RecordBuilderImpl;
import org.lilyproject.repository.impl.RecordImpl;

/**
 * Fake Repository implementation that keeps a few records in a hashmap. No support for blobs, scanners, versions or
 * scopes at the moment. We also don't do anything with mutation conditions.
 *
 * This is not thread safe either so thread carefully
 *
 */
public class FakeLTable implements Repository {
    private Map<RecordId, Record> records = new HashMap<RecordId, Record>();
    private LRepository repository;
    private String repositoryName;
    private String tableName;

    public FakeLTable(LRepository repository, String repositoryName, String tableName) {
        this.repository = repository;
        this.repositoryName = repositoryName;
        this.tableName = tableName;
    }

    @Override
    public Record newRecord() throws RecordException {
        return new RecordImpl();
    }

    @Override
    public Record newRecord(RecordId recordId) throws RecordException {
        return new RecordImpl(recordId);
    }

    @Override
    public Record create(Record record) throws RepositoryException, InterruptedException {
        if (record.getId() == null) {
            record.setId(repository.getIdGenerator().newRecordId());
        }
        if (records.containsKey(record.getId())) {
            throw new RecordExistsException(record.getId());
        }
        record.setVersion(0l);
        record = writeRecord(record);
        return record;
    }

    @Override
    public Record update(Record record, boolean updateVersion, boolean useLastRecordType) throws RepositoryException, InterruptedException {
        if (updateVersion) {
            Long version = record.getVersion();
            if (version == null) {
                version = 0L;
            }

        }

        record = writeRecord(record);
        return record;

    }
    private Record merge(Record r1, Record r2) {
        Record result = r2.clone();
        if (r1.getRecordTypeName() != null) {
            result.setRecordType(r1.getRecordTypeName());
        }
        // TODO merge meta map
        for(Map.Entry<QName,Object> entry : r1.getFields().entrySet()) {
            result.setField(entry.getKey(), entry.getValue());
        }
        return result;
    }

    private Record writeRecord(Record record) throws RepositoryException, InterruptedException{
        record = record.cloneRecord();
        Record originalRecord = record;
        ResponseStatus status = ResponseStatus.UP_TO_DATE;
        if (records.containsKey(record.getId())) {
            originalRecord = records.get(record.getId());
            record = merge(record, originalRecord);

            if (!originalRecord.equals(record)) {
                status = ResponseStatus.UPDATED;
            }
        } else {
            status = ResponseStatus.CREATED;
        }
        QName recordTypeName = record.getRecordTypeName();
        Long recordTypeVersion = getTypeManager().getRecordTypeByName(recordTypeName, null).getVersion();
        record.setRecordType(recordTypeName, recordTypeVersion);

        validateRecord(record, originalRecord, getTypeManager().getRecordTypeByName(recordTypeName, null));
        Long version = record.getVersion() == null ? 0l : record.getVersion();
        record.setVersion(version + 1);
        records.put(record.getId(), record);
        record.setResponseStatus(status);
        return record;
    }


    /* Grabbed from {@link org.lilyproject.repository.impl.HBaseRepository} */
    private void validateRecord(Record record, Record originalRecord, RecordType recordType)
            throws TypeException, InvalidRecordException, InterruptedException, RepositoryException {
        // Check mandatory fields
        Collection<FieldTypeEntry> fieldTypeEntries = recordType.getFieldTypeEntries();
        List<QName> fieldsToDelete = record.getFieldsToDelete();
        for (FieldTypeEntry fieldTypeEntry : fieldTypeEntries) {
            if (fieldTypeEntry.isMandatory()) {
                FieldType fieldType = getTypeManager().getFieldTypeById(fieldTypeEntry.getFieldTypeId());
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

    @Override
    public Record update(Record record) throws RepositoryException, InterruptedException {
        return update(record, true, false);
    }

    @Override
    public Record update(Record record, List<MutationCondition> mutationConditions) throws RepositoryException, InterruptedException {
        return update(record, true, false);
    }

    @Override
    public Record update(Record record, boolean updateVersion, boolean updateLastRecordType, List<MutationCondition> mutationConditions) throws RepositoryException, InterruptedException {
        return update(record, updateVersion, updateLastRecordType);
    }

    @Override
    public Record createOrUpdate(Record record) throws RepositoryException, InterruptedException {
        return update(record);
    }

    @Override
    public Record createOrUpdate(Record record, boolean b) throws RepositoryException, InterruptedException {
        return update(record);
    }

    private Record getRecord(RecordId recordId) throws RecordNotFoundException {
        Record record = records.get(recordId);
        if (record == null) {
            throw new RecordNotFoundException(recordId, this, repository);
        }
        return record.clone();
    }

    @Override
    public Record read(RecordId recordId, List<QName> qNames) throws RepositoryException, InterruptedException {
        return getRecord(recordId);
    }

    @Override
    public Record read(RecordId recordId, QName... qNames) throws RepositoryException, InterruptedException {
        return getRecord(recordId);
    }

    @Override
    public List<Record> read(List<RecordId> recordIds, List<QName> qNames) throws RepositoryException, InterruptedException {
        return read(recordIds);
    }

    @Override
    public List<Record> read(List<RecordId> recordIds, QName... qNames) throws RepositoryException, InterruptedException {
        List<Record> list = Lists.newArrayList();
        for (RecordId id : recordIds) {
            list.add(getRecord(id));
        }
        return list;
    }

    @Override
    public Record read(RecordId recordId, Long aLong, List<QName> qNames) throws RepositoryException, InterruptedException {
        return getRecord(recordId);
    }

    @Override
    public Record read(RecordId recordId, Long aLong, QName... qNames) throws RepositoryException, InterruptedException {
        return getRecord(recordId);
    }

    @Override
    public List<Record> readVersions(RecordId recordId, Long aLong, Long aLong2, List<QName> qNames) throws RepositoryException, InterruptedException {
        return Lists.newArrayList(read(recordId));    }

    @Override
    public List<Record> readVersions(RecordId recordId, Long aLong, Long aLong2, QName... qNames) throws RepositoryException, InterruptedException {
        return Lists.newArrayList(read(recordId));    }

    @Override
    public List<Record> readVersions(RecordId recordId, List<Long> longs, List<QName> qNames) throws RepositoryException, InterruptedException {
        return Lists.newArrayList(read(recordId));    }

    @Override
    public List<Record> readVersions(RecordId recordId, List<Long> longs, QName... qNames) throws RepositoryException, InterruptedException {
        return Lists.newArrayList(read(recordId));
    }

    @Override
    public IdRecord readWithIds(RecordId recordId, Long aLong, List<SchemaId> schemaIds) throws RepositoryException, InterruptedException {
        Record record = getRecord(recordId);
        TypeManager typeManager = this.getTypeManager();

        Map<SchemaId, QName> map = Maps.newHashMap();
        for (QName qname : record.getFields().keySet()) {
            map.put(typeManager.getFieldTypeByName(qname).getId(), qname);            
        }

        Map<Scope,SchemaId> recordTypeIds = Maps.newHashMap();
        for (Scope scope : Scope.values()) {
            RecordType recordType = typeManager.getRecordTypeByName(record.getRecordTypeName(scope), record.getVersion());
            if (recordType != null) {
                recordTypeIds.put(scope, recordType.getId());
            }
        }
        IdRecord idRecord = new IdRecordImpl(record, map, recordTypeIds);
        return idRecord;
    }

    @Override
    public void delete(RecordId recordId) throws RepositoryException, InterruptedException {
        records.remove(recordId);
    }

    @Override
    public Record delete(RecordId recordId, List<MutationCondition> mutationConditions) throws RepositoryException, InterruptedException {
        return records.remove(recordId);
    }

    @Override
    public void delete(Record record) throws RepositoryException, InterruptedException {
        records.remove(record.getId());
    }

    @Override
    public OutputStream getOutputStream(Blob blob) throws RepositoryException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlobAccess getBlob(RecordId recordId, Long aLong, QName qName, int... ints) throws RepositoryException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlobAccess getBlob(RecordId recordId, Long aLong, QName qName, Integer integer, Integer integer2) throws RepositoryException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlobAccess getBlob(RecordId recordId, QName qName) throws RepositoryException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public InputStream getInputStream(RecordId recordId, Long aLong, QName qName, int... ints) throws RepositoryException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public InputStream getInputStream(RecordId recordId, Long aLong, QName qName, Integer integer, Integer integer2) throws RepositoryException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public InputStream getInputStream(RecordId recordId, QName qName) throws RepositoryException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public InputStream getInputStream(Record record, QName qName, int... ints) throws RepositoryException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public InputStream getInputStream(Record record, QName qName, Integer integer, Integer integer2) throws RepositoryException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<RecordId> getVariants(RecordId recordId) throws RepositoryException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public RecordScanner getScanner(RecordScan recordScan) throws RepositoryException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public IdRecordScanner getScannerWithIds(RecordScan recordScan) throws RepositoryException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public RecordBuilder recordBuilder() throws RecordException, InterruptedException {
        return new RecordBuilderImpl(this, repository.getIdGenerator());
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public LTable getTable(String s) throws InterruptedException, RepositoryException {
        throw new UnsupportedOperationException();
    }

    @Override
    public LTable getDefaultTable() throws InterruptedException, RepositoryException {
        return repository.getDefaultTable();
    }

    @Override
    public TableManager getTableManager() {
        return repository.getTableManager();
    }

    @Override
    public IdGenerator getIdGenerator() {
        return repository.getIdGenerator();
    }

    @Override
    public TypeManager getTypeManager() {
        return repository.getTypeManager();
    }

    @Override
    public RecordFactory getRecordFactory() {
        return repository.getRecordFactory();
    }

    @Override
    public String getRepositoryName() {
        return repository.getRepositoryName();
    }

    @Override
    public void close() throws IOException {
        // nop
    }
}
