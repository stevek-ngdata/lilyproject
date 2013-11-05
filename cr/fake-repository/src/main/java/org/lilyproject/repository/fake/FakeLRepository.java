/*
 * Copyright 2013 NGDATA nv
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
package org.lilyproject.repository.fake;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.lilyproject.repository.api.Blob;
import org.lilyproject.repository.api.BlobAccess;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.IdRecord;
import org.lilyproject.repository.api.IdRecordScanner;
import org.lilyproject.repository.api.LRepository;
import org.lilyproject.repository.api.LTable;
import org.lilyproject.repository.api.MutationCondition;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordBuilder;
import org.lilyproject.repository.api.RecordException;
import org.lilyproject.repository.api.RecordFactory;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.RecordScan;
import org.lilyproject.repository.api.RecordScanner;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.RepositoryManager;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.TableManager;
import org.lilyproject.repository.api.TypeManager;

/**
 * Fake Repository implementation that keeps a few records in a hashmap. No support for blobs, scanners, versions or
 * scopes
 * at the moment. We also don't do anything with mutation conditions
 */
public class FakeLRepository implements LRepository, Repository {
    private LoadingCache<String, LTable> tables;
    private String repositoryName;
    private TableManager tableManager;
    private IdGenerator idGenerator;
    private TypeManager typeManager;
    private RecordFactory recordFactory;

    public FakeLRepository(final RepositoryManager repositoryManager, final String repositoryName, TableManager tableManager, IdGenerator idGenerator, TypeManager typeManager, RecordFactory recordFactory) {
        this.repositoryName = repositoryName;
        this.tableManager = tableManager;
        this.idGenerator = idGenerator;
        this.typeManager = typeManager;
        this.recordFactory = recordFactory;
        tables = CacheBuilder.newBuilder().build(new CacheLoader<String, LTable>() {

            @Override
            public LTable load(String tableName) throws Exception {
                return new FakeLTable(FakeLRepository.this, repositoryName, tableName);
            }
        });
    }

    @Override
    public LTable getTable(String tableName) throws InterruptedException, RepositoryException {
        try {
            return tables.get(tableName);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public LTable getDefaultTable() throws InterruptedException, RepositoryException {
        try {
            return tables.get("record");
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public TableManager getTableManager() {
        return tableManager;
    }

    @Override
    public IdGenerator getIdGenerator() {
        return idGenerator;
    }

    @Override
    public TypeManager getTypeManager() {
        return typeManager;
    }

    @Override
    public RecordFactory getRecordFactory() {
        return recordFactory;
    }

    @Override
    public Record newRecord() throws RecordException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Record newRecord(RecordId recordId) throws RecordException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Record create(Record record) throws RepositoryException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Record update(Record record, boolean b, boolean b2) throws RepositoryException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Record update(Record record) throws RepositoryException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Record update(Record record, List<MutationCondition> mutationConditions) throws RepositoryException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Record update(Record record, boolean b, boolean b2, List<MutationCondition> mutationConditions) throws RepositoryException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Record createOrUpdate(Record record) throws RepositoryException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Record createOrUpdate(Record record, boolean b) throws RepositoryException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Record read(RecordId recordId, List<QName> qNames) throws RepositoryException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Record read(RecordId recordId, QName... qNames) throws RepositoryException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Record> read(List<RecordId> recordIds, List<QName> qNames) throws RepositoryException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Record> read(List<RecordId> recordIds, QName... qNames) throws RepositoryException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Record read(RecordId recordId, Long aLong, List<QName> qNames) throws RepositoryException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Record read(RecordId recordId, Long aLong, QName... qNames) throws RepositoryException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Record> readVersions(RecordId recordId, Long aLong, Long aLong2, List<QName> qNames) throws RepositoryException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Record> readVersions(RecordId recordId, Long aLong, Long aLong2, QName... qNames) throws RepositoryException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Record> readVersions(RecordId recordId, List<Long> longs, List<QName> qNames) throws RepositoryException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Record> readVersions(RecordId recordId, List<Long> longs, QName... qNames) throws RepositoryException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public IdRecord readWithIds(RecordId recordId, Long aLong, List<SchemaId> schemaIds) throws RepositoryException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void delete(RecordId recordId) throws RepositoryException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Record delete(RecordId recordId, List<MutationCondition> mutationConditions) throws RepositoryException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void delete(Record record) throws RepositoryException, InterruptedException {
        throw new UnsupportedOperationException();
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
        throw new UnsupportedOperationException();
    }

    @Override
    public String getTableName() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getRepositoryName() {
        return repositoryName;
    }

    @Override
    public void close() throws IOException {
        // no closing needed
    }
}
