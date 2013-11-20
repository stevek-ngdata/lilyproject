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
package org.lilyproject.repository.bulk;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.zookeeper.KeeperException;
import org.lilyproject.client.LilyClient;
import org.lilyproject.repository.api.Blob;
import org.lilyproject.repository.api.BlobAccess;
import org.lilyproject.repository.api.BlobException;
import org.lilyproject.repository.api.BlobManager;
import org.lilyproject.repository.api.BlobReference;
import org.lilyproject.repository.api.BlobStoreAccess;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.FieldTypes;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordFactory;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.RepositoryManager;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.impl.HBaseRepository;
import org.lilyproject.repository.impl.HBaseRepository.FieldValueWriter;
import org.lilyproject.repository.impl.HBaseRepositoryManager;
import org.lilyproject.repository.impl.HBaseTypeManager;
import org.lilyproject.repository.impl.RecordFactoryImpl;
import org.lilyproject.repository.impl.id.IdGeneratorImpl;
import org.lilyproject.repository.model.api.RepositoryModel;
import org.lilyproject.repository.model.impl.RepositoryModelImpl;
import org.lilyproject.util.exception.ExceptionUtil;
import org.lilyproject.util.hbase.HBaseTableFactory;
import org.lilyproject.util.hbase.HBaseTableFactoryImpl;
import org.lilyproject.util.hbase.LilyHBaseSchema;
import org.lilyproject.util.repo.RecordEvent;
import org.lilyproject.util.repo.RecordEvent.Type;
import org.lilyproject.util.zookeeper.ZkUtil;
import org.lilyproject.util.zookeeper.ZooKeeperItf;
import org.python.google.common.collect.Lists;

/**
 * Writes Lily records in bulk to HBase. Also provides methods for creating HBase {@code Put} or
 * {@code KeyValue} object for alternative methods of writing (e.g. via MapReduce).
 */
public class BulkIngester implements Closeable {

    public static final int PUT_BUFFER_SIZE = 1000;

    /**
     * Bulk mode is default. If not in bulk mode, the bulk ingester merely delegates all operations to lily client.
     */
    private boolean bulkMode = true;

    private LilyClient lilyClient;

    private HBaseRepository hbaseRepo;
    private RecordFactory recordFactory;
    private HTableInterface recordTable;
    private FieldTypes fieldTypes;
    private List<Put> putBuffer = Lists.newArrayListWithCapacity(PUT_BUFFER_SIZE);

    /**
     * Factory method for creation of a {@code BulkIngester} that operates on the default repository table.
     *
     * @param zkConnString Connection string for ZooKeeper
     * @param timeout      ZooKeeper session timeout
     * @return a new BulkIngester
     */
    public static BulkIngester newBulkIngester(String zkConnString, int timeout) {
        return newBulkIngester(zkConnString, timeout, null, null, true);
    }

    /**
     * Factory method for creation of a {@code BulkIngester} that operates on a non-default repository table.
     *
     * @param zkConnString connection string for ZooKeeper
     * @param timeout      ZooKeeper session timeout
     * @param tableName    name of the repository table to write to
     */
    public static BulkIngester newBulkIngester(String zkConnString, int timeout, String repositoryName, String tableName,
                                               boolean bulkMode) {
        try {
            ZooKeeperItf zk = ZkUtil.connect(zkConnString, timeout);

            // we need a lily client for non bulk access
            LilyClient lilyClient = new LilyClient(zk);

            // we need an HBaseRepository for bulk access
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", zkConnString);
            HBaseTableFactory hbaseTableFactory = new HBaseTableFactoryImpl(conf);
            HBaseRepository hbaseRepository = createHBaseRepository(repositoryName, tableName, zk, conf, hbaseTableFactory);

            return new BulkIngester(
                    lilyClient,
                    hbaseRepository,
                    LilyHBaseSchema.getRecordTable(hbaseTableFactory, hbaseRepository.getRepositoryName(),
                            hbaseRepository.getTableName()),
                    bulkMode);

        } catch (Exception e) {
            ExceptionUtil.handleInterrupt(e);
            throw new RuntimeException(e);
        }
    }

    private static HBaseRepository createHBaseRepository(String repositoryName, String tableName, ZooKeeperItf zk,
                                                         Configuration conf, HBaseTableFactory hbaseTableFactory)
            throws KeeperException, InterruptedException, IOException, RepositoryException {
        RepositoryModel repositoryModel = new RepositoryModelImpl(zk);
        IdGenerator idGenerator = new IdGeneratorImpl();
        TypeManager typeManager = new HBaseTypeManager(idGenerator, conf, zk, hbaseTableFactory);
        RecordFactory recordFactory = new RecordFactoryImpl();

        RepositoryManager repositoryManager = new HBaseRepositoryManager(typeManager, idGenerator,
                recordFactory, hbaseTableFactory, new BlobsNotSupportedBlobManager(), conf, repositoryModel);
        HBaseRepository hbaseRepository;
        if (tableName != null) {
            hbaseRepository = (HBaseRepository) repositoryManager.getRepository(repositoryName).getTable(tableName);
        } else {
            hbaseRepository = (HBaseRepository) repositoryManager.getRepository(repositoryName);
        }
        return hbaseRepository;
    }

    BulkIngester(LilyClient lilyClient, HBaseRepository hbaseRepo, HTableInterface recordTable, boolean bulkMode)
            throws InterruptedException {
        this.lilyClient = lilyClient;
        this.hbaseRepo = hbaseRepo;
        this.recordFactory = hbaseRepo.getRecordFactory();
        this.recordTable = recordTable;
        this.fieldTypes = hbaseRepo.getTypeManager().getFieldTypesSnapshot();
        this.bulkMode = bulkMode;
    }

    /**
     * Factory method for creation of Records, with the same semantics as
     * {@link Repository#newRecord()}.
     *
     * @return A newly-created record
     */
    public Record newRecord() {
        return recordFactory.newRecord();
    }

    /**
     * Same as {@link Repository#getIdGenerator()}.
     *
     * @return The IdGenerator of the underlying repository
     */
    public IdGenerator getIdGenerator() {
        return hbaseRepo.getIdGenerator();
    }

    /**
     * When in bulk mode, write a single record directly to Lily, circumventing any indexing or other secondary actions
     * that are performed when using the standard Lily API.
     * <p>
     * <b>WARNING:</b>This method is not thread-safe.
     * <p>
     * Puts are first written to a buffer, which is flushed when it reaches {@link BulkIngester#PUT_BUFFER_SIZE}.
     *
     * When not in bulk mode, this merely delegates to createOrUpdate on the lily hbase repository.
     *
     * @param record Record to be written
     */
    public void write(Record record) throws InterruptedException, RepositoryException, IOException {
        if (bulkMode) {
            putBuffer.add(buildPut(record));
            if (putBuffer.size() == PUT_BUFFER_SIZE) {
                flush();
            }
        } else {
            lilyClient.getRepository(hbaseRepo.getRepositoryName()).getTable(hbaseRepo.getTableName()).createOrUpdate(record);
        }
    }

    /**
     * Flush buffered Puts to the Lily record table.
     * <p>
     * This method is not thread-safe.
     */
    public void flush() throws IOException {
        if (!putBuffer.isEmpty()) {
            recordTable.put(Lists.newArrayList(putBuffer));
            putBuffer.clear();
        }
    }

    /**
     * Build the {@code Put} that represents a record for inserting into HBase.
     *
     * @param record The record to be translated into an HBase {@code Put}
     * @return Put which can be directly written to HBase
     */
    public Put buildPut(Record record) throws InterruptedException, RepositoryException {
        RecordEvent recordEvent = new RecordEvent();
        recordEvent.setType(Type.CREATE);
        if (record.getId() == null) {
            record.setId(getIdGenerator().newRecordId());
        }
        return hbaseRepo.buildPut(record, 1L, fieldTypes, recordEvent, Sets.<BlobReference>newHashSet(),
                Sets.<BlobReference>newHashSet(), 1L);
    }

    /**
     * Build a {@code Put} to update a record. No metadata updates are performed, and any existing metadata on the
     * fields will be overwritten.
     * <p>
     * The record to be updated must exist, otherwise a "partial" record will be created. No checking is done to ensure
     * that the record to be updated exists.
     * <p>
     * Additionally, records updated in this manner must be unversioned records.
     * <p>
     * In other words, use this method at your own risk. Unless you are very certain about the context you are working
     * in, updates should go via the Lily API.
     *
     * @param recordId    identifier of the record to be updated
     * @param fieldValues map of field names and values to be updated on the record
     * @return Put containing all field updates
     */
    public Put buildRecordUpdate(RecordId recordId, Map<QName, Object> fieldValues) {
        Put put = new Put(recordId.toBytes());
        FieldValueWriter fieldValueWriter = hbaseRepo.newFieldValueWriter(put, null);

        for (Entry<QName, Object> fieldEntry : fieldValues.entrySet()) {
            try {
                fieldValueWriter.addFieldValue(fieldTypes.getFieldType(fieldEntry.getKey()), fieldEntry.getValue(), null);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return put;
    }

    @Override
    public void close() throws IOException {
        flush();
        lilyClient.close();
    }


    /**
     * Returns the underlying repository manager. This allows performing any other kind of
     * repository-based task within the context of a bulk import job.
     *
     * @return the underlying RepositoryManager
     */
    public RepositoryManager getRepositoryManager() {
        return lilyClient;
    }


    /**
     * BlobManager that ensures that blobs aren't supported in bulk import.
     * <p>
     * In the future, it may be interesting to support blobs, but at the moment it seems as this
     * would likely lead to poor import performance.
     */
    private static class BlobsNotSupportedBlobManager implements BlobManager {

        private static final String NOT_SUPPORTED_MESSAGE = "Blobs are not supported for bulk imports";

        @Override
        public void incubateBlob(byte[] blobKey) throws IOException {
            throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
        }

        @Override
        public Set<BlobReference> reserveBlobs(Set<BlobReference> blobs) throws IOException {
            throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
        }

        @Override
        public void handleBlobReferences(RecordId recordId, Set<BlobReference> referencedBlobs,
                                         Set<BlobReference> unReferencedBlobs) {
            // no op
        }

        @Override
        public OutputStream getOutputStream(Blob blob) throws BlobException {
            throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
        }

        @Override
        public BlobAccess getBlobAccess(Record record, QName fieldName, FieldType fieldType, int... indexes)
                throws BlobException {
            throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
        }

        @Override
        public void register(BlobStoreAccess blobStoreAccess) {
            throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
        }

        @Override
        public void delete(byte[] blobKey) throws BlobException {
            throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
        }


    }

}
