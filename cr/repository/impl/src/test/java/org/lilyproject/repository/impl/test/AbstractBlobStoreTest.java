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
package org.lilyproject.repository.impl.test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.lilyproject.repository.api.*;
import org.lilyproject.repository.impl.*;
import org.lilyproject.rowlog.api.RowLog;
import org.lilyproject.rowlog.api.RowLogConfig;
import org.lilyproject.rowlog.api.RowLogConfigurationManager;
import org.lilyproject.rowlog.api.RowLogShard;
import org.lilyproject.rowlog.impl.RowLogConfigurationManagerImpl;
import org.lilyproject.rowlog.impl.RowLogImpl;
import org.lilyproject.rowlog.impl.RowLogShardImpl;
import org.lilyproject.testfw.HBaseProxy;
import org.lilyproject.util.hbase.HBaseTableFactory;
import org.lilyproject.util.hbase.LilyHBaseSchema;
import org.lilyproject.util.hbase.LilyHBaseSchema.RecordCf;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

public abstract class AbstractBlobStoreTest {
    protected final static HBaseProxy HBASE_PROXY = new HBaseProxy();
    protected static RowLog wal;
    protected static Repository repository;
    protected static TypeManager typeManager;
    protected static Configuration configuration;
    protected static ZooKeeperItf zooKeeper;
    protected static RowLogConfigurationManager rowLogConfMgr;
    protected static HBaseTableFactory hbaseTableFactory;
    protected static SizeBasedBlobStoreAccessFactory blobStoreAccessFactory;
    protected static BlobStoreAccess dfsBlobStoreAccess;
    protected static BlobStoreAccess hbaseBlobStoreAccess;
    protected static BlobStoreAccess inlineBlobStoreAccess;
    protected static Random random = new Random();
    protected static BlobStoreAccessRegistry testBlobStoreAccessRegistry;
    protected static BlobManager blobManager;

    protected static void setupWal() throws Exception {
        rowLogConfMgr = new RowLogConfigurationManagerImpl(zooKeeper);
        rowLogConfMgr.addRowLog("WAL", new RowLogConfig(10000L, true, false, 100L, 5000L));
        wal = new RowLogImpl("WAL", LilyHBaseSchema.getRecordTable(hbaseTableFactory), RecordCf.WAL_PAYLOAD.bytes,
                RecordCf.WAL_STATE.bytes, rowLogConfMgr);
        RowLogShard walShard = new RowLogShardImpl("WS1", configuration, wal, 100);
        wal.registerShard(walShard);
    }

    protected static BlobManager setupBlobManager() throws IOException, URISyntaxException {
        dfsBlobStoreAccess = new DFSBlobStoreAccess(HBASE_PROXY.getBlobFS(), new Path("/lily/blobs"));
        hbaseBlobStoreAccess = new HBaseBlobStoreAccess(configuration);
        inlineBlobStoreAccess = new InlineBlobStoreAccess();
        blobStoreAccessFactory = new SizeBasedBlobStoreAccessFactory(dfsBlobStoreAccess);
        blobStoreAccessFactory.addBlobStoreAccess(50, inlineBlobStoreAccess);
        blobStoreAccessFactory.addBlobStoreAccess(1024, hbaseBlobStoreAccess);
        return new BlobManagerImpl(hbaseTableFactory, blobStoreAccessFactory, false);
    }
    
    @Test
    public void testCreate() throws Exception {
        QName fieldName = new QName("test", "testCreate");
        FieldType fieldType = typeManager.newFieldType(typeManager.getValueType("BLOB", false, false), fieldName,
                Scope.NON_VERSIONED);
        fieldType = typeManager.createFieldType(fieldType);
        RecordType recordType = typeManager.newRecordType(new QName(null, "testCreateRT"));
        FieldTypeEntry fieldTypeEntry = typeManager.newFieldTypeEntry(fieldType.getId(), true);
        recordType.addFieldTypeEntry(fieldTypeEntry);
        recordType = typeManager.createRecordType(recordType);
        
        byte[] bytes = Bytes.toBytes("someBytes");
        Blob blob = writeBlob(bytes, "aMediaType", "testCreate");
        Record record = repository.newRecord();
        record.setRecordType(recordType.getName());
        record.setField(fieldName, blob);
        record = repository.create(record);
        
        byte[] readBytes = readBlob(record.getId(), fieldName, blob.getSize());
        assertTrue(Arrays.equals(bytes, readBytes));
    }

    @Test
    public void testThreeSizes() throws Exception {
        QName fieldName1 = new QName("test", "testThreeSizes1");
        QName fieldName2 = new QName("test", "testThreeSizes2");
        QName fieldName3 = new QName("test", "testThreeSizes3");
        FieldType fieldType1 = typeManager.newFieldType(typeManager.getValueType("BLOB", false, false), fieldName1,
                Scope.NON_VERSIONED);
        fieldType1 = typeManager.createFieldType(fieldType1);
        FieldType fieldType2 = typeManager.newFieldType(typeManager.getValueType("BLOB", false, false), fieldName2,
                Scope.NON_VERSIONED);
        fieldType2 = typeManager.createFieldType(fieldType2);
        FieldType fieldType3 = typeManager.newFieldType(typeManager.getValueType("BLOB", false, false), fieldName3,
                Scope.NON_VERSIONED);
        fieldType3 = typeManager.createFieldType(fieldType3);
        RecordType recordType = typeManager.newRecordType(new QName(null, "testThreeSizes"));
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType1.getId(), true));
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType2.getId(), true));
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType3.getId(), true));
        recordType = typeManager.createRecordType(recordType);
        
        byte[] small = new byte[10];
        random.nextBytes(small);
        byte[] medium = new byte[100];
        random.nextBytes(medium);
        byte[] large = new byte[2048];
        random.nextBytes(large);
        
        Blob smallBlob = writeBlob(small, "mime/small", "small");
        Blob mediumBlob = writeBlob(medium, "mime/medium", "medium");
        Blob largeBlob = writeBlob(large, "mime/large", "large");
        
        Record record = repository.newRecord();
        record.setRecordType(recordType.getName());
        record.setField(fieldName1, smallBlob);
        record.setField(fieldName2, mediumBlob);
        record.setField(fieldName3, largeBlob);
        record = repository.create(record);
        
        byte[] readBytes = readBlob(record.getId(), fieldName1, smallBlob.getSize());
        assertTrue(Arrays.equals(small, readBytes));
        readBytes = readBlob(record.getId(), fieldName2, mediumBlob.getSize());
        assertTrue(Arrays.equals(medium, readBytes));
        readBytes = readBlob(record.getId(), fieldName3, largeBlob.getSize());
        assertTrue(Arrays.equals(large, readBytes));
    }

    @Test
    public void testCreateTwoRecordsWithSameBlob() throws Exception {
        QName fieldName = new QName("test", "ablob2");
        FieldType fieldType = typeManager.newFieldType(typeManager.getValueType("BLOB", false, false), fieldName,
                Scope.VERSIONED);
        fieldType = typeManager.createFieldType(fieldType);
        RecordType recordType = typeManager.newRecordType(new QName(null, "testCreateTwoRecordsWithSameBlobRT"));
        FieldTypeEntry fieldTypeEntry = typeManager.newFieldTypeEntry(fieldType.getId(), true);
        recordType.addFieldTypeEntry(fieldTypeEntry);
        recordType = typeManager.createRecordType(recordType);

        byte[] bytes = Bytes.toBytes("someBytes");
        Blob blob = writeBlob(bytes, "aMediaType", "testCreate");

        Record record = repository.newRecord();
        record.setRecordType(recordType.getName(), null);
        record.setField(fieldName, blob);
        record = repository.create(record);

        Record record2 = repository.newRecord();
        record2.setRecordType(recordType.getName(), null);
        record2.setField(fieldName, blob);
        record2 = repository.create(record2); // For an inline record this succeeds

        byte[] bytesLarge = new byte[3000]; 
        random.nextBytes(bytesLarge);
        Blob largeBlob = writeBlob(bytesLarge, "largeBlob", "testCreate");
        
        Record record3 = repository.newRecord();
        record3.setRecordType(recordType.getName(), null);
        record3.setField(fieldName, largeBlob);
        record3 = repository.create(record3);

        Record record4 = repository.newRecord();
        record4.setRecordType(recordType.getName(), null);
        record4.setField(fieldName, largeBlob);
        
        try {
            record4 = repository.create(record4);
            fail("Using the same blob in two records should not succeed");
        } catch (InvalidRecordException expected) {
        }
    }

    @Test
    public void testUpdateNonVersionedBlobHDFS() throws Exception {
        testUpdateNonVersionedBlob(3000, true);
    }
    
    @Test
    public void testUpdateNonVersionedBlobHBase() throws Exception {
        testUpdateNonVersionedBlob(150, true);
    }
    
    @Test
    public void testUpdateNonVersionedBlobInline() throws Exception {
        testUpdateNonVersionedBlob(50, false);
    }
    
    private void testUpdateNonVersionedBlob(int size, boolean expectDelete) throws Exception {
        QName fieldName = new QName("test", "testUpdateNonVersionedBlob"+size);
        FieldType fieldType = typeManager.newFieldType(typeManager.getValueType("BLOB", false, false), fieldName,
                Scope.NON_VERSIONED);
        fieldType = typeManager.createFieldType(fieldType);
        RecordType recordType = typeManager.newRecordType(new QName(null, "testUpdateNonVersionedBlobRT"+size));
        FieldTypeEntry fieldTypeEntry = typeManager.newFieldTypeEntry(fieldType.getId(), true);
        recordType.addFieldTypeEntry(fieldTypeEntry);
        recordType = typeManager.createRecordType(recordType);

        byte[] bytes = new byte[size]; 
        random.nextBytes(bytes);
        Blob blob = writeBlob(bytes, "aMediaType", "testUpdateNonVersionedBlob");

        byte[] bytes2 = new byte[size]; 
        random.nextBytes(bytes2);
        Blob blob2 = writeBlob(bytes2, "aMediaType", "testUpdateNonVersionedBlob2");
        
        Record record = repository.newRecord();
        record.setRecordType(recordType.getName(), null);
        record.setField(fieldName, blob);
        record = repository.create(record);

        Record record2 = repository.newRecord(record.getId());
        record2.setRecordType(recordType.getName(), null);
        record2.setField(fieldName, blob2);
        
        record = repository.update(record2);
        
        // Reading should return blob2
        byte[] readBytes = readBlob(record.getId(), record.getVersion(), fieldName, null, null, size);
        assertTrue(Arrays.equals(bytes2, readBytes));
        
        assertBlobDelete(expectDelete, blob);
    }
    
    @Test
    public void testDeleteNonVersionedBlobHDFS() throws Exception {
        testDeleteNonVersionedBlob(3000, true);
    }
    
    @Test
    public void testDeleteNonVersionedBlobHBase() throws Exception {
        testDeleteNonVersionedBlob(150, true);
    }
    
    @Test
    public void testDeleteNonVersionedBlobInline() throws Exception {
        testDeleteNonVersionedBlob(50, false);
    }
    
    private void testDeleteNonVersionedBlob(int size, boolean expectDelete) throws Exception {
        QName fieldName = new QName("test", "testDeleteNonVersionedBlob"+size);
        FieldType fieldType = typeManager.newFieldType(typeManager.getValueType("BLOB", false, false), fieldName,
                Scope.NON_VERSIONED);
        fieldType = typeManager.createFieldType(fieldType);
        RecordType recordType = typeManager.newRecordType(new QName(null, "testDeleteNonVersionedBlobRT"+size));
        FieldTypeEntry fieldTypeEntry = typeManager.newFieldTypeEntry(fieldType.getId(), false);
        recordType.addFieldTypeEntry(fieldTypeEntry);
        recordType = typeManager.createRecordType(recordType);

        byte[] bytes = new byte[size]; 
        random.nextBytes(bytes);
        Blob blob = writeBlob(bytes, "aMediaType", "testDeleteNonVersionedBlob");

        Record record = repository.newRecord();
        record.setRecordType(recordType.getName(), null);
        record.setField(fieldName, blob);
        record = repository.create(record);

        Record record2 = repository.newRecord(record.getId());
        record2.setRecordType(recordType.getName(), null);
        record2.addFieldsToDelete(Arrays.asList(new QName[]{fieldName}));
        
        record = repository.update(record2);

        assertBlobDelete(expectDelete, blob);
    }
    
    @Test
    public void testUpdateMutableBlobHDFS() throws Exception {
        testUpdateMutableBlob(3000, true);
    }

    @Test
    public void testUpdateMutableBlobHBase() throws Exception {
        testUpdateMutableBlob(150, true);
    }

    @Test
    public void testUpdateMutableBlobInline() throws Exception {
        testUpdateMutableBlob(50, false);
    }

    private void testUpdateMutableBlob(int size, boolean expectDelete) throws Exception {
        QName fieldName = new QName("test", "testUpdateMutableBlob"+size);
        FieldType fieldType = typeManager.newFieldType(typeManager.getValueType("BLOB", false, false), fieldName,
                Scope.VERSIONED_MUTABLE);
        fieldType = typeManager.createFieldType(fieldType);
        RecordType recordType = typeManager.newRecordType(new QName(null, "testUpdateMutableBlobRT"+size));
        FieldTypeEntry fieldTypeEntry = typeManager.newFieldTypeEntry(fieldType.getId(), true);
        recordType.addFieldTypeEntry(fieldTypeEntry);
        recordType = typeManager.createRecordType(recordType);

        byte[] bytes = new byte[size]; 
        random.nextBytes(bytes);
        Blob blob = writeBlob(bytes, "aMediaType", "testUpdateMutableBlob");

        byte[] bytes2 = new byte[size]; 
        random.nextBytes(bytes2);
        Blob blob2 = writeBlob(bytes2, "aMediaType", "testUpdateMutableBlob2");
        
        Record record = repository.newRecord();
        record.setRecordType(recordType.getName(), null);
        record.setField(fieldName, blob);
        record = repository.create(record);

        Record record2 = repository.newRecord(record.getId());
        record2.setRecordType(recordType.getName(), null);
        record2.setField(fieldName, blob2);
        record2.setVersion(record.getVersion());
        
        record = repository.update(record2, true, false);
        
        // Blob2 should still exist
        byte[] readBytes = readBlob(record.getId(), record.getVersion(), fieldName, null, null, size);
        assertTrue(Arrays.equals(bytes2, readBytes));
        
        assertBlobDelete(expectDelete, blob);
    }

    @Test
    public void testDeleteMutableBlobHDFS() throws Exception {
        testDeleteMutableBlob(3000, true);
    }

    @Test
    public void testDeleteMutableBlobHBase() throws Exception {
        testDeleteMutableBlob(150, true);
    }
    
    @Test
    public void testDeleteMutableBlobInline() throws Exception {
        testDeleteMutableBlob(50, false);
    }
    
    private void testDeleteMutableBlob(int size, boolean expectDelete) throws Exception {
        QName fieldName = new QName("test", "testDeleteMutableBlob"+size);
        FieldType fieldType = typeManager.newFieldType(typeManager.getValueType("BLOB", false, false), fieldName,
                Scope.VERSIONED_MUTABLE);
        fieldType = typeManager.createFieldType(fieldType);
        RecordType recordType = typeManager.newRecordType(new QName(null, "testDeleteMutableBlobRT"+size));
        FieldTypeEntry fieldTypeEntry = typeManager.newFieldTypeEntry(fieldType.getId(), false);
        recordType.addFieldTypeEntry(fieldTypeEntry);
        recordType = typeManager.createRecordType(recordType);

        byte[] bytes = new byte[size]; 
        random.nextBytes(bytes);
        Blob blob = writeBlob(bytes, "aMediaType", "testDeleteMutableBlob");

        byte[] bytes2 = new byte[size]; 
        random.nextBytes(bytes2);
        Blob blob2 = writeBlob(bytes2, "aMediaType", "testDeleteMutableBlob2");
        
        Record record = repository.newRecord();
        record.setRecordType(recordType.getName(), null);
        record.setField(fieldName, blob);
        record = repository.create(record);

        Record record2 = repository.newRecord(record.getId());
        record2.setRecordType(recordType.getName(), null);
        record2.setField(fieldName, blob2);
        repository.update(record2, false, false);
        
        // Blob1 should still exist
        byte[] readBytes = readBlob(record.getId(), record.getVersion(), fieldName, null, null, size);
        assertTrue(Arrays.equals(bytes, readBytes));
        // Blob2 should still exist
        readBytes = readBlob(record2.getId(), record2.getVersion(), fieldName, null, null, size);
        assertTrue(Arrays.equals(bytes2, readBytes));
        
        Record record3 = repository.newRecord(record.getId());
        record3.setRecordType(recordType.getName(), null);
        record3.addFieldsToDelete(Arrays.asList(new QName[]{fieldName}));
        record3.setVersion(record.getVersion());
        repository.update(record3, true, false);
        
        // Blob2 should still exist
        readBytes = readBlob(record2.getId(), record2.getVersion(), fieldName, null, null, size);
        assertTrue(Arrays.equals(bytes2, readBytes));
        
        assertBlobDelete(expectDelete, blob);
    }
    
    @Test
    public void testUpdateMutableMultivalueBlobHDFS() throws Exception {
        testUpdateMutableMultivalueBlob(3000, true);
    }

    @Test
    public void testUpdateMutableMultivalueBlobHBase() throws Exception {
        testUpdateMutableMultivalueBlob(150, true);
    }
    
    @Test
    public void testUpdateMutableMultivalueBlobInline() throws Exception {
        testUpdateMutableMultivalueBlob(50, false);
    }
    
    private void testUpdateMutableMultivalueBlob(int size, boolean expectDelete) throws Exception {
        QName fieldName = new QName("test", "testUpdateMutableMultivalueBlob"+size);
        FieldType fieldType = typeManager.newFieldType(typeManager.getValueType("BLOB", true, false), fieldName,
                Scope.VERSIONED_MUTABLE);
        fieldType = typeManager.createFieldType(fieldType);
        RecordType recordType = typeManager.newRecordType(new QName(null, "testUpdateMutableMultivalueBlobRT"+size));
        FieldTypeEntry fieldTypeEntry = typeManager.newFieldTypeEntry(fieldType.getId(), true);
        recordType.addFieldTypeEntry(fieldTypeEntry);
        recordType = typeManager.createRecordType(recordType);

        byte[] bytes = new byte[size]; 
        random.nextBytes(bytes);
        Blob blob = writeBlob(bytes, "aMediaType", "testUpdateMutableMultivalueBlob");

        byte[] bytes2 = new byte[size]; 
        random.nextBytes(bytes2);
        Blob blob2 = writeBlob(bytes2, "aMediaType", "testUpdateMutableMultivalueBlob2");
        
        byte[] bytes3 = new byte[size]; 
        random.nextBytes(bytes3);
        Blob blob3 = writeBlob(bytes3, "aMediaType", "testUpdateMutableMultivalueBlob3");
        
        byte[] bytes4 = new byte[size]; 
        random.nextBytes(bytes4);
        Blob blob4 = writeBlob(bytes4, "aMediaType", "testUpdateMutableMultivalueBlob4");
        
        Record record = repository.newRecord();
        record.setRecordType(recordType.getName(), null);
        record.setField(fieldName, Arrays.asList(new Blob[]{blob, blob2}));
        record = repository.create(record);

        Record record2 = repository.newRecord(record.getId());
        record2.setRecordType(recordType.getName(), null);
        record2.setField(fieldName, Arrays.asList(new Blob[]{blob2, blob3}));
        record2 = repository.update(record2, false, false);

        // Mutable update of first version
        Record record3 = repository.newRecord(record.getId());
        record3.setVersion(record.getVersion());
        record3.setRecordType(recordType.getName(), null);
        record3.setField(fieldName, Arrays.asList(new Blob[]{blob4}));
        record3 = repository.update(record3, true, false);
        
        //Blob2
        byte[] readBytes = readBlob(record2.getId(), record2.getVersion(), fieldName, 0, null, size);
        assertTrue(Arrays.equals(bytes2, readBytes));

        //Blob3
        readBytes = readBlob(record2.getId(), record2.getVersion(), fieldName, 1, null, size);
        assertTrue(Arrays.equals(bytes3, readBytes));
        
        //Blob4 in version 1
        readBytes = readBlob(record.getId(), record.getVersion(), fieldName, 0, null, size);
        assertTrue(Arrays.equals(bytes4, readBytes));
        
        assertBlobDelete(expectDelete, blob);
        
        try {
            readBlob(record.getId(), record.getVersion(), fieldName, null, null, size);
            fail("BlobNotFoundException expected since index should not be null");
        } catch (BlobNotFoundException expected) {
            
        }

        try {
            readBlob(record.getId(), record.getVersion(), fieldName, 1, null, size);
            fail("BlobNotFoundException expected since index is out of bounds");
        } catch (BlobNotFoundException expected) {
            
        }

    }
    
    @Test
    public void testUpdateMutableHierarchyBlobHDFS() throws Exception {
        testUpdateMutableHierarchyBlob(3000, true);
    }
    
    @Test
    public void testUpdateMutableHierarchyBlobHBase() throws Exception {
        testUpdateMutableHierarchyBlob(150, true);
    }
    
    @Test
    public void testUpdateMutableHierarchyBlobInline() throws Exception {
        testUpdateMutableHierarchyBlob(50, false);
    }
    
    private void testUpdateMutableHierarchyBlob(int size, boolean expectDelete) throws Exception {
        QName fieldName = new QName("test", "testUpdateMutableHierarchyBlob"+size);
        FieldType fieldType = typeManager.newFieldType(typeManager.getValueType("BLOB", false, true), fieldName,
                Scope.VERSIONED_MUTABLE);
        fieldType = typeManager.createFieldType(fieldType);
        RecordType recordType = typeManager.newRecordType(new QName(null, "testUpdateMutableHierarchyBlobRT"+size));
        FieldTypeEntry fieldTypeEntry = typeManager.newFieldTypeEntry(fieldType.getId(), true);
        recordType.addFieldTypeEntry(fieldTypeEntry);
        recordType = typeManager.createRecordType(recordType);

        byte[] bytes = new byte[size]; 
        random.nextBytes(bytes);
        Blob blob = writeBlob(bytes, "aMediaType", "testUpdateMutableHierarchyBlob");

        byte[] bytes2 = new byte[size]; 
        random.nextBytes(bytes2);
        Blob blob2 = writeBlob(bytes2, "aMediaType", "testUpdateMutableHierarchyBlob2");
        
        byte[] bytes3 = new byte[size]; 
        random.nextBytes(bytes3);
        Blob blob3 = writeBlob(bytes3, "aMediaType", "testUpdateMutableHierarchyBlob3");
        
        byte[] bytes4 = new byte[size]; 
        random.nextBytes(bytes4);
        Blob blob4 = writeBlob(bytes4, "aMediaType", "testUpdateMutableHierarchyBlob4");
        
        Record record = repository.newRecord();
        record.setRecordType(recordType.getName(), null);
        record.setField(fieldName, new HierarchyPath(blob, blob2));
        record = repository.create(record);

        Record record2 = repository.newRecord(record.getId());
        record2.setRecordType(recordType.getName(), null);
        record2.setField(fieldName, new HierarchyPath(blob2, blob3, blob4));
        record2 = repository.update(record2, false, false);

        // Mutable update of first version
        Record record3 = repository.newRecord(record.getId());
        record3.setVersion(record.getVersion());
        record3.setRecordType(recordType.getName(), null);
        record3.setField(fieldName, new HierarchyPath(blob4, blob4));
        record3 = repository.update(record3, true, false);
        
        // Blob2
        byte[] readBytes = readBlob(record2.getId(), record2.getVersion(), fieldName, null, 0, size);
        assertTrue(Arrays.equals(bytes2, readBytes));
        
        // Blob3
        readBytes = readBlob(record2.getId(), record2.getVersion(), fieldName, null, 1, size);
        assertTrue(Arrays.equals(bytes3, readBytes));
        
        // Blob4 in version1
        readBytes = readBlob(record.getId(), record.getVersion(), fieldName, null, 1, size);
        assertTrue(Arrays.equals(bytes4, readBytes));
        
        assertBlobDelete(expectDelete, blob);

        try {
            readBlob(record.getId(), record.getVersion(), fieldName, null, null, size);
            fail("BlobNotFoundException expected since index should not be null");
        } catch (BlobNotFoundException expected) {
            
        }

        try {
            readBlob(record.getId(), record.getVersion(), fieldName, null, 2, size);
            fail("BlobNotFoundException expected since index is out of bounds");
        } catch (BlobNotFoundException expected) {
            
        }
    }

    @Test
    public void testUpdateMutableMultivalueHierarchyBlobHDFS() throws Exception {
        testUpdateMutableMultivalueHierarchyBlob(3000, true);
    }
    
    @Test
    public void testUpdateMutableMultivalueHierarchyBlobHBase() throws Exception {
        testUpdateMutableMultivalueHierarchyBlob(150, true);
    }

    @Test
    public void testUpdateMutableMultivalueHierarchyBlobInline() throws Exception {
        testUpdateMutableMultivalueHierarchyBlob(50, false);
    }

    private void testUpdateMutableMultivalueHierarchyBlob(int size, boolean expectDelete) throws Exception {
        QName fieldName = new QName("test", "testUpdateMutableMultivalueHierarchyBlob"+size);
        FieldType fieldType = typeManager.newFieldType(typeManager.getValueType("BLOB", true, true), fieldName,
                Scope.VERSIONED_MUTABLE);
        fieldType = typeManager.createFieldType(fieldType);
        RecordType recordType = typeManager.newRecordType(new QName(null, "testUpdateMutableMultivalueHierarchyBlobRT"+size));
        FieldTypeEntry fieldTypeEntry = typeManager.newFieldTypeEntry(fieldType.getId(), true);
        recordType.addFieldTypeEntry(fieldTypeEntry);
        recordType = typeManager.createRecordType(recordType);

        byte[] bytes = new byte[size]; 
        random.nextBytes(bytes);
        Blob blob = writeBlob(bytes, "aMediaType", "testUpdateMutableMultivalueHierarchyBlob");

        byte[] bytes2 = new byte[size]; 
        random.nextBytes(bytes);
        Blob blob2 = writeBlob(bytes2, "aMediaType", "testUpdateMutableMultivalueHierarchyBlob2");
        
        byte[] bytes3 = new byte[size]; 
        random.nextBytes(bytes3);
        Blob blob3 = writeBlob(bytes3, "aMediaType", "testUpdateMutableMultivalueHierarchyBlob3");
        
        byte[] bytes4 = new byte[size]; 
        random.nextBytes(bytes4);
        Blob blob4 = writeBlob(bytes4, "aMediaType", "testUpdateMutableMultivalueHierarchyBlob4");
        
        Record record = repository.newRecord();
        record.setRecordType(recordType.getName(), null);
        record.setField(fieldName, Arrays.asList(new HierarchyPath[]{new HierarchyPath(blob, blob2), new HierarchyPath(blob3)}));
        record = repository.create(record);

        Record record2 = repository.newRecord(record.getId());
        record2.setRecordType(recordType.getName(), null);
        record2.setField(fieldName, Arrays.asList(new HierarchyPath[]{new HierarchyPath(blob2), new HierarchyPath(blob3, blob4)}));
        record2 = repository.update(record2, false, false);

        // Mutable update of first version
        Record record3 = repository.newRecord(record.getId());
        record3.setVersion(record.getVersion());
        record3.setRecordType(recordType.getName(), null);
        record3.setField(fieldName, Arrays.asList(new HierarchyPath[]{new HierarchyPath(blob3, blob4), new HierarchyPath(blob4)}));
        record3 = repository.update(record3, true, false);
        
        // Blob2
        byte[] readBytes = readBlob(record2.getId(), record2.getVersion(), fieldName, 0, 0, size);
        assertTrue(Arrays.equals(bytes2, readBytes));
        
        // Blob3
        readBytes = readBlob(record2.getId(), record2.getVersion(), fieldName, 1, 0, size);
        assertTrue(Arrays.equals(bytes3, readBytes));
        
        // Blob4 in version1
        readBytes = readBlob(record.getId(), record.getVersion(), fieldName, 0, 1, size);
        assertTrue(Arrays.equals(bytes4, readBytes));
        
        assertBlobDelete(expectDelete, blob);
        
        try {
            readBlob(record.getId(), record.getVersion(), fieldName, null, null, size);
            fail("BlobNotFoundException expected since index should not be null");
        } catch (BlobNotFoundException expected) {
            
        }
        
        try {
            readBlob(record.getId(), record.getVersion(), fieldName, 0, null, size);
            fail("BlobNotFoundException expected since index should not be null");
        } catch (BlobNotFoundException expected) {
            
        }

        try {
            readBlob(record.getId(), record.getVersion(), fieldName, 2, 0, size);
            fail("BlobNotFoundException expected since index is out of bounds");
        } catch (BlobNotFoundException expected) {
            
        }
        
        try {
            readBlob(record.getId(), record.getVersion(), fieldName, 1, 1, size);
            fail("BlobNotFoundException expected since index is out of bounds");
        } catch (BlobNotFoundException expected) {
            
        }
    }
    
    @Test
    public void testDelete() throws Exception {
        QName fieldName = new QName("test", "testDelete");
        FieldType fieldType = typeManager.newFieldType(typeManager.getValueType("BLOB", false, false), fieldName,
                Scope.NON_VERSIONED);
        fieldType = typeManager.createFieldType(fieldType);
        RecordType recordType = typeManager.newRecordType(new QName(null, "testDeleteRT"));
        FieldTypeEntry fieldTypeEntry = typeManager.newFieldTypeEntry(fieldType.getId(), true);
        recordType.addFieldTypeEntry(fieldTypeEntry);
        recordType = typeManager.createRecordType(recordType);
        
        byte[] bytes = new byte[3000]; 
        random.nextBytes(bytes);
        Blob blob = writeBlob(bytes, "aMediaType", "testCreate");
        Record record = repository.newRecord();
        record.setRecordType(recordType.getName());
        record.setField(fieldName, blob);
        record = repository.create(record);
        
        repository.delete(record.getId());
        
        assertBlobDelete(true, blob);
    }
    
    @Test
    public void testDeleteMultivalueHierarchyBlobSmall() throws Exception {
        testDeleteMultivalueHierarchyBlob(50, false); // An inputstream for the inline blob is created on the blobKey directly 
    }
    
    @Test
    public void testDeleteMultivalueHierarchyBlobMedium() throws Exception {
        testDeleteMultivalueHierarchyBlob(150, true);
    }
    
    @Test
    public void testDeleteMultivalueHierarchyBlobLarge() throws Exception {
        testDeleteMultivalueHierarchyBlob(3000, true);
    }
    
    private void testDeleteMultivalueHierarchyBlob(int size, boolean expectDelete) throws Exception {
        QName fieldName = new QName("test", "testDeleteMultivalueHierarchyBlob"+size);
        FieldType fieldType = typeManager.newFieldType(typeManager.getValueType("BLOB", true, true), fieldName,
                Scope.VERSIONED_MUTABLE);
        fieldType = typeManager.createFieldType(fieldType);
        RecordType recordType = typeManager.newRecordType(new QName(null, "testDeleteMultivalueHierarchyBlobRT"+size));
        FieldTypeEntry fieldTypeEntry = typeManager.newFieldTypeEntry(fieldType.getId(), true);
        recordType.addFieldTypeEntry(fieldTypeEntry);
        recordType = typeManager.createRecordType(recordType);

        byte[] bytes = new byte[size]; 
        random.nextBytes(bytes);
        Blob blob = writeBlob(bytes, "aMediaType", "testUpdateMutableMultivalueHierarchyBlob");

        byte[] bytes2 = new byte[size]; 
        random.nextBytes(bytes);
        Blob blob2 = writeBlob(bytes2, "aMediaType", "testUpdateMutableMultivalueHierarchyBlob2");
        
        byte[] bytes3 = new byte[size]; 
        random.nextBytes(bytes3);
        Blob blob3 = writeBlob(bytes3, "aMediaType", "testUpdateMutableMultivalueHierarchyBlob3");
        
        byte[] bytes4 = new byte[size]; 
        random.nextBytes(bytes4);
        Blob blob4 = writeBlob(bytes4, "aMediaType", "testUpdateMutableMultivalueHierarchyBlob4");
        
        Record record = repository.newRecord();
        record.setRecordType(recordType.getName(), null);
        record.setField(fieldName, Arrays.asList(new HierarchyPath[]{new HierarchyPath(blob, blob2), new HierarchyPath(blob3)}));
        record = repository.create(record);

        Record record2 = repository.newRecord(record.getId());
        record2.setRecordType(recordType.getName(), null);
        record2.setField(fieldName, Arrays.asList(new HierarchyPath[]{new HierarchyPath(blob2), new HierarchyPath(blob3, blob4)}));
        record2 = repository.update(record2, false, false);

        repository.delete(record.getId());
        
        assertBlobDelete(expectDelete, blob);
        assertBlobDelete(expectDelete, blob2);
        assertBlobDelete(expectDelete, blob3);
        assertBlobDelete(expectDelete, blob4);
    }
    
    @Test
    public void testBlobIncubatorMonitorUnusedBlob() throws Exception {
        QName fieldName = new QName("test", "testBlobIncubatorMonitorUnusedBlob");
        FieldType fieldType = typeManager.newFieldType(typeManager.getValueType("BLOB", false, false), fieldName,
                Scope.NON_VERSIONED);
        fieldType = typeManager.createFieldType(fieldType);
        RecordType recordType = typeManager.newRecordType(new QName(null, "testBlobIncubatorMonitorUnusedBlobRT"));
        FieldTypeEntry fieldTypeEntry = typeManager.newFieldTypeEntry(fieldType.getId(), true);
        recordType.addFieldTypeEntry(fieldTypeEntry);
        recordType = typeManager.createRecordType(recordType);
        
        // Incubate blob but never use it
        byte[] bytes = new byte[3000]; 
        random.nextBytes(bytes);
        Blob blob = writeBlob(bytes, "aMediaType", "testCreate");  
        
        BlobIncubatorMonitor monitor = new BlobIncubatorMonitor(zooKeeper, hbaseTableFactory, blobManager, typeManager, 1000, 100, 0);
        monitor.startMonitoring();
        Thread.sleep(10000);
        monitor.stopMonitoring();
        
        assertBlobDelete(true, blob);
    }

    @Test
    public void testBlobIncubatorMonitorFailureAfterReservation() throws Exception {
        QName fieldName = new QName("test", "testBlobIncubatorMonitorFailureAfterReservation");
        FieldType fieldType = typeManager.newFieldType(typeManager.getValueType("BLOB", false, false), fieldName,
                Scope.NON_VERSIONED);
        fieldType = typeManager.createFieldType(fieldType);
        RecordType recordType = typeManager.newRecordType(new QName(null, "testBlobIncubatorMonitorFailureAfterReservationRT"));
        FieldTypeEntry fieldTypeEntry = typeManager.newFieldTypeEntry(fieldType.getId(), true);
        recordType.addFieldTypeEntry(fieldTypeEntry);
        recordType = typeManager.createRecordType(recordType);
        
    // This is the failure scenario where creating the record fails after reserving the blob
        byte[] bytes = new byte[3000]; 
        random.nextBytes(bytes);
        Blob blob = writeBlob(bytes, "aMediaType", "testCreate");
        IdGeneratorImpl idGeneratorImpl = new IdGeneratorImpl();
        RecordId recordId = idGeneratorImpl.newRecordId();
        BlobReference blobReference = new BlobReference(blob, recordId, fieldType);
        Set<BlobReference> blobs = new HashSet<BlobReference>();
        blobs.add(blobReference);
        blobManager.reserveBlobs(blobs);
        
        BlobIncubatorMonitor monitor = new BlobIncubatorMonitor(zooKeeper, hbaseTableFactory, blobManager, typeManager, 1000, 100, 0);
        monitor.startMonitoring();
        Thread.sleep(10000);
        monitor.stopMonitoring();
     
        assertBlobDelete(true, blob);
    }
    
    @Test
    public void testBlobIncubatorMonitorFailureBeforeRemovingReservation() throws Exception {
        QName fieldName = new QName("test", "testBlobIncubatorMonitorFailureBeforeRemovingReservation");
        FieldType fieldType = typeManager.newFieldType(typeManager.getValueType("BLOB", false, false), fieldName,
                Scope.NON_VERSIONED);
        fieldType = typeManager.createFieldType(fieldType);
        RecordType recordType = typeManager.newRecordType(new QName(null, "testBlobIncubatorMonitorFailureBeforeRemovingReservation"));
        FieldTypeEntry fieldTypeEntry = typeManager.newFieldTypeEntry(fieldType.getId(), true);
        recordType.addFieldTypeEntry(fieldTypeEntry);
        recordType = typeManager.createRecordType(recordType);
        
    // This is the failure scenario where creating the record fails after reserving the blob
        byte[] bytes = new byte[3000]; 
        random.nextBytes(bytes);
        Blob blob = writeBlob(bytes, "aMediaType", "testCreate");
        IdGeneratorImpl idGeneratorImpl = new IdGeneratorImpl();
        RecordId recordId = idGeneratorImpl.newRecordId();
        BlobReference blobReference = new BlobReference(blob, recordId, fieldType);
        Set<BlobReference> blobs = new HashSet<BlobReference>();
        blobs.add(blobReference);
        repository.newRecord();
        Record record = repository.newRecord();
        record.setRecordType(recordType.getName());
        record.setField(fieldName, blob);
        record = repository.create(record);
        
        // Faking failure
        HTableInterface blobIncubatorTable = LilyHBaseSchema.getBlobIncubatorTable(hbaseTableFactory, true);
        Put put = new Put(blob.getValue());
        put.add(LilyHBaseSchema.BlobIncubatorCf.REF.bytes, LilyHBaseSchema.BlobIncubatorColumn.RECORD.bytes, record.getId().toBytes());
        put.add(LilyHBaseSchema.BlobIncubatorCf.REF.bytes, LilyHBaseSchema.BlobIncubatorColumn.FIELD.bytes, ((FieldTypeImpl)fieldType).getIdBytes());
        blobIncubatorTable.put(put);
        
        BlobIncubatorMonitor monitor = new BlobIncubatorMonitor(zooKeeper, hbaseTableFactory, blobManager, typeManager, 1000, 100, 0);
        monitor.startMonitoring();
        Thread.sleep(10000);
        monitor.stopMonitoring();
     
        assertBlobDelete(false, blob);
        Get get = new Get(blob.getValue());
        Result result = blobIncubatorTable.get(get);
        assertTrue(result == null || result.isEmpty());
    }
        
    private void assertBlobDelete(boolean expectDelete, Blob blob) throws BlobNotFoundException, BlobException {
        if (expectDelete) {
            try {
                testBlobStoreAccessRegistry.getInputStream(blob);
                fail("The blob " + blob + " should have been deleted.");
            } catch (BlobException expected) {
            }
        } else {
            testBlobStoreAccessRegistry.getInputStream(blob);
        }
    }
    
    @Test
    public void testBadEncoding() throws Exception {
        Blob blob = new Blob("aMediaType", (long) 10, "aName");
        blob.setValue(new byte[0]);
        try {
            testBlobStoreAccessRegistry.getInputStream(blob);
            fail();
        } catch (BlobException expected) {
        }
    }

    private Blob writeBlob(byte[] bytes, String mediaType, String name) throws BlobException, InterruptedException,
            IOException {
        Blob blob = new Blob(mediaType, (long) bytes.length, name);
        OutputStream outputStream = repository.getOutputStream(blob);
        outputStream.write(bytes);
        outputStream.close();
        return blob;
    }
    
    private byte[] readBlob(RecordId recordId, QName fieldName, long size) throws BlobNotFoundException, BlobException, RecordNotFoundException, RecordTypeNotFoundException, FieldTypeNotFoundException, RecordException, VersionNotFoundException, TypeException, InterruptedException, IOException {
        return readBlob(recordId, null, fieldName, null, null, size);
    }
    
    private byte[] readBlob(RecordId recordId, Long version, QName fieldName, Integer multivalueIndex, Integer hierarchyIndex, long size) throws BlobNotFoundException, BlobException, InterruptedException, IOException, RecordNotFoundException, RecordTypeNotFoundException, FieldTypeNotFoundException, RecordException, VersionNotFoundException, TypeException {
        InputStream inputStream = repository.getInputStream(recordId, version, fieldName, multivalueIndex, hierarchyIndex);
        byte[] readBytes = new byte[(int)size];
        inputStream.read(readBytes);
        inputStream.close();
        return readBytes;
    }
}
