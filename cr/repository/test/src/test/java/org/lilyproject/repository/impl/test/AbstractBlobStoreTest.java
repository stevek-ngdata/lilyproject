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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.lilyproject.repository.api.Blob;
import org.lilyproject.repository.api.BlobException;
import org.lilyproject.repository.api.BlobManager;
import org.lilyproject.repository.api.BlobNotFoundException;
import org.lilyproject.repository.api.BlobReference;
import org.lilyproject.repository.api.FieldNotFoundException;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.FieldTypeEntry;
import org.lilyproject.repository.api.FieldTypeNotFoundException;
import org.lilyproject.repository.api.HierarchyPath;
import org.lilyproject.repository.api.InvalidRecordException;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.RecordNotFoundException;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.api.ValueType;
import org.lilyproject.repository.impl.BlobIncubatorMonitor;
import org.lilyproject.repository.impl.BlobStoreAccessRegistry;
import org.lilyproject.repository.impl.id.IdGeneratorImpl;
import org.lilyproject.repotestfw.RepositorySetup;
import org.lilyproject.util.hbase.LilyHBaseSchema;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class AbstractBlobStoreTest {
    private static String namespace = "test";
    protected static final RepositorySetup repoSetup = new RepositorySetup();
    static {
        repoSetup.setBlobLimits(50, 1024);
    }

    protected static Repository repository;
    protected static TypeManager typeManager;

    protected static Random random = new Random();
    protected static BlobStoreAccessRegistry testBlobStoreAccessRegistry;
    protected static BlobManager blobManager;

    @Test
    public void testCreate() throws Exception {
        QName fieldName = new QName(namespace, "testCreate");
        FieldType fieldType = typeManager.newFieldType(typeManager.getValueType("BLOB"), fieldName,
                Scope.NON_VERSIONED);
        fieldType = typeManager.createFieldType(fieldType);
        RecordType recordType = typeManager.newRecordType(new QName(namespace, "testCreateRT"));
        FieldTypeEntry fieldTypeEntry = typeManager.newFieldTypeEntry(fieldType.getId(), true);
        recordType.addFieldTypeEntry(fieldTypeEntry);
        recordType = typeManager.createRecordType(recordType);

        byte[] bytes = Bytes.toBytes("someBytes");
        Blob blob = writeBlob(bytes, "aMediaType", "testCreate");
        Record record = repository.newRecord();
        record.setRecordType(recordType.getName());
        record.setField(fieldName, blob);
        record = repository.create(record);

        byte[] readBytes = readBlob(record.getId(), fieldName);
        assertTrue(Arrays.equals(bytes, readBytes));

        // Test the getInputStream with giving the record instead of the recordId
        InputStream inputStream = repository.getInputStream(record, fieldName);
        try {
            byte[] readBytes2 = IOUtils.toByteArray(inputStream);
            assertTrue(Arrays.equals(bytes, readBytes2));
        } finally {
            IOUtils.closeQuietly(inputStream);
        }
    }

    @Test
    public void testThreeSizes() throws Exception {
        QName fieldName1 = new QName(namespace, "testThreeSizes1");
        QName fieldName2 = new QName(namespace, "testThreeSizes2");
        QName fieldName3 = new QName(namespace, "testThreeSizes3");
        FieldType fieldType1 = typeManager.newFieldType(typeManager.getValueType("BLOB"), fieldName1,
                Scope.NON_VERSIONED);
        fieldType1 = typeManager.createFieldType(fieldType1);
        FieldType fieldType2 = typeManager.newFieldType(typeManager.getValueType("BLOB"), fieldName2,
                Scope.NON_VERSIONED);
        fieldType2 = typeManager.createFieldType(fieldType2);
        FieldType fieldType3 = typeManager.newFieldType(typeManager.getValueType("BLOB"), fieldName3,
                Scope.NON_VERSIONED);
        fieldType3 = typeManager.createFieldType(fieldType3);
        RecordType recordType = typeManager.newRecordType(new QName(namespace, "testThreeSizes"));
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

        byte[] readBytes = readBlob(record.getId(), fieldName1);
        assertTrue(Arrays.equals(small, readBytes));
        readBytes = readBlob(record.getId(), fieldName2);
        assertTrue(Arrays.equals(medium, readBytes));
        readBytes = readBlob(record.getId(), fieldName3);
        assertTrue(Arrays.equals(large, readBytes));
    }

    /**
     * Test case to reproduce the 'Row key is invalid' problem reported here:
     * https://groups.google.com/forum/#!topic/lily-discuss/XiRxOxJTv70/discussion
     *
     * @throws Exception
     */
    @Test
    public void testForceInline() throws Exception {
        QName fieldName = new QName(namespace, "testForceInline");
        FieldType fieldType = typeManager.newFieldType(typeManager.getValueType("BLOB"), fieldName,
                Scope.NON_VERSIONED);
        fieldType = typeManager.createFieldType(fieldType);
        RecordType recordType = typeManager.newRecordType(new QName(namespace, "testForceInlineRT"));
        FieldTypeEntry fieldTypeEntry = typeManager.newFieldTypeEntry(fieldType.getId(), true);
        recordType.addFieldTypeEntry(fieldTypeEntry);
        recordType = typeManager.createRecordType(recordType);

        int size = 4096;
        Random rg = new Random();
        byte[] bytes = new byte[size];
        rg.nextBytes(bytes);

        // create BLOB object
        Blob blob = new Blob("application/pdf", 0L, "Document");
        // create a stream to write the BLOB
        OutputStream bos = repository.getOutputStream(blob);
        // write the data
        bos.write(bytes);
        bos.close();
        blob.setSize(5L);
        // create a new record ID
        RecordId rid = repository.getIdGenerator().newRecordId();
        // create a new record
        Record record = repository.newRecord(rid);
        record.setRecordType(new QName(namespace, "testForceInlineRT"));
        // set the blob
        record.setField(fieldName, blob);

        // create the record
        record = repository.create(record);

        byte[] readBytes = readBlob(record.getId(), fieldName);
        assertTrue(Arrays.equals(bytes, readBytes));

        // Test the getInputStream with giving the record instead of the recordId
        InputStream inputStream = repository.getInputStream(record, fieldName);
        try {
            byte[] readBytes2 = IOUtils.toByteArray(inputStream);
            assertTrue(Arrays.equals(bytes, readBytes2));
        } finally {
            IOUtils.closeQuietly(inputStream);
        }
    }

    @Test
    public void testCreateTwoRecordsWithSameBlob() throws Exception {
        QName fieldName = new QName(namespace, "ablob2");
        FieldType fieldType = typeManager.newFieldType(typeManager.getValueType("BLOB"), fieldName,
                Scope.VERSIONED);
        fieldType = typeManager.createFieldType(fieldType);
        RecordType recordType = typeManager.newRecordType(new QName(namespace, "testCreateTwoRecordsWithSameBlobRT"));
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
        QName fieldName = new QName(namespace, "testUpdateNonVersionedBlob" + size);
        FieldType fieldType = typeManager.newFieldType(typeManager.getValueType("BLOB"), fieldName,
                Scope.NON_VERSIONED);
        fieldType = typeManager.createFieldType(fieldType);
        RecordType recordType = typeManager.newRecordType(new QName(namespace, "testUpdateNonVersionedBlobRT" + size));
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
        byte[] readBytes = readBlob(record.getId(), record.getVersion(), fieldName);
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
        QName fieldName = new QName(namespace, "testDeleteNonVersionedBlob" + size);
        FieldType fieldType = typeManager.newFieldType(typeManager.getValueType("BLOB"), fieldName,
                Scope.NON_VERSIONED);
        fieldType = typeManager.createFieldType(fieldType);
        RecordType recordType = typeManager.newRecordType(new QName(namespace, "testDeleteNonVersionedBlobRT" + size));
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
        record2.addFieldsToDelete(Arrays.asList(fieldName));

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
        QName fieldName = new QName(namespace, "testUpdateMutableBlob" + size);
        FieldType fieldType = typeManager.newFieldType(typeManager.getValueType("BLOB"), fieldName,
                Scope.VERSIONED_MUTABLE);
        fieldType = typeManager.createFieldType(fieldType);
        RecordType recordType = typeManager.newRecordType(new QName(namespace, "testUpdateMutableBlobRT" + size));
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
        byte[] readBytes = readBlob(record.getId(), record.getVersion(), fieldName);
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
        QName fieldName = new QName(namespace, "testDeleteMutableBlob" + size);
        FieldType fieldType = typeManager.newFieldType(typeManager.getValueType("BLOB"), fieldName,
                Scope.VERSIONED_MUTABLE);
        fieldType = typeManager.createFieldType(fieldType);
        RecordType recordType = typeManager.newRecordType(new QName(namespace, "testDeleteMutableBlobRT" + size));
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
        byte[] readBytes = readBlob(record.getId(), record.getVersion(), fieldName);
        assertTrue(Arrays.equals(bytes, readBytes));
        // Blob2 should still exist
        readBytes = readBlob(record2.getId(), record2.getVersion(), fieldName);
        assertTrue(Arrays.equals(bytes2, readBytes));

        Record record3 = repository.newRecord(record.getId());
        record3.setRecordType(recordType.getName(), null);
        record3.addFieldsToDelete(Arrays.asList(fieldName));
        record3.setVersion(record.getVersion());
        repository.update(record3, true, false);

        // Blob2 should still exist
        readBytes = readBlob(record2.getId(), record2.getVersion(), fieldName);
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
        QName fieldName = new QName(namespace, "testUpdateMutableMultivalueBlob" + size);
        FieldType fieldType = typeManager.newFieldType(typeManager.getValueType("LIST<BLOB>"), fieldName, Scope.VERSIONED_MUTABLE);
        fieldType = typeManager.createFieldType(fieldType);
        RecordType recordType = typeManager.newRecordType(new QName(namespace, "testUpdateMutableMultivalueBlobRT"
                + size));
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
        record.setField(fieldName, Arrays.asList(blob, blob2));
        record = repository.create(record);

        Record record2 = repository.newRecord(record.getId());
        record2.setRecordType(recordType.getName(), null);
        record2.setField(fieldName, Arrays.asList(blob2, blob3));
        record2 = repository.update(record2, false, false);

        // Mutable update of first version
        Record record3 = repository.newRecord(record.getId());
        record3.setVersion(record.getVersion());
        record3.setRecordType(recordType.getName(), null);
        record3.setField(fieldName, Arrays.asList(blob4));
        record3 = repository.update(record3, true, false);

        //Blob2
        byte[] readBytes = readBlob(record2.getId(), record2.getVersion(), fieldName, 0);
        assertTrue(Arrays.equals(bytes2, readBytes));

        //Blob3
        readBytes = readBlob(record2.getId(), record2.getVersion(), fieldName, 1);
        assertTrue(Arrays.equals(bytes3, readBytes));

        //Blob4 in version 1
        readBytes = readBlob(record.getId(), record.getVersion(), fieldName, 0);
        assertTrue(Arrays.equals(bytes4, readBytes));

        assertBlobDelete(expectDelete, blob);

        try {
            readBlob(record.getId(), record.getVersion(), fieldName);
            fail("BlobNotFoundException expected since index should not be null");
        } catch (BlobNotFoundException expected) {

        }

        try {
            readBlob(record.getId(), record.getVersion(), fieldName, 1);
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
        QName fieldName = new QName(namespace, "testUpdateMutableHierarchyBlob" + size);
        FieldType fieldType = typeManager.newFieldType(typeManager.getValueType("PATH<BLOB>"), fieldName,
                Scope.VERSIONED_MUTABLE);
        fieldType = typeManager.createFieldType(fieldType);
        RecordType recordType = typeManager.newRecordType(new QName(namespace, "testUpdateMutableHierarchyBlobRT"
                + size));
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
        byte[] readBytes = readBlob(record2.getId(), record2.getVersion(), fieldName, 0);
        assertTrue(Arrays.equals(bytes2, readBytes));

        // Blob3
        readBytes = readBlob(record2.getId(), record2.getVersion(), fieldName, 1);
        assertTrue(Arrays.equals(bytes3, readBytes));

        // Blob4 in version1
        readBytes = readBlob(record.getId(), record.getVersion(), fieldName, 1);
        assertTrue(Arrays.equals(bytes4, readBytes));

        assertBlobDelete(expectDelete, blob);

        try {
            readBlob(record.getId(), record.getVersion(), fieldName);
            fail("BlobNotFoundException expected since index should not be null");
        } catch (BlobNotFoundException expected) {

        }

        try {
            readBlob(record.getId(), record.getVersion(), fieldName, 2);
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
        QName fieldName = new QName(namespace, "testUpdateMutableMultivalueHierarchyBlob" + size);
        FieldType fieldType = typeManager.newFieldType(typeManager.getValueType("LIST<PATH<BLOB>>"), fieldName,
                Scope.VERSIONED_MUTABLE);
        fieldType = typeManager.createFieldType(fieldType);
        RecordType recordType = typeManager.newRecordType(new QName(namespace,
                "testUpdateMutableMultivalueHierarchyBlobRT" + size));
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
        record.setField(fieldName, Arrays.asList(new HierarchyPath(blob, blob2), new HierarchyPath(blob3)));
        record = repository.create(record);

        Record record2 = repository.newRecord(record.getId());
        record2.setRecordType(recordType.getName(), null);
        record2.setField(fieldName, Arrays.asList(new HierarchyPath(blob2), new HierarchyPath(blob3, blob4)));
        record2 = repository.update(record2, false, false);

        // Mutable update of first version
        Record record3 = repository.newRecord(record.getId());
        record3.setVersion(record.getVersion());
        record3.setRecordType(recordType.getName(), null);
        record3.setField(fieldName, Arrays.asList(new HierarchyPath(blob3, blob4), new HierarchyPath(blob4)));
        record3 = repository.update(record3, true, false);

        // Blob2
        byte[] readBytes = readBlob(record2.getId(), record2.getVersion(), fieldName, 0, 0);
        assertTrue(Arrays.equals(bytes2, readBytes));

        // Blob3
        readBytes = readBlob(record2.getId(), record2.getVersion(), fieldName, 1, 0);
        assertTrue(Arrays.equals(bytes3, readBytes));

        // Blob4 in version1
        readBytes = readBlob(record.getId(), record.getVersion(), fieldName, 0, 1);
        assertTrue(Arrays.equals(bytes4, readBytes));

        assertBlobDelete(expectDelete, blob);

        try {
            readBlob(record.getId(), record.getVersion(), fieldName);
            fail("BlobNotFoundException expected since index should not be null");
        } catch (BlobNotFoundException expected) {

        }

        try {
            readBlob(record.getId(), record.getVersion(), fieldName, 0);
            fail("BlobNotFoundException expected since index should not be null");
        } catch (BlobNotFoundException expected) {

        }

        try {
            readBlob(record.getId(), record.getVersion(), fieldName, 2, 0);
            fail("BlobNotFoundException expected since index is out of bounds");
        } catch (BlobNotFoundException expected) {

        }

        try {
            readBlob(record.getId(), record.getVersion(), fieldName, 1, 1);
            fail("BlobNotFoundException expected since index is out of bounds");
        } catch (BlobNotFoundException expected) {

        }
    }

    @Test
    public void testDelete() throws Exception {
        QName fieldName = new QName(namespace, "testDelete");
        FieldType fieldType = typeManager.newFieldType(typeManager.getValueType("BLOB"), fieldName,
                Scope.NON_VERSIONED);
        fieldType = typeManager.createFieldType(fieldType);
        RecordType recordType = typeManager.newRecordType(new QName(namespace, "testDeleteRT"));
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
        QName fieldName = new QName(namespace, "testDeleteMultivalueHierarchyBlob" + size);
        FieldType fieldType = typeManager.newFieldType(typeManager.getValueType("LIST<PATH<BLOB>>"), fieldName,
                Scope.VERSIONED_MUTABLE);
        fieldType = typeManager.createFieldType(fieldType);
        RecordType recordType = typeManager.newRecordType(new QName(namespace, "testDeleteMultivalueHierarchyBlobRT"
                + size));
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
        record.setField(fieldName, Arrays.asList(new HierarchyPath(blob, blob2), new HierarchyPath(blob3)));
        record = repository.create(record);

        Record record2 = repository.newRecord(record.getId());
        record2.setRecordType(recordType.getName(), null);
        record2.setField(fieldName, Arrays.asList(new HierarchyPath(blob2), new HierarchyPath(blob3, blob4)));
        record2 = repository.update(record2, false, false);

        repository.delete(record.getId());

        assertBlobDelete(expectDelete, blob);
        assertBlobDelete(expectDelete, blob2);
        assertBlobDelete(expectDelete, blob3);
        assertBlobDelete(expectDelete, blob4);
    }

    @Test
    public void testBlobIncubatorMonitorUnusedBlob() throws Exception {
        QName fieldName = new QName(namespace, "testBlobIncubatorMonitorUnusedBlob");
        FieldType fieldType = typeManager.newFieldType(typeManager.getValueType("BLOB"), fieldName,
                Scope.NON_VERSIONED);
        fieldType = typeManager.createFieldType(fieldType);
        RecordType recordType = typeManager.newRecordType(new QName(namespace, "testBlobIncubatorMonitorUnusedBlobRT"));
        FieldTypeEntry fieldTypeEntry = typeManager.newFieldTypeEntry(fieldType.getId(), true);
        recordType.addFieldTypeEntry(fieldTypeEntry);
        recordType = typeManager.createRecordType(recordType);

        // Incubate blob but never use it
        byte[] bytes = new byte[3000];
        random.nextBytes(bytes);
        Blob blob = writeBlob(bytes, "aMediaType", "testCreate");

        // Give time for the blob to expire
        Thread.sleep(60);

        BlobIncubatorMonitor monitor = new BlobIncubatorMonitor(repoSetup.getZk(), repoSetup.getHbaseTableFactory(),
                repoSetup.getTableManager(), blobManager, typeManager, 50, 0, 0);
        monitor.runMonitorOnce();

        assertBlobDelete(true, blob);
    }

    @Test
    public void testBlobIncubatorMonitorFailureAfterReservation() throws Exception {
        QName fieldName = new QName(namespace, "testBlobIncubatorMonitorFailureAfterReservation");
        FieldType fieldType = typeManager.newFieldType(typeManager.getValueType("BLOB"), fieldName,
                Scope.NON_VERSIONED);
        fieldType = typeManager.createFieldType(fieldType);
        RecordType recordType = typeManager.newRecordType(new QName(namespace,
                "testBlobIncubatorMonitorFailureAfterReservationRT"));
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

        // Give time for the blob to expire
        Thread.sleep(60);

        BlobIncubatorMonitor monitor = new BlobIncubatorMonitor(repoSetup.getZk(), repoSetup.getHbaseTableFactory(),
                                            repoSetup.getTableManager(), blobManager, typeManager, 50, 0, 0);
        monitor.runMonitorOnce();

        assertBlobDelete(true, blob);
    }

    @Test
    public void testBlobIncubatorMonitorFailureBeforeRemovingReservation() throws Exception {
        QName fieldName = new QName(namespace, "testBlobIncubatorMonitorFailureBeforeRemovingReservation");
        FieldType fieldType = typeManager.newFieldType(typeManager.getValueType("BLOB"), fieldName,
                Scope.NON_VERSIONED);
        fieldType = typeManager.createFieldType(fieldType);
        RecordType recordType = typeManager.newRecordType(new QName(namespace,
                "testBlobIncubatorMonitorFailureBeforeRemovingReservation"));
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
        HTableInterface blobIncubatorTable = LilyHBaseSchema.getBlobIncubatorTable(repoSetup.getHbaseTableFactory(), true);
        Put put = new Put(blob.getValue());
        put.add(LilyHBaseSchema.BlobIncubatorCf.REF.bytes, LilyHBaseSchema.BlobIncubatorColumn.RECORD.bytes, record.getId().toBytes());
        put.add(LilyHBaseSchema.BlobIncubatorCf.REF.bytes, LilyHBaseSchema.BlobIncubatorColumn.FIELD.bytes, fieldType.getId().getBytes());
        blobIncubatorTable.put(put);

        // Give time for the blob to expire
        Thread.sleep(60);

        BlobIncubatorMonitor monitor = new BlobIncubatorMonitor(repoSetup.getZk(), repoSetup.getHbaseTableFactory(),
                                            repoSetup.getTableManager(), blobManager, typeManager, 50, 0, 0);
        monitor.runMonitorOnce();

        assertBlobDelete(false, blob);
        Get get = new Get(blob.getValue());
        Result result = blobIncubatorTable.get(get);
        assertTrue(result == null || result.isEmpty());
    }

    private void assertBlobDelete(boolean expectDelete, Blob blob) throws BlobNotFoundException, BlobException {
        if (expectDelete) {
            try {
                testBlobStoreAccessRegistry.getBlobAccess(blob).getInputStream();
                fail("The blob " + blob + " should have been deleted.");
            } catch (BlobException expected) {
            }
        } else {
            testBlobStoreAccessRegistry.getBlobAccess(blob).getInputStream();
        }
    }

    @Test
    public void testBadEncoding() throws Exception {
        Blob blob = new Blob("aMediaType", (long) 10, "aName");
        blob.setValue(new byte[0]);
        try {
            testBlobStoreAccessRegistry.getBlobAccess(blob).getInputStream();
            fail();
        } catch (BlobException expected) {
        }
    }

    @Test
    public void testInvalidReadRequests() throws Exception {
        ValueType stringType = typeManager.getValueType("STRING");
        ValueType blobType = typeManager.getValueType("BLOB");

        FieldType nonBlobField = typeManager.newFieldType(stringType, new QName(namespace, "NonBlobField"),
                Scope.VERSIONED);
        nonBlobField = typeManager.createFieldType(nonBlobField);

        FieldType absentField = typeManager
                .newFieldType(blobType, new QName(namespace, "AbsentField"), Scope.VERSIONED);
        absentField = typeManager.createFieldType(absentField);

        RecordType rt = typeManager.newRecordType(new QName(namespace, "NoBlobsRT"));
        rt.addFieldTypeEntry(nonBlobField.getId(), false);
        rt = typeManager.createRecordType(rt);

        Record record = repository.newRecord();
        record.setRecordType(rt.getName());
        record.setField(nonBlobField.getName(), "This is not a blob");
        record = repository.create(record);

        try {
            repository.getInputStream(record.getId(), record.getVersion(), nonBlobField.getName(), null, null);
            fail("Expected exception");
        } catch (BlobException e) {
            // ok
        }

        try {
            repository.getInputStream(record.getId(), record.getVersion(), absentField.getName(), null, null);
            fail("Expected exception");
        } catch (FieldNotFoundException e) {
            // ok
        }

        try {
            repository.getInputStream(record.getId(), record.getVersion(),
                    new QName(namespace, "nonExistingFieldType"),
                    null, null);
            fail("Expected exception");
        } catch (FieldTypeNotFoundException e) {
            // ok
        }

        try {
            repository.getInputStream(record.getId(), record.getVersion(), null, null, null);
            fail("Expected exception");
        } catch (IllegalArgumentException e) {
            // ok
        }

        try {
            repository.getInputStream(repoSetup.getIdGenerator().fromString("USER.nonexistingrecord"), null,
                    absentField.getName());
            fail("Expected exception");
        } catch (RecordNotFoundException e) {
            // ok
        }
    }

    private Blob writeBlob(byte[] bytes, String mediaType, String name) throws RepositoryException, InterruptedException,
 IOException {
        return writeBlob(bytes, mediaType, name, bytes.length);
    }

    /**
     * @param length The blob site to be used when constructing the blob
     * (this can be used to control how the blob will be stored)
     */
    private Blob writeBlob(byte[] bytes, String mediaType, String name, long length) throws RepositoryException, InterruptedException,
 IOException {
        Blob blob = new Blob(mediaType, length, name);
        OutputStream outputStream = repository.getOutputStream(blob);
        outputStream.write(bytes);
        outputStream.close();
        return blob;
    }

    private byte[] readBlob(RecordId recordId, QName fieldName) throws RepositoryException,
            InterruptedException, IOException {
        return readBlob(recordId, null, fieldName);
    }

    private byte[] readBlob(RecordId recordId, Long version, QName fieldName, int...indexes)
            throws RepositoryException, InterruptedException, IOException {
        InputStream inputStream = repository.getInputStream(recordId, version, fieldName, indexes);
        try {
            return IOUtils.toByteArray(inputStream);
        } finally {
            IOUtils.closeQuietly(inputStream);
        }
    }
}
