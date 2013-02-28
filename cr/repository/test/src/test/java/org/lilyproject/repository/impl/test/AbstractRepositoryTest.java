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

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hbase.util.Bytes;
import org.easymock.IMocksControl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.lilyproject.bytes.api.ByteArray;
import org.lilyproject.repository.api.Blob;
import org.lilyproject.repository.api.CompareOp;
import org.lilyproject.repository.api.FieldNotFoundException;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.IdRecord;
import org.lilyproject.repository.api.IdRecordScanner;
import org.lilyproject.repository.api.InvalidRecordException;
import org.lilyproject.repository.api.Link;
import org.lilyproject.repository.api.Metadata;
import org.lilyproject.repository.api.MetadataBuilder;
import org.lilyproject.repository.api.MutationCondition;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordBuilder;
import org.lilyproject.repository.api.RecordException;
import org.lilyproject.repository.api.RecordExistsException;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.RecordNotFoundException;
import org.lilyproject.repository.api.RecordScan;
import org.lilyproject.repository.api.RecordScanner;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.RecordTypeNotFoundException;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.ResponseStatus;
import org.lilyproject.repository.api.ReturnFields;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.api.ValueType;
import org.lilyproject.repository.api.VersionNotFoundException;
import org.lilyproject.repository.api.filter.FieldValueFilter;
import org.lilyproject.repository.api.filter.RecordFilterList;
import org.lilyproject.repository.api.filter.RecordIdPrefixFilter;
import org.lilyproject.repository.api.filter.RecordTypeFilter;
import org.lilyproject.repository.api.filter.RecordVariantFilter;
import org.lilyproject.repotestfw.RepositorySetup;
import org.lilyproject.util.Pair;

import static org.easymock.EasyMock.createControl;
import static org.junit.Assert.*;

public abstract class AbstractRepositoryTest {

    protected static final RepositorySetup repoSetup = new RepositorySetup();
    protected static boolean avro = false;

    protected static IdGenerator idGenerator;
    protected static TypeManager typeManager;
    protected static Repository repository;
    protected static FieldType fieldType1;

    private static FieldType fieldType1B;
    private static FieldType fieldType2;
    private static FieldType fieldType3;
    private static FieldType fieldType4;
    private static FieldType fieldType5;
    private static FieldType fieldType6;
    protected static RecordType recordType1;
    private static RecordType recordType1B;
    private static RecordType recordType2;
    private static RecordType recordType3;
    private static String namespace = "/test/repository";


    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    protected static void setupTypes() throws Exception {
        setupFieldTypes();
        setupRecordTypes();
    }

    private static void setupFieldTypes() throws Exception {
        fieldType1 = typeManager.createFieldType(typeManager
                .newFieldType(typeManager.getValueType("STRING"), new QName(namespace, "field1"), Scope.NON_VERSIONED));
        fieldType1B = typeManager.createFieldType(typeManager
                .newFieldType(typeManager.getValueType("STRING"), new QName(namespace, "field1B"),
                        Scope.NON_VERSIONED));
        fieldType2 = typeManager.createFieldType(typeManager
                .newFieldType(typeManager.getValueType("INTEGER"), new QName(namespace, "field2"), Scope.VERSIONED));
        fieldType3 = typeManager.createFieldType(
                typeManager.newFieldType(typeManager.getValueType("BOOLEAN"), new QName(namespace, "field3"),
                        Scope.VERSIONED_MUTABLE));

        fieldType4 = typeManager.createFieldType(typeManager
                .newFieldType(typeManager.getValueType("INTEGER"), new QName(namespace, "field4"),
                        Scope.NON_VERSIONED));
        fieldType5 = typeManager.createFieldType(typeManager
                .newFieldType(typeManager.getValueType("BOOLEAN"), new QName(namespace, "field5"), Scope.VERSIONED));
        fieldType6 = typeManager.createFieldType(typeManager
                .newFieldType(typeManager.getValueType("STRING"), new QName(namespace, "field6"),
                        Scope.VERSIONED_MUTABLE));

    }

    private static void setupRecordTypes() throws Exception {
        recordType1 = typeManager.newRecordType(new QName(namespace, "RT1"));
        recordType1.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType1.getId(), false));
        recordType1.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType2.getId(), false));
        recordType1.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType3.getId(), false));
        recordType1 = typeManager.createRecordType(recordType1);

        recordType1B = recordType1.clone();
        recordType1B.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType1B.getId(), false));
        recordType1B = typeManager.updateRecordType(recordType1B);

        recordType2 = typeManager.newRecordType(new QName(namespace, "RT2"));

        recordType2.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType4.getId(), false));
        recordType2.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType5.getId(), false));
        recordType2.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType6.getId(), false));
        recordType2 = typeManager.createRecordType(recordType2);

        recordType3 = typeManager.newRecordType(new QName(namespace, "RT3"));
        recordType3.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType1.getId(), false));
        recordType3.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType2.getId(), false));
        recordType3.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType3.getId(), false));
        recordType3 = typeManager.createRecordType(recordType3);
        recordType3.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType1.getId(), true));
        recordType3 = typeManager.updateRecordType(recordType3);
        recordType3.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType3.getId(), true));
        recordType3 = typeManager.updateRecordType(recordType3);
        recordType3.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType6.getId(), false));
        recordType3 = typeManager.updateRecordType(recordType3);


    }


    @Test
    public void testRecordCreateWithoutRecordType() throws Exception {
        IMocksControl control = createControl();
        control.replay();
        Record record = repository.newRecord(idGenerator.newRecordId());
        try {
            if (avro)
                System.out.println("Expecting InvalidRecordException");
            record = repository.create(record);
            fail();
        } catch (InvalidRecordException expected) {
        }
        control.verify();
    }

    @Test
    public void testEmptyRecordCreate() throws Exception {
        Record record = repository.newRecord();
        record.setRecordType(recordType1.getName());
        try {
            if (avro)
                System.out.println("Expecting InvalidRecordException");
            record = repository.create(record);
            fail();
        } catch (InvalidRecordException expected) {
        }
    }

    @Test
    public void testCreate() throws Exception {
        IMocksControl control = createControl();
        control.replay();
        Record createdRecord = createDefaultRecord();

        assertEquals(Long.valueOf(1), createdRecord.getVersion());
        assertEquals("value1", createdRecord.getField(fieldType1.getName()));
        assertEquals(123, createdRecord.getField(fieldType2.getName()));
        assertTrue((Boolean) createdRecord.getField(fieldType3.getName()));
        assertEquals(recordType1.getName(), createdRecord.getRecordTypeName());
        assertEquals(Long.valueOf(1), createdRecord.getRecordTypeVersion());
        assertEquals(recordType1.getName(), createdRecord.getRecordTypeName(Scope.NON_VERSIONED));
        assertEquals(Long.valueOf(1), createdRecord.getRecordTypeVersion(Scope.NON_VERSIONED));
        assertEquals(recordType1.getName(), createdRecord.getRecordTypeName(Scope.VERSIONED));
        assertEquals(Long.valueOf(1), createdRecord.getRecordTypeVersion(Scope.VERSIONED));
        assertEquals(recordType1.getName(), createdRecord.getRecordTypeName(Scope.VERSIONED_MUTABLE));
        assertEquals(Long.valueOf(1), createdRecord.getRecordTypeVersion(Scope.VERSIONED_MUTABLE));

        assertEquals(createdRecord, repository.read(createdRecord.getId()));
        control.verify();
    }

    @Test
    public void testCreateTwice() throws Exception {
        IMocksControl control = createControl();
        control.replay();
        Record createdRecord = createDefaultRecord();

        Record record = repository.newRecord(createdRecord.getId());
        record.setRecordType(recordType1.getName(), recordType1.getVersion());
        record.setField(fieldType1.getName(), "value1");
        record.setField(fieldType2.getName(), 123);
        record.setField(fieldType3.getName(), true);
        try {
            repository.create(record);
            fail();
        } catch (RecordExistsException expected) {
        }

        control.verify();
    }

    @Test
    public void testCreateNoVersions() throws Exception {
        Record record = repository.newRecord();
        record.setRecordType(recordType1.getName(), recordType1.getVersion());
        record.setField(fieldType1.getName(), "value1");

        record = repository.create(record);
        assertEquals(null, record.getVersion());
    }

    @Test
    public void testCreateOnlyVersionAndCheckRecordType() throws Exception {
        Record record = repository.newRecord();
        record.setRecordType(recordType1.getName(), recordType1.getVersion());
        record.setField(fieldType2.getName(), 123);

        record = repository.create(record);

        Record readRecord = repository.read(record.getId());
        // Check that the 'global' record type of the read record is also filled in
        assertEquals(recordType1.getName(), readRecord.getRecordTypeName());
        assertEquals(recordType1.getVersion(), readRecord.getRecordTypeVersion());

        // The record type for the versioned scope (only field present) should be returned
        assertEquals(recordType1.getName(), readRecord.getRecordTypeName(Scope.VERSIONED));
        assertEquals(recordType1.getVersion(), readRecord.getRecordTypeVersion(Scope.VERSIONED));

        // The record type for the version-mutable scope should not be returned since no such field is present
        assertEquals(null, readRecord.getRecordTypeName(Scope.VERSIONED_MUTABLE));
        assertEquals(null, readRecord.getRecordTypeVersion(Scope.VERSIONED_MUTABLE));
    }

    protected Record createDefaultRecord() throws Exception {
        Record record = repository.newRecord();
        record.setRecordType(recordType1.getName(), recordType1.getVersion());
        record.setField(fieldType1.getName(), "value1");
        record.setField(fieldType2.getName(), 123);
        record.setField(fieldType3.getName(), true);
        return repository.create(record);
    }

    @Test
    public void testCreateWithNonExistingRecordTypeFails() throws Exception {
        Record record = repository.newRecord(idGenerator.newRecordId());
        record.setRecordType(new QName("foo", "bar"));
        record.setField(fieldType1.getName(), "value1");
        try {
            if (avro)
                System.out.println("Expecting RecordTypeNotFoundException");
            repository.create(record);
            fail();
        } catch (RecordTypeNotFoundException expected) {
        }
    }

    @Test
    public void testCreateUsesLatestRecordType() throws Exception {
        Record record = repository.newRecord();
        record.setRecordType(recordType1.getName());
        record.setField(fieldType1.getName(), "value1");
        Record createdRecord = repository.create(record);
        assertEquals(recordType1.getName(), createdRecord.getRecordTypeName());
        assertEquals(Long.valueOf(2), createdRecord.getRecordTypeVersion());
        assertEquals(recordType1.getName(), createdRecord.getRecordTypeName(Scope.NON_VERSIONED));
        assertEquals(Long.valueOf(2), createdRecord.getRecordTypeVersion(Scope.NON_VERSIONED));
        assertNull(createdRecord.getRecordTypeName(Scope.VERSIONED));
        assertNull(createdRecord.getRecordTypeVersion(Scope.VERSIONED));
        assertNull(createdRecord.getRecordTypeName(Scope.VERSIONED_MUTABLE));
        assertNull(createdRecord.getRecordTypeVersion(Scope.VERSIONED_MUTABLE));

        assertEquals(createdRecord, repository.read(createdRecord.getId()));
    }

    @Test
    public void testCreateVariant() throws Exception {
        Record record = createDefaultRecord();

        Map<String, String> variantProperties = new HashMap<String, String>();
        variantProperties.put("dimension1", "dimval1");
        Record variant = repository.newRecord(idGenerator.newRecordId(record.getId(), variantProperties));
        variant.setRecordType(recordType1.getName());
        variant.setField(fieldType1.getName(), "value2");
        variant.setField(fieldType2.getName(), 567);
        variant.setField(fieldType3.getName(), false);

        Record createdVariant = repository.create(variant);

        assertEquals(Long.valueOf(1), createdVariant.getVersion());
        assertEquals("value2", createdVariant.getField(fieldType1.getName()));
        assertEquals(567, createdVariant.getField(fieldType2.getName()));
        assertFalse((Boolean) createdVariant.getField(fieldType3.getName()));

        assertEquals(createdVariant, repository.read(variant.getId()));

        Set<RecordId> variants = repository.getVariants(record.getId());
        assertEquals(2, variants.size());
        assertTrue(variants.contains(record.getId()));
        assertTrue(variants.contains(createdVariant.getId()));
    }

    @Test
    public void testUpdate() throws Exception {
        Record record = createDefaultRecord();
        Record updateRecord = record.cloneRecord();
        updateRecord.setField(fieldType1.getName(), "value2");
        updateRecord.setField(fieldType2.getName(), 789);
        updateRecord.setField(fieldType3.getName(), false);

        Record updatedRecord = repository.update(updateRecord);

        assertEquals(Long.valueOf(2), updatedRecord.getVersion());
        assertEquals("value2", updatedRecord.getField(fieldType1.getName()));
        assertEquals(789, updatedRecord.getField(fieldType2.getName()));
        assertEquals(false, updatedRecord.getField(fieldType3.getName()));

        assertEquals(updatedRecord, repository.read(record.getId()));
    }

    @Test
    public void testUpdateWithoutRecordType() throws Exception {
        Record record = createDefaultRecord();
        Record updateRecord = repository.newRecord(record.getId());
        updateRecord.setField(fieldType1.getName(), "value2");
        updateRecord.setField(fieldType2.getName(), 789);
        updateRecord.setField(fieldType3.getName(), false);

        Record updatedRecord = repository.update(updateRecord);

        assertEquals(record.getRecordTypeName(), updatedRecord.getRecordTypeName());
        assertEquals(Long.valueOf(2), updatedRecord.getRecordTypeVersion());

        assertEquals(Long.valueOf(2), updatedRecord.getVersion());
        assertEquals("value2", updatedRecord.getField(fieldType1.getName()));
        assertEquals(789, updatedRecord.getField(fieldType2.getName()));
        assertEquals(false, updatedRecord.getField(fieldType3.getName()));

        assertEquals(updatedRecord, repository.read(record.getId()));
    }

    @Test
    public void testUpdateOnlyOneField() throws Exception {
        Record record = createDefaultRecord();
        Record updateRecord = repository.newRecord(record.getId());
        updateRecord.setRecordType(record.getRecordTypeName(), record.getRecordTypeVersion());
        updateRecord.setField(fieldType1.getName(), "value2");

        Record updatedRecord = repository.update(updateRecord);

        assertEquals(Long.valueOf(1), updatedRecord.getVersion());
        assertEquals("value2", updatedRecord.getField(fieldType1.getName()));
        try {
            if (avro)
                System.out.println("Expecting FieldNotFoundException");
            updatedRecord.getField(fieldType2.getName());
            fail();
        } catch (FieldNotFoundException expected) {
        }
        try {
            if (avro)
                System.out.println("Expecting FieldNotFoundException");
            updatedRecord.getField(fieldType3.getName());
            fail();
        } catch (FieldNotFoundException expected) {
        }

        Record readRecord = repository.read(record.getId());
        assertEquals("value2", readRecord.getField(fieldType1.getName()));
        assertEquals(123, readRecord.getField(fieldType2.getName()));
        assertEquals(true, readRecord.getField(fieldType3.getName()));
    }

    @Test
    public void testEmptyUpdate() throws Exception {
        Record record = createDefaultRecord();
        Record updateRecord = repository.newRecord(record.getId());
        updateRecord.setRecordType(record.getRecordTypeName(), record.getRecordTypeVersion());

        Record updatedRecord = repository.update(updateRecord);

        assertEquals(Long.valueOf(1), updatedRecord.getVersion());
        try {
            if (avro)
                System.out.println("Expecting FieldNotFoundException");
            updatedRecord.getField(fieldType1.getName());
            fail();
        } catch (FieldNotFoundException expected) {
        }
        try {
            if (avro)
                System.out.println("Expecting FieldNotFoundException");
            updatedRecord.getField(fieldType2.getName());
            fail();
        } catch (FieldNotFoundException expected) {
        }
        try {
            if (avro)
                System.out.println("Expecting FieldNotFoundException");
            updatedRecord.getField(fieldType3.getName());
            fail();
        } catch (FieldNotFoundException expected) {
        }

        assertEquals(record, repository.read(record.getId()));
    }

    @Test
    public void testIdempotentUpdate() throws Exception {
        Record record = createDefaultRecord();
        Record updateRecord = record.cloneRecord();

        Record updatedRecord = repository.update(updateRecord);

        assertEquals(Long.valueOf(1), updatedRecord.getVersion());
        assertEquals("value1", updatedRecord.getField(fieldType1.getName()));
        assertEquals(123, updatedRecord.getField(fieldType2.getName()));
        assertEquals(true, updatedRecord.getField(fieldType3.getName()));

        assertEquals(record, repository.read(record.getId()));
    }

    @Test
    public void testUpdateIgnoresVersion() throws Exception {
        Record record = createDefaultRecord();
        Record updateRecord = record.cloneRecord();
        updateRecord.setVersion(Long.valueOf(99));
        updateRecord.setField(fieldType1.getName(), "value2");

        Record updatedRecord = repository.update(updateRecord);

        assertEquals(Long.valueOf(1), updatedRecord.getVersion());

        assertEquals(updatedRecord, repository.read(record.getId()));
    }

    @Test
    public void testUpdateNonVersionable() throws Exception {
        Record record = createDefaultRecord();
        Record updateRecord = repository.newRecord(record.getId());
        updateRecord.setRecordType(record.getRecordTypeName());
        updateRecord.setField(fieldType1.getName(), "aNewValue");
        repository.update(updateRecord);

        Record readRecord = repository.read(record.getId());
        assertEquals(Long.valueOf(1), readRecord.getVersion());
        assertEquals("aNewValue", readRecord.getField(fieldType1.getName()));
    }

    @Test
    public void testReadOlderVersions() throws Exception {
        Record record = createDefaultRecord();
        Record updateRecord = record.cloneRecord();
        updateRecord.setField(fieldType1.getName(), "value2");
        updateRecord.setField(fieldType2.getName(), 789);
        updateRecord.setField(fieldType3.getName(), false);

        // This update will use the latest version of the RecordType
        // I.e. version2 of recordType1 instead of version 1
        repository.update(updateRecord);

        record.setRecordType(recordType1B.getName(), recordType1B.getVersion());
        record.setField(fieldType1.getName(), "value2");
        assertEquals(record, repository.read(record.getId(), 1L));

        try {
            if (avro)
                System.out.println("Expecting RecordNotFoundException");
            repository.read(record.getId(), 0L);
            fail();
        } catch (RecordNotFoundException expected) {
        }
    }

    @Test
    public void testReadAllVersions() throws Exception {
        Record record = createDefaultRecord();
        Record updateRecord = record.cloneRecord();
        updateRecord.setField(fieldType1.getName(), "value2");
        updateRecord.setField(fieldType2.getName(), 789);
        updateRecord.setField(fieldType3.getName(), false);

        repository.update(updateRecord);

        List<Record> list = repository.readVersions(record.getId(), 1L, 2L, (QName[]) null);
        assertEquals(2, list.size());
        assertTrue(list.contains(repository.read(record.getId(), 1L)));
        assertTrue(list.contains(repository.read(record.getId(), 2L)));
    }

    @Test
    public void testReadVersionsWideBoundaries() throws Exception {
        Record record = createDefaultRecord();
        Record updateRecord = record.cloneRecord();
        updateRecord.setField(fieldType1.getName(), "value2");
        updateRecord.setField(fieldType2.getName(), 789);
        updateRecord.setField(fieldType3.getName(), false);

        repository.update(updateRecord);

        List<Record> list = repository.readVersions(record.getId(), 0L, 5L, (QName[]) null);
        assertEquals(2, list.size());
        assertTrue(list.contains(repository.read(record.getId(), 1L)));
        assertTrue(list.contains(repository.read(record.getId(), 2L)));
    }

    @Test
    public void testReadVersionsNarrowBoundaries() throws Exception {
        Record record = createDefaultRecord();

        Record updateRecord = record.cloneRecord();
        updateRecord.setField(fieldType1.getName(), "value2");
        updateRecord.setField(fieldType2.getName(), 789);
        updateRecord.setField(fieldType3.getName(), false);
        repository.update(updateRecord);

        updateRecord = record.cloneRecord();
        updateRecord.setField(fieldType2.getName(), 790);
        repository.update(updateRecord);

        updateRecord = record.cloneRecord();
        updateRecord.setField(fieldType2.getName(), 791);
        repository.update(updateRecord);

        List<Record> list = repository.readVersions(record.getId(), 2L, 3L);
        assertEquals(2, list.size());
        assertTrue(list.contains(repository.read(record.getId(), 2L)));
        assertTrue(list.contains(repository.read(record.getId(), 3L)));
    }

    @Test
    public void testReadSpecificVersions() throws Exception {
        Record record = createDefaultRecord();
        Record updateRecord = record.cloneRecord();
        updateRecord.setField(fieldType1.getName(), "value2");
        updateRecord.setField(fieldType2.getName(), 789);
        // Don't update this field, as a test that the internal version inheritance code works correctly
        // updateRecord.setField(fieldType3.getName(), false);

        repository.update(updateRecord);

        // Now update this field again
        updateRecord.setField(fieldType3.getName(), false);
        repository.update(updateRecord);

        Record record1 = repository.read(record.getId(), 1L);
        Record record2 = repository.read(record.getId(), 2L);
        Record record3 = repository.read(record.getId(), 3L);

        List<Record> records = repository.readVersions(record.getId(), Arrays.asList(1L, 2L, 3L), (QName[]) null);
        assertEquals(3, records.size());
        assertTrue(records.contains(record1));
        assertTrue(records.contains(record2));
        assertTrue(records.contains(record3));

        records = repository.readVersions(record.getId(), new ArrayList<Long>(), (QName[]) null);
        assertEquals(0, records.size());

        records = repository.readVersions(record.getId(), Arrays.asList(1L, 5L), (QName[]) null);
        assertEquals(1, records.size());
        assertTrue(records.contains(record1));
    }

    @Test
    public void testReadNonExistingRecord() throws Exception {
        try {
            if (avro)
                System.out.println("Expecting RecordNotFoundException");
            repository.read(idGenerator.newRecordId());
            fail();
        } catch (RecordNotFoundException expected) {
        }
    }

    @Test
    public void testReadTooRecentRecord() throws Exception {
        Record record = createDefaultRecord();
        try {
            if (avro)
                System.out.println("Expecting VersionNotFoundException");
            repository.read(record.getId(), Long.valueOf(2));
            fail();
        } catch (VersionNotFoundException expected) {
        }
    }

    @Test
    public void testReadSpecificFields() throws Exception {
        Record record = createDefaultRecord();
        Record readRecord =
                repository.read(record.getId(), fieldType1.getName(), fieldType2.getName(), fieldType3.getName());
        assertEquals(repository.read(record.getId()), readRecord);
    }

    @Test
    public void testUpdateWithNewRecordTypeVersion() throws Exception {
        Record record = createDefaultRecord();
        Record updateRecord = repository.newRecord(record.getId());
        updateRecord.setRecordType(recordType1B.getName(), recordType1B.getVersion());
        updateRecord.setField(fieldType1.getName(), "value2");
        updateRecord.setField(fieldType2.getName(), 789);
        updateRecord.setField(fieldType3.getName(), false);

        Record updatedRecord = repository.update(updateRecord);
        assertEquals(recordType1B.getName(), updatedRecord.getRecordTypeName());
        assertEquals(recordType1B.getVersion(), updatedRecord.getRecordTypeVersion());
        assertEquals(recordType1B.getName(), updatedRecord.getRecordTypeName(Scope.NON_VERSIONED));
        assertEquals(recordType1B.getVersion(), updatedRecord.getRecordTypeVersion(Scope.NON_VERSIONED));
        assertEquals(recordType1B.getName(), updatedRecord.getRecordTypeName(Scope.VERSIONED));
        assertEquals(recordType1B.getVersion(), updatedRecord.getRecordTypeVersion(Scope.VERSIONED));
        assertEquals(recordType1B.getName(), updatedRecord.getRecordTypeName(Scope.VERSIONED_MUTABLE));
        assertEquals(recordType1B.getVersion(), updatedRecord.getRecordTypeVersion(Scope.VERSIONED_MUTABLE));

        Record recordV1 = repository.read(record.getId(), Long.valueOf(1));
        assertEquals(recordType1B.getName(), recordV1.getRecordTypeName());
        assertEquals(recordType1B.getVersion(), recordV1.getRecordTypeVersion());
        assertEquals(recordType1B.getName(), recordV1.getRecordTypeName(Scope.NON_VERSIONED));
        assertEquals(recordType1B.getVersion(), recordV1.getRecordTypeVersion(Scope.NON_VERSIONED));
        assertEquals(recordType1.getName(), recordV1.getRecordTypeName(Scope.VERSIONED));
        assertEquals(recordType1.getVersion(), recordV1.getRecordTypeVersion(Scope.VERSIONED));
        assertEquals(recordType1.getName(), recordV1.getRecordTypeName(Scope.VERSIONED_MUTABLE));
        assertEquals(recordType1.getVersion(), recordV1.getRecordTypeVersion(Scope.VERSIONED_MUTABLE));
    }

    @Test
    public void testUpdateWithNewRecordTypeVersionOnlyOneFieldUpdated() throws Exception {
        Record record = createDefaultRecord();
        Record updateRecord = repository.newRecord(record.getId());
        updateRecord.setRecordType(recordType1B.getName(), recordType1B.getVersion());
        updateRecord.setField(fieldType2.getName(), 789);

        Record updatedRecord = repository.update(updateRecord);
        assertEquals(recordType1B.getName(), updatedRecord.getRecordTypeName());
        assertEquals(recordType1B.getVersion(), updatedRecord.getRecordTypeVersion());
        assertEquals(recordType1B.getName(), updatedRecord.getRecordTypeName(Scope.VERSIONED));
        assertEquals(recordType1B.getVersion(), updatedRecord.getRecordTypeVersion(Scope.VERSIONED));

        Record readRecord = repository.read(record.getId());
        assertEquals(recordType1B.getName(), updatedRecord.getRecordTypeName());
        assertEquals(recordType1B.getVersion(), updatedRecord.getRecordTypeVersion());
        assertEquals(recordType1B.getName(), readRecord.getRecordTypeName(Scope.NON_VERSIONED));
        assertEquals(recordType1B.getVersion(), readRecord.getRecordTypeVersion(Scope.NON_VERSIONED));
        assertEquals(recordType1B.getName(), updatedRecord.getRecordTypeName(Scope.VERSIONED));
        assertEquals(recordType1B.getVersion(), updatedRecord.getRecordTypeVersion(Scope.VERSIONED));
        assertEquals(recordType1.getName(), readRecord.getRecordTypeName(Scope.VERSIONED_MUTABLE));
        assertEquals(recordType1.getVersion(), readRecord.getRecordTypeVersion(Scope.VERSIONED_MUTABLE));
    }

    @Test
    public void testUpdateWithNewRecordType() throws Exception {
        Record record = createDefaultRecord();
        Record updateRecord = repository.newRecord(record.getId());
        updateRecord.setRecordType(recordType2.getName(), recordType2.getVersion());
        updateRecord.setField(fieldType4.getName(), 1024);
        updateRecord.setField(fieldType5.getName(), false);
        updateRecord.setField(fieldType6.getName(), "value2");

        Record updatedRecord = repository.update(updateRecord);
        assertEquals(recordType2.getName(), updatedRecord.getRecordTypeName());
        assertEquals(recordType2.getVersion(), updatedRecord.getRecordTypeVersion());
        assertEquals(recordType2.getName(), updatedRecord.getRecordTypeName(Scope.NON_VERSIONED));
        assertEquals(recordType2.getVersion(), updatedRecord.getRecordTypeVersion(Scope.NON_VERSIONED));
        assertEquals(recordType2.getName(), updatedRecord.getRecordTypeName(Scope.VERSIONED));
        assertEquals(recordType2.getVersion(), updatedRecord.getRecordTypeVersion(Scope.VERSIONED));
        assertEquals(recordType2.getName(), updatedRecord.getRecordTypeName(Scope.VERSIONED_MUTABLE));
        assertEquals(recordType2.getVersion(), updatedRecord.getRecordTypeVersion(Scope.VERSIONED_MUTABLE));

        assertEquals(3, updatedRecord.getFields().size());

        Record readRecord = repository.read(record.getId());
        // Nothing got deleted
        assertEquals(6, readRecord.getFields().size());
        assertEquals("value1", readRecord.getField(fieldType1.getName()));
        assertEquals(1024, readRecord.getField(fieldType4.getName()));
        assertEquals(123, readRecord.getField(fieldType2.getName()));
        assertFalse((Boolean) readRecord.getField(fieldType5.getName()));
        assertTrue((Boolean) readRecord.getField(fieldType3.getName()));
        assertEquals("value2", readRecord.getField(fieldType6.getName()));
    }

    @Test
    public void testDeleteField() throws Exception {
        Record record = createDefaultRecord();
        Record deleteRecord = repository.newRecord(record.getId());
        deleteRecord.setRecordType(record.getRecordTypeName());
        deleteRecord.addFieldsToDelete(Arrays.asList(fieldType1.getName(), fieldType2.getName(), fieldType3.getName()));

        repository.update(deleteRecord);
        Record readRecord = repository.read(record.getId());
        assertEquals(0, readRecord.getFields().size());
    }

    @Test
    public void testDeleteFieldFollowedBySet() throws Exception {
        Record record = repository.newRecord();
        record.setRecordType(recordType1.getName(), recordType1.getVersion());
        record.setField(fieldType1.getName(), "hello");
        record = repository.create(record);

        // Delete the field
        record.delete(fieldType1.getName(), true);
        record = repository.update(record);
        assertFalse(record.getFieldsToDelete().contains(fieldType1.getName()));

        // Set the field again
        record.setField(fieldType1.getName(), "hello");
        record = repository.update(record);
        assertEquals("hello", record.getField(fieldType1.getName()));

        // Check it also there after a fresh read
        record = repository.read(record.getId());
        assertEquals("hello", record.getField(fieldType1.getName()));

        // Calling delete field followed by set field should remove it from the deleted fields
        record.delete(fieldType1.getName(), true);
        assertTrue(record.getFieldsToDelete().contains(fieldType1.getName()));
        record.setField(fieldType1.getName(), "hello");
        assertFalse(record.getFieldsToDelete().contains(fieldType1.getName()));
    }

    @Test
    public void testDeleteFieldsNoLongerInRecordType() throws Exception {
        Record record = createDefaultRecord();
        Record updateRecord = repository.newRecord(record.getId());
        updateRecord.setRecordType(recordType2.getName(), recordType2.getVersion());
        updateRecord.setField(fieldType4.getName(), 2222);
        updateRecord.setField(fieldType5.getName(), false);
        updateRecord.setField(fieldType6.getName(), "value2");

        repository.update(updateRecord);

        Record deleteRecord = repository.newRecord(record.getId());
        deleteRecord.setRecordType(recordType1.getName(), recordType1.getVersion());
        deleteRecord.addFieldsToDelete(Arrays.asList(fieldType1.getName()));
        repository.update(deleteRecord);

        Record readRecord = repository.read(record.getId());
        assertEquals(Long.valueOf(2), readRecord.getVersion());
        assertEquals(5, readRecord.getFields().size());
        try {
            if (avro)
                System.out.println("Expecting FieldNotFoundException");
            readRecord.getField(fieldType1.getName());
            fail();
        } catch (FieldNotFoundException expected) {
        }
        assertEquals("value2", readRecord.getField(fieldType6.getName()));
        assertEquals(2222, readRecord.getField(fieldType4.getName()));

        deleteRecord.addFieldsToDelete(Arrays.asList(fieldType2.getName(), fieldType3.getName()));
        repository.update(deleteRecord);

        readRecord = repository.read(record.getId());
        assertEquals(Long.valueOf(3), readRecord.getVersion());
        assertEquals(3, readRecord.getFields().size());
        assertEquals(2222, readRecord.getField(fieldType4.getName()));
        assertEquals(false, readRecord.getField(fieldType5.getName()));
        assertEquals("value2", readRecord.getField(fieldType6.getName()));
    }

    @Test
    public void testDeleteFieldTwice() throws Exception {
        Record record = createDefaultRecord();
        Record updateRecord = repository.newRecord(record.getId());
        updateRecord.setRecordType(recordType2.getName(), recordType2.getVersion());
        updateRecord.setField(fieldType4.getName(), 2222);
        updateRecord.setField(fieldType5.getName(), false);
        updateRecord.setField(fieldType6.getName(), "value2");

        repository.update(updateRecord);

        Record deleteRecord = repository.newRecord(record.getId());
        deleteRecord.setRecordType(recordType1.getName(), recordType1.getVersion());
        deleteRecord.addFieldsToDelete(Arrays.asList(fieldType1.getName()));
        repository.update(deleteRecord);
        repository.update(deleteRecord);
    }

    @Test
    public void testUpdateAfterDelete() throws Exception {
        Record record = createDefaultRecord();
        Record deleteRecord = repository.newRecord(record.getId());
        deleteRecord.setRecordType(record.getRecordTypeName(), record.getRecordTypeVersion());
        deleteRecord.addFieldsToDelete(Arrays.asList(fieldType2.getName()));
        repository.update(deleteRecord);

        Record updateRecord = repository.newRecord(record.getId());
        updateRecord.setRecordType(record.getRecordTypeName(), record.getRecordTypeVersion());
        updateRecord.setField(fieldType2.getName(), 3333);
        repository.update(updateRecord);

        // Read version 3
        Record readRecord = repository.read(record.getId());
        assertEquals(Long.valueOf(3), readRecord.getVersion());
        assertEquals(3333, readRecord.getField(fieldType2.getName()));

        // Read version 2
        readRecord = repository.read(record.getId(), Long.valueOf(2));
        try {
            if (avro)
                System.out.println("Expecting FieldNotFoundException");
            readRecord.getField(fieldType2.getName());
            fail();
        } catch (FieldNotFoundException expected) {
        }

        // Read version 1
        readRecord = repository.read(record.getId(), Long.valueOf(1));
        assertEquals(123, readRecord.getField(fieldType2.getName()));
    }

    @Test
    public void testDeleteNonVersionableFieldAndUpdateVersionableField() throws Exception {
        Record record = createDefaultRecord();
        Record updateRecord = repository.newRecord(record.getId());
        updateRecord.setRecordType(record.getRecordTypeName(), record.getRecordTypeVersion());
        updateRecord.setField(fieldType2.getName(), 999);
        updateRecord.addFieldsToDelete(Arrays.asList(fieldType1.getName()));
        repository.update(updateRecord);

        Record readRecord = repository.read(record.getId());
        assertEquals(999, readRecord.getField(fieldType2.getName()));
        try {
            if (avro)
                System.out.println("Expecting FieldNotFoundException");
            readRecord.getField(fieldType1.getName());
            fail();
        } catch (FieldNotFoundException expected) {
        }

        readRecord = repository.read(record.getId(), Long.valueOf(1));
        try {
            if (avro)
                System.out.println("Expecting FieldNotFoundException");
            readRecord.getField(fieldType1.getName());
            fail();
        } catch (FieldNotFoundException expected) {
        }

    }

    @Test
    public void testUpdateAndDeleteSameField() throws Exception {
        Record record = createDefaultRecord();
        Record updateRecord = repository.newRecord(record.getId());
        updateRecord.setRecordType(record.getRecordTypeName(), record.getRecordTypeVersion());
        updateRecord.setField(fieldType2.getName(), 789);
        updateRecord.addFieldsToDelete(Arrays.asList(fieldType2.getName()));
        repository.update(updateRecord);

        try {
            if (avro)
                System.out.println("Expecting FieldNotFoundException");
            repository.read(record.getId()).getField(fieldType2.getName());
            fail();
        } catch (FieldNotFoundException expected) {
        }
    }

    @Test
    public void testDeleteRecordById() throws Exception {
        Record record = createDefaultRecord();
        repository.delete(record.getId());
        try {
            if (avro)
                System.out.println("Expecting RecordNotFoundException");
            repository.read(record.getId());
            fail();
        } catch (RecordNotFoundException expected) {
        }
        try {
            if (avro)
                System.out.println("Expecting RecordNotFoundException");
            repository.update(record);
            fail();
        } catch (RecordNotFoundException expected) {
        }
        try {
            if (avro)
                System.out.println("Expecting RecordNotFoundException");
            repository.delete(record.getId());
            fail();
        } catch (RecordNotFoundException expected) {
        }
    }
    

    @Test
    public void testDeleteRecord() throws Exception {
        Record record = createDefaultRecord();
        repository.delete(record);
        try {
            if (avro)
                System.out.println("Expecting RecordNotFoundException");
            repository.read(record.getId());
            fail();
        } catch (RecordNotFoundException expected) {
        }
        try {
            if (avro)
                System.out.println("Expecting RecordNotFoundException");
            repository.update(record);
            fail();
        } catch (RecordNotFoundException expected) {
        }
        try {
            if (avro)
                System.out.println("Expecting RecordNotFoundException");
            repository.delete(record.getId());
            fail();
        } catch (RecordNotFoundException expected) {
        }
    }

    @Test
    public void testDeleteRecordCleansUpData() throws Exception {
        Record record = createDefaultRecord();
        RecordId recordId = record.getId();
        repository.delete(recordId);

        record = repository.newRecord(recordId);
        record.setRecordType(recordType2.getName(), recordType2.getVersion());
        record.setField(fieldType4.getName(), 555);
        record.setField(fieldType5.getName(), false);
        record.setField(fieldType6.getName(), "zzz");
        repository.create(record);
        Record readRecord = repository.read(recordId);
        assertEquals(Long.valueOf(2), readRecord.getVersion());
        try {
            readRecord.getField(fieldType1.getName());
            fail();
        } catch (FieldNotFoundException expected) {
        }
        try {
            readRecord.getField(fieldType2.getName());
            fail();
        } catch (FieldNotFoundException expected) {
        }
        try {
            readRecord.getField(fieldType3.getName());
            fail();
        } catch (FieldNotFoundException expected) {
        }

        assertEquals(555, readRecord.getField(fieldType4.getName()));
        assertFalse((Boolean) readRecord.getField(fieldType5.getName()));
        assertEquals("zzz", readRecord.getField(fieldType6.getName()));
    }

    @Test
    public void testRecordRecreateFromVersionedToNonVersioned() throws Exception {
        QName vfield = new QName("recreate", "vfield");
        QName nvfield = new QName("recreate", "nvfield");

        FieldType vfieldtype = typeManager.newFieldType(typeManager.getValueType("STRING"), vfield, Scope.VERSIONED);
        vfieldtype = typeManager.createFieldType(vfieldtype);

        FieldType nvfieldtype =
                typeManager.newFieldType(typeManager.getValueType("STRING"), nvfield, Scope.NON_VERSIONED);
        nvfieldtype = typeManager.createFieldType(nvfieldtype);

        RecordType rt = typeManager.newRecordType(new QName("reinc", "rt"));
        rt.addFieldTypeEntry(vfieldtype.getId(), false);
        rt.addFieldTypeEntry(nvfieldtype.getId(), false);
        rt = typeManager.createRecordType(rt);

        // Create a record with versions
        RecordId recordId = repository.getIdGenerator().newRecordId();
        Record record = repository.newRecord(recordId);
        record.setRecordType(rt.getName());

        record.setField(vfield, "value 1");
        record = repository.createOrUpdate(record);

        record.setField(vfield, "value 2");
        record = repository.createOrUpdate(record);

        assertEquals(2L, record.getVersion().longValue());

        // Delete the record
        repository.delete(recordId);

        // Re-create the record, this time without versions
        record = repository.newRecord(recordId);
        record.setRecordType(rt.getName());
        record.setField(nvfield, "nv value 1");
        record = repository.createOrUpdate(record);

        assertEquals(null, record.getVersion());

        assertEquals(rt.getName(), record.getRecordTypeName());

        // Now add a version again, reusing last value from previously deleted record
        record.setField(vfield, "value 2");
        record = repository.createOrUpdate(record);

        assertEquals(3L, record.getVersion().longValue());
    }

    @Test
    public void testRecordRecreateOnlyVersionedFields() throws Exception {
        QName versionedOnlyQN = new QName(namespace, "VersionedOnly");
        RecordType versionedOnlyRT = typeManager.newRecordType(versionedOnlyQN);
        versionedOnlyRT.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType2.getId(), false));
        versionedOnlyRT = typeManager.createRecordType(versionedOnlyRT);

        Record record = repository.newRecord();
        record.setRecordType(versionedOnlyQN);
        record.setField(fieldType2.getName(), 111);
        record = repository.create(record);
        RecordId id = record.getId();
        repository.delete(id);

        record = repository.newRecord(id);
        record.setRecordType(versionedOnlyQN);
        record.setField(fieldType2.getName(), 222);
        record = repository.create(record);

        assertEquals(versionedOnlyQN, record.getRecordTypeName());

        record = repository.read(id);
        assertEquals(versionedOnlyQN, record.getRecordTypeName());
        assertEquals(versionedOnlyQN, record.getRecordTypeName(Scope.VERSIONED));
    }

    @Test
    public void testRecordRecreateNonVersionedOnly() throws Exception {
        QName nvfield = new QName("recreate", "OnlyNonVersioned");

        FieldType nvfieldtype =
                typeManager.newFieldType(typeManager.getValueType("STRING"), nvfield, Scope.NON_VERSIONED);
        nvfieldtype = typeManager.createFieldType(nvfieldtype);

        QName rtName = new QName("recreate", "rtOnlyNonVersioned");
        RecordType rt = typeManager.newRecordType(rtName);
        rt.addFieldTypeEntry(nvfieldtype.getId(), false);
        rt = typeManager.createRecordType(rt);

        // Create a record with versions
        RecordId recordId = repository.getIdGenerator().newRecordId();
        Record record = repository.newRecord(recordId);
        record.setRecordType(rt.getName());

        record.setField(nvfield, "nv value 1");
        record = repository.createOrUpdate(record);
        record = repository.read(record.getId());

        assertEquals("nv value 1", record.getField(nvfield));
        assertEquals(rtName, record.getRecordTypeName());

        assertEquals(null, record.getVersion());

        // Delete the record
        repository.delete(recordId);

        // Re-create the record,
        record = repository.newRecord(recordId);
        record.setRecordType(rt.getName());
        record.setField(nvfield, "nv value 2");
        record = repository.createOrUpdate(record);

        assertEquals(rtName, record.getRecordTypeName());
        assertEquals(null, record.getVersion());

        record = repository.read(record.getId());
        assertEquals("nv value 2", record.getField(nvfield));
        assertEquals(rtName, record.getRecordTypeName());
    }

    @Test
    public void testUpdateMutableField() throws Exception {
        Record record = repository.newRecord();
        record.setRecordType(recordType2.getName(), recordType2.getVersion());
        record.setField(fieldType4.getName(), 123);
        record.setField(fieldType5.getName(), true);
        record.setField(fieldType6.getName(), "value1");
        record = repository.create(record);

        Record updateRecord = record.cloneRecord();
        updateRecord.setField(fieldType4.getName(), 456);
        updateRecord.setField(fieldType5.getName(), false);
        updateRecord.setField(fieldType6.getName(), "value2");
        repository.update(updateRecord);

        // Read version 1
        Record readRecord = repository.read(record.getId(), Long.valueOf(1));
        assertEquals(456, readRecord.getField(fieldType4.getName()));
        assertEquals(true, readRecord.getField(fieldType5.getName()));
        assertEquals("value1", readRecord.getField(fieldType6.getName()));

        // Update mutable version 1
        Record mutableRecord = repository.newRecord(record.getId());
        mutableRecord.setRecordType(recordType2.getName(), recordType2.getVersion());
        mutableRecord.setField(fieldType6.getName(), "value3");
        mutableRecord.setVersion(1L);
        mutableRecord = repository.update(mutableRecord, true, false);

        // Read version 1 again
        readRecord = repository.read(record.getId(), 1L);
        assertEquals(456, readRecord.getField(fieldType4.getName()));
        assertEquals(true, readRecord.getField(fieldType5.getName()));
        assertEquals("value3", readRecord.getField(fieldType6.getName()));

        // Update mutable version 2
        mutableRecord.setVersion(2L);
        mutableRecord.setField(fieldType6.getName(), "value4");
        mutableRecord = repository.update(mutableRecord, true, false);

        // Read version 2
        readRecord = repository.read(record.getId(), 2L);
        assertEquals(456, readRecord.getField(fieldType4.getName()));
        assertEquals(false, readRecord.getField(fieldType5.getName()));
        assertEquals("value4", readRecord.getField(fieldType6.getName()));
    }

    @Test
    public void testUpdateMutableFieldWithNewRecordType() throws Exception {
        // Create default record
        Record record = createDefaultRecord();

        // Update the record, creates a second version
        Record updateRecord = record.cloneRecord();
        updateRecord.setField(fieldType1.getName(), "value2");
        updateRecord.setField(fieldType2.getName(), 789);
        updateRecord.setField(fieldType3.getName(), false);
        repository.update(updateRecord, false, false);

        // Read the first version of the record
        Record readRecord = repository.read(record.getId(), Long.valueOf(1));
        assertEquals(recordType1.getName(), readRecord.getRecordTypeName());
        assertEquals(recordType1.getVersion(), readRecord.getRecordTypeVersion());
        assertEquals(recordType1.getName(), readRecord.getRecordTypeName(Scope.NON_VERSIONED));
        assertEquals(recordType1.getVersion(), readRecord.getRecordTypeVersion(Scope.NON_VERSIONED));
        assertEquals(recordType1.getName(), readRecord.getRecordTypeName(Scope.VERSIONED));
        assertEquals(recordType1.getVersion(), readRecord.getRecordTypeVersion(Scope.VERSIONED));
        assertEquals(recordType1.getName(), readRecord.getRecordTypeName(Scope.VERSIONED_MUTABLE));
        assertEquals(recordType1.getVersion(), readRecord.getRecordTypeVersion(Scope.VERSIONED_MUTABLE));

        // Do a mutable update of the first version of the record, change the record type
        Record updateMutableRecord = repository.newRecord(record.getId());
        updateMutableRecord.setVersion(Long.valueOf(1));
        updateMutableRecord.setRecordType(recordType2.getName(), recordType2.getVersion());
        updateMutableRecord.setField(fieldType4.getName(), 888);
        updateMutableRecord.setField(fieldType5.getName(), false);
        updateMutableRecord.setField(fieldType6.getName(), "value3");

        Record updatedMutableRecord = repository.update(updateMutableRecord, true, false);
        assertEquals(Long.valueOf(1), updatedMutableRecord.getVersion());

        // Read the first version of the record again
        readRecord = repository.read(record.getId(), Long.valueOf(1));
        assertEquals(Long.valueOf(1), readRecord.getVersion());
        assertEquals("value2", readRecord.getField(fieldType1.getName()));
        assertEquals(123, readRecord.getField(fieldType2.getName()));
        assertEquals(true, readRecord.getField(fieldType3.getName()));
        // Only the mutable fields got updated
        assertEquals("value3", readRecord.getField(fieldType6.getName()));
        try {
            if (avro)
                System.out.println("Expecting FieldNotFoundException");
            readRecord.getField(fieldType4.getName());
            fail();
        } catch (FieldNotFoundException expected) {
        }
        try {
            if (avro)
                System.out.println("Expecting FieldNotFoundException");
            readRecord.getField(fieldType5.getName());
            fail();
        } catch (FieldNotFoundException expected) {
        }
        assertEquals(recordType1.getName(), readRecord.getRecordTypeName());
        assertEquals(recordType1.getVersion(), readRecord.getRecordTypeVersion());
        assertEquals(recordType1.getName(), readRecord.getRecordTypeName(Scope.NON_VERSIONED));
        assertEquals(recordType1.getVersion(), readRecord.getRecordTypeVersion(Scope.NON_VERSIONED));
        assertEquals(recordType1.getName(), readRecord.getRecordTypeName(Scope.VERSIONED));
        assertEquals(recordType1.getVersion(), readRecord.getRecordTypeVersion(Scope.VERSIONED));
        // The mutable record type should have been changed
        assertEquals(recordType2.getName(), readRecord.getRecordTypeName(Scope.VERSIONED_MUTABLE));
        assertEquals(recordType2.getVersion(), readRecord.getRecordTypeVersion(Scope.VERSIONED_MUTABLE));

        // Read the second version again of the record
        readRecord = repository.read(record.getId());
        assertEquals(Long.valueOf(2), readRecord.getVersion());
        assertEquals("value2", readRecord.getField(fieldType1.getName()));
        assertEquals(789, readRecord.getField(fieldType2.getName()));
        assertEquals(false, readRecord.getField(fieldType3.getName()));
        assertEquals("value3", readRecord.getField(fieldType6.getName()));
        assertEquals(recordType1.getName(), readRecord.getRecordTypeName());
        assertEquals(recordType1.getName(), readRecord.getRecordTypeName(Scope.NON_VERSIONED));
        assertEquals(recordType1.getName(), readRecord.getRecordTypeName(Scope.VERSIONED));
        // The original mutable record type should have been copied to the next version
        assertEquals(recordType1.getName(), readRecord.getRecordTypeName(Scope.VERSIONED_MUTABLE));
    }

    @Test
    public void testUpdateMutableFieldCopiesValueToNext() throws Exception {
        Record record = createDefaultRecord();
        Record updateRecord = record.cloneRecord();
        updateRecord.setField(fieldType1.getName(), "value2");
        updateRecord.setField(fieldType2.getName(), 789);
        updateRecord = repository.update(updateRecord); // Leave mutable field
        // same on first update

        updateRecord.setField(fieldType3.getName(), false);
        updateRecord = repository.update(updateRecord);

        Record readRecord = repository.read(record.getId(), Long.valueOf(2));
        assertEquals(true, readRecord.getField(fieldType3.getName()));

        updateRecord = repository.newRecord(record.getId());
        updateRecord.setRecordType(recordType1.getName(), recordType1.getVersion());
        updateRecord.setField(fieldType3.getName(), false);
        updateRecord.setVersion(1L);

        repository.update(updateRecord, true, false);

        readRecord = repository.read(record.getId(), Long.valueOf(1));
        assertFalse((Boolean) readRecord.getField(fieldType3.getName()));
        readRecord = repository.read(record.getId(), Long.valueOf(2));
        assertTrue((Boolean) readRecord.getField(fieldType3.getName()));
        readRecord = repository.read(record.getId(), Long.valueOf(3));
        assertFalse((Boolean) readRecord.getField(fieldType3.getName()));
    }

    @Test
    public void testDeleteMutableField() throws Exception {
        Record record = createDefaultRecord();
        Record updateRecord = record.cloneRecord();
        updateRecord.setField(fieldType1.getName(), "value2");
        updateRecord.setField(fieldType2.getName(), 789);
        updateRecord.setField(fieldType3.getName(), false);
        repository.update(updateRecord);

        Record deleteRecord = repository.newRecord(record.getId());
        deleteRecord.setVersion(Long.valueOf(1));
        deleteRecord.setRecordType(record.getRecordTypeName(), record.getRecordTypeVersion());
        deleteRecord.addFieldsToDelete(Arrays.asList(fieldType1.getName(), fieldType2.getName(), fieldType3.getName()));

        repository.update(deleteRecord, true, false);

        Record readRecord = repository.read(record.getId(), Long.valueOf(1));
        // The non-mutable fields were ignored
        assertEquals("value2", readRecord.getField(fieldType1.getName()));
        assertEquals(123, readRecord.getField(fieldType2.getName()));
        try {
            // The mutable field got deleted
            if (avro)
                System.out.println("Expecting FieldNotFoundException");
            readRecord.getField(fieldType3.getName());
            fail();
        } catch (FieldNotFoundException expected) {
        }

        readRecord = repository.read(record.getId());
        assertEquals(false, readRecord.getField(fieldType3.getName()));
    }

    @Test
    public void testDeleteMutableFieldCopiesValueToNext() throws Exception {
        Record record = createDefaultRecord();
        Record updateRecord = record.cloneRecord();
        updateRecord.setField(fieldType1.getName(), "value2");
        updateRecord.setField(fieldType2.getName(), 789);
        updateRecord = repository.update(updateRecord); // Leave mutable field
        // same on first update

        updateRecord.setField(fieldType3.getName(), false);
        updateRecord = repository.update(updateRecord);

        Record readRecord = repository.read(record.getId(), Long.valueOf(2));
        assertEquals(true, readRecord.getField(fieldType3.getName()));

        Record deleteMutableFieldRecord = repository.newRecord(record.getId());
        deleteMutableFieldRecord.setVersion(Long.valueOf(1));
        deleteMutableFieldRecord.setRecordType(record.getRecordTypeName(), record.getRecordTypeVersion());
        deleteMutableFieldRecord.addFieldsToDelete(Arrays.asList(fieldType3.getName()));

        repository.update(deleteMutableFieldRecord, true, false);

        readRecord = repository.read(record.getId(), Long.valueOf(1));
        try {
            if (avro)
                System.out.println("Expecting FieldNotFoundException");
            readRecord.getField(fieldType3.getName());
            fail();
        } catch (FieldNotFoundException expected) {
        }

        readRecord = repository.read(record.getId(), Long.valueOf(2));
        assertEquals(true, readRecord.getField(fieldType3.getName()));

        readRecord = repository.read(record.getId());
        assertEquals(false, readRecord.getField(fieldType3.getName()));
    }

    @Test
    public void testSupertypeLatestVersion() throws Exception {
        RecordType recordType4 = typeManager.newRecordType(new QName(namespace, "RT4"));
        recordType4.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType6.getId(), false));
        recordType4.addSupertype(recordType1.getId()); // In fact recordType1B should be taken as supertype
        recordType4 = typeManager.createRecordType(recordType4);

        Record record = repository.newRecord(idGenerator.newRecordId());
        record.setRecordType(recordType4.getName(), recordType4.getVersion());
        record.setField(fieldType1.getName(), "foo");
        record.setField(fieldType2.getName(), 555);
        record.setField(fieldType3.getName(), true);
        record.setField(fieldType1B.getName(), "fromLatestSupertypeRecordTypeVersion");
        record.setField(fieldType6.getName(), "bar");
        record = repository.create(record);

        Record readRecord = repository.read(record.getId());
        assertEquals(Long.valueOf(1), readRecord.getVersion());
        assertEquals("foo", readRecord.getField(fieldType1.getName()));
        assertEquals(555, readRecord.getField(fieldType2.getName()));
        assertEquals(true, readRecord.getField(fieldType3.getName()));
        assertEquals("fromLatestSupertypeRecordTypeVersion", readRecord.getField(fieldType1B.getName()));
        assertEquals("bar", readRecord.getField(fieldType6.getName()));
    }

    @Test
    public void testNonVersionedToVersioned() throws Exception {
        // Create a record with only a versioned and non-versioned field
        Record record = repository.newRecord();
        record.setRecordType(recordType1.getName(), recordType1.getVersion());
        record.setField(fieldType1.getName(), "hello");
        record.setField(fieldType2.getName(), new Integer(4));
        record = repository.create(record);

        // Try to read the created version
        record = repository.read(record.getId(), 1L);
    }

    @Test
    public void testIdRecord() throws Exception {
        Record record = repository.newRecord();
        record.setRecordType(recordType1.getName(), recordType1.getVersion());
        record.setField(fieldType1.getName(), "hello");
        record.setField(fieldType2.getName(), new Integer(4));
        record = repository.create(record);

        IdRecord idRecord = repository.readWithIds(record.getId(), null, null);
        assertEquals("hello", idRecord.getField(fieldType1.getId()));
        assertTrue(idRecord.hasField(fieldType1.getId()));
        assertEquals(new Integer(4), idRecord.getField(fieldType2.getId()));
        assertTrue(idRecord.hasField(fieldType2.getId()));

        Map<SchemaId, Object> fields = idRecord.getFieldsById();
        assertEquals(2, fields.size());
        assertEquals("hello", fields.get(fieldType1.getId()));
        assertEquals(new Integer(4), fields.get(fieldType2.getId()));

        assertEquals(record, idRecord.getRecord());
    }

    @Test
    public void testVersionNumbers() throws Exception {
        // Create a record without versioned fields, the record will be without versions
        Record record = repository.newRecord();
        record.setRecordType(recordType1.getName(), recordType1.getVersion());
        record.setField(fieldType1.getName(), "hello");
        record = repository.create(record);

        // Check the version is null
        assertEquals(null, record.getVersion());

        // Check version number stays null after additional update
        record.setField(fieldType1.getName(), "hello2");
        repository.update(record);
        record = repository.read(record.getId());
        assertEquals(null, record.getVersion());

        // add a versioned field to the record
        record.setField(fieldType2.getName(), new Integer(4));
        record = repository.update(record);
        assertEquals(new Long(1), record.getVersion());

        // Verify the last version number after a fresh record read
        record = repository.read(record.getId());
        assertEquals(new Long(1), record.getVersion());

        // Read specific version
        record = repository.read(record.getId(), 1L);
        assertEquals(new Long(1), record.getVersion());
        assertTrue(record.hasField(fieldType2.getName()));
        assertEquals(2, record.getFields().size());

        try {
            if (avro)
                System.out.println("Expecting VersionNotFoundException");
            record = repository.read(record.getId(), 2L);
            fail("expected exception");
        } catch (VersionNotFoundException e) {
            // expected
        }
    }

    @Test
    public void testValidateCreate() throws Exception {
        Record record = repository.newRecord();
        record.setRecordType(recordType3.getName(), 1L);
        record.setField(fieldType2.getName(), 123);
        repository.create(record);

        record = repository.newRecord();
        record.setRecordType(recordType3.getName(), 2L);
        record.setField(fieldType2.getName(), 123);
        try {
            if (avro)
                System.out.println("Expecting InvalidRecordException");
            repository.create(record);
            fail();
        } catch (InvalidRecordException expected) {
        }

        record = repository.newRecord();
        record.setRecordType(recordType3.getName(), 2L);
        record.setField(fieldType1.getName(), "abc");
        record.setField(fieldType2.getName(), 123);
    }

    @Test
    public void testValidateUpdate() throws Exception {
        Record record = repository.newRecord();
        record.setRecordType(recordType3.getName(), 1L);
        record.setField(fieldType2.getName(), 123);
        record = repository.create(record);

        record.setRecordType(recordType3.getName(), 2L);
        record.setField(fieldType2.getName(), 567);
        try {
            if (avro)
                System.out.println("Expecting InvalidRecordException");
            repository.update(record, false, false);
            fail();
        } catch (InvalidRecordException expected) {
        }

        record.setField(fieldType1.getName(), "abc");
        repository.update(record, false, false);
    }

    @Test
    public void testValidateMutableUpdate() throws Exception {
        // Nothing mandatory
        Record record = repository.newRecord();
        record.setRecordType(recordType3.getName(), 1L);
        record.setField(fieldType2.getName(), 123);
        record = repository.create(record);

        // Non-versioned field1 mandatory
        record = repository.newRecord(record.getId());
        record.setRecordType(recordType3.getName(), 2L);
        record.setField(fieldType1.getName(), "abc");
        repository.update(record, false, false); // record version 1

        // Mutable field3 mandatory
        record.setRecordType(recordType3.getName(), 3L);
        record.setField(fieldType1.getName(), "efg");
        try {
            if (avro)
                System.out.println("Expecting InvalidRecordException");
            repository.update(record, false, false);
            fail();
        } catch (InvalidRecordException expected) {
        }

        // Mutable field3 not mandatory
        record = repository.newRecord(record.getId());
        record.setRecordType(recordType3.getName(), 2L);
        record.setField(fieldType3.getName(), true);
        repository.update(record, false, false); // record version 2

        // Mutable field update of record version 1 with field3 mandatory
        // Field3 already exists, but in record version 2 not version 1 
        record = repository.newRecord(record.getId());
        record.setRecordType(recordType3.getName(), 4L);
        record.setField(fieldType6.getName(), "zzz");
        record.setVersion(1L);
        try {
            if (avro)
                System.out.println("Expecting InvalidRecordException");
            repository.update(record, true, false);
            fail();
        } catch (InvalidRecordException expected) {
        }
        record.setField(fieldType3.getName(), false);
        repository.update(record, true, false);
    }

    @Test
    public void testCreateOrUpdate() throws Exception {
        RecordId id = idGenerator.newRecordId();
        Record record = repository.newRecord(id);
        record.setRecordType(recordType1.getName(), recordType1.getVersion());
        record.setField(fieldType1.getName(), "value1");

        Record resultRecord;
        resultRecord = repository.createOrUpdate(record);
        assertEquals(ResponseStatus.CREATED, resultRecord.getResponseStatus());
        resultRecord = repository.createOrUpdate(record);
        assertEquals(ResponseStatus.UP_TO_DATE, resultRecord.getResponseStatus());

        record.setField(fieldType1.getName(), "value2");
        resultRecord = repository.createOrUpdate(record);
        assertEquals(ResponseStatus.UPDATED, resultRecord.getResponseStatus());
        resultRecord = repository.createOrUpdate(record);
        assertEquals(ResponseStatus.UP_TO_DATE, resultRecord.getResponseStatus());
        resultRecord = repository.createOrUpdate(record);
        assertEquals(ResponseStatus.UP_TO_DATE, resultRecord.getResponseStatus());
    }

    @Test
    public void testUpdateMutableFieldsRecordType() throws Exception {
        Record record = repository.newRecord();
        record.setRecordType(recordType2.getName(), recordType2.getVersion());
        record.setField(fieldType4.getName(), 123);
        record.setField(fieldType5.getName(), true);
        record.setField(fieldType6.getName(), "value1");
        record = repository.create(record);

        // Updating versioned mutable fields should not require record type to be specified in the record
        record = repository.newRecord(record.getId());
        record.setVersion(1L);
        record.setField(fieldType6.getName(), "value2");
        record = repository.update(record, true, true);

        // Change record to a different record type
        RecordType recordTypeA = typeManager.newRecordType(new QName("testmut", "RTA"));
        recordTypeA.addFieldTypeEntry(fieldType4.getId(), false);
        recordTypeA.addFieldTypeEntry(fieldType5.getId(), false);
        recordTypeA.addFieldTypeEntry(fieldType6.getId(), false);
        recordTypeA = typeManager.createRecordType(recordTypeA);

        // Change the record type of the non-versioned scope (at the time of this writing, could not modify
        // just the record type of a record, hence also touching a field)
        record = repository.read(record.getId());
        record.setRecordType(recordTypeA.getName(), null);
        record.setField(fieldType4.getName(), 456);
        record = repository.update(record);
        record = repository.read(record.getId());
        assertEquals(recordTypeA.getName(), record.getRecordTypeName());

        // The record type of the versioned mutable scope should still be what it was before
        assertEquals(recordType2.getName(), record.getRecordTypeName(Scope.VERSIONED_MUTABLE));

        // Now update a versioned-mutable field without specifying a record type, would expect it to move
        // also to the new record type of the non-versioned scope.
        record = repository.newRecord(record.getId());
        record.setVersion(1L);
        record.setField(fieldType6.getName(), "value3");
        record = repository.update(record, true, true);
        assertEquals(recordTypeA.getName(), record.getRecordTypeName(Scope.VERSIONED_MUTABLE));
        assertEquals(recordTypeA.getVersion(), record.getRecordTypeVersion(Scope.VERSIONED_MUTABLE));
    }

    @Test
    public void testReadMultipleRecords() throws Exception {
        Record record1 = createDefaultRecord();
        Record record2 = createDefaultRecord();
        Record record3 = createDefaultRecord();

        List<Record> readRecords = repository.read(Arrays.asList(record3.getId(), record1.getId()));

        assertEquals(2, readRecords.size());
        assertTrue(readRecords.contains(record1));
        assertTrue(readRecords.contains(record3));

        repository.delete(record2.getId());
        readRecords = repository.read(Arrays.asList(record2.getId(), record1.getId()));
        assertEquals(1, readRecords.size());
        assertTrue(readRecords.contains(record1));

        readRecords = repository.read(Collections.<RecordId>emptyList());
        assertTrue(readRecords.isEmpty());
    }

    @Test
    public void testConditionalUpdate() throws Exception {
        Record record = createDefaultRecord();

        //
        // Single condition
        //
        record.setField(fieldType1.getName(), "value2");
        record = repository
                .update(record, Collections.singletonList(new MutationCondition(fieldType1.getName(), "xyz")));

        assertEquals(ResponseStatus.CONFLICT, record.getResponseStatus());
        assertEquals("value1", record.getField(fieldType1.getName()));

        // Check repository state was really not modified
        record = repository.read(record.getId());
        assertEquals("value1", record.getField(fieldType1.getName()));

        //
        // Multiple conditions
        //
        List<MutationCondition> conditions = new ArrayList<MutationCondition>();
        conditions.add(new MutationCondition(fieldType1.getName(), "value1")); // evals to true
        conditions.add(new MutationCondition(fieldType2.getName(), 123)); // evals to true
        conditions.add(new MutationCondition(fieldType3.getName(), false)); // evals to false

        record.setField(fieldType1.getName(), "value2");
        record = repository.update(record, conditions);

        assertEquals(ResponseStatus.CONFLICT, record.getResponseStatus());
        assertEquals("value1", record.getField(fieldType1.getName()));

        // Check repository state was really not modified
        record = repository.read(record.getId());
        assertEquals("value1", record.getField(fieldType1.getName()));

        //
        // Record state already matches supplied state, conditions should not be checked, so we expect response
        // UP_TO_DATE rather than CONFLICT.
        //
        record.setField(fieldType1.getName(), "value1");
        record = repository
                .update(record, Collections.singletonList(new MutationCondition(fieldType1.getName(), "xyz")));

        assertEquals(ResponseStatus.UP_TO_DATE, record.getResponseStatus());

        // Do the same update twice (as can happen when auto-retrying in case of IO exceptions)
        record.setField(fieldType1.getName(), "value2");
        record = repository
                .update(record, Collections.singletonList(new MutationCondition(fieldType1.getName(), "value1")));
        assertEquals(ResponseStatus.UPDATED, record.getResponseStatus());

        record = repository
                .update(record, Collections.singletonList(new MutationCondition(fieldType1.getName(), "value1")));
        assertEquals(ResponseStatus.UP_TO_DATE, record.getResponseStatus());

        // reset record state
        record = createDefaultRecord();

        //
        // Not-equals condition
        //
        record.setField(fieldType1.getName(), "value2");
        record = repository.update(record,
                Collections.singletonList(new MutationCondition(fieldType1.getName(), CompareOp.NOT_EQUAL, "value1")));

        assertEquals(ResponseStatus.CONFLICT, record.getResponseStatus());
        assertEquals("value1", record.getField(fieldType1.getName()));

        //
        // Other than equals conditions
        //
        for (CompareOp op : CompareOp.values()) {
            List<Integer> testValues = new ArrayList<Integer>();
            switch (op) {
                case LESS:
                    testValues.add(123);
                    testValues.add(122);
                    break;
                case LESS_OR_EQUAL:
                    testValues.add(122);
                    break;
                case EQUAL:
                    testValues.add(122);
                    testValues.add(124);
                    break;
                case NOT_EQUAL:
                    testValues.add(123);
                    break;
                case GREATER_OR_EQUAL:
                    testValues.add(124);
                    break;
                case GREATER:
                    testValues.add(123);
                    testValues.add(124);
            }

            for (Integer testValue : testValues) {
                record.setField(fieldType2.getName(), 999);
                record = repository.update(record,
                        Collections.singletonList(new MutationCondition(fieldType2.getName(), op, testValue)));

                assertEquals(ResponseStatus.CONFLICT, record.getResponseStatus());
                assertEquals(123, record.getField(fieldType2.getName()));
            }
        }

        //
        // Allow missing fields
        //

        record.setField(fieldType1.getName(), "value2");
        // note that we're testing on field 1B!
        record = repository.update(record,
                Collections.singletonList(
                        new MutationCondition(fieldType1B.getName(), CompareOp.EQUAL, "whatever", true)));

        assertEquals(ResponseStatus.UPDATED, record.getResponseStatus());
        assertEquals("value2", record.getField(fieldType1.getName()));

        // reset record state
        record.setField(fieldType1.getName(), "value1");
        record = repository.update(record);

        //
        // Test for missing/present field
        //

        // Field MUST be missing
        record.setField(fieldType1.getName(), "value2");
        record = repository.update(record,
                Collections.singletonList(new MutationCondition(fieldType1.getName(), CompareOp.EQUAL, null)));

        assertEquals(ResponseStatus.CONFLICT, record.getResponseStatus());
        assertEquals("value1", record.getField(fieldType1.getName()));

        // Field MUST NOT be missing (but can have whatever value) -- note that we test on field 1B!
        record.setField(fieldType1.getName(), "value2");
        record = repository.update(record,
                Collections.singletonList(new MutationCondition(fieldType1B.getName(), CompareOp.NOT_EQUAL, null)));

        assertEquals(ResponseStatus.CONFLICT, record.getResponseStatus());
        assertEquals("value1", record.getField(fieldType1.getName()));

        // Same, successful case
        record.setField(fieldType1.getName(), "value2");
        record = repository.update(record,
                Collections.singletonList(new MutationCondition(fieldType1.getName(), CompareOp.NOT_EQUAL, null)));

        assertEquals(ResponseStatus.UPDATED, record.getResponseStatus());
        assertEquals("value2", record.getField(fieldType1.getName()));

        // reset record state
        record.setField(fieldType1.getName(), "value1");
        record = repository.update(record);

        //
        // Supplied values differ from field type (classcastexception)
        //

        // TODO
//        record.setField(fieldType1.getName(), "value2");
//        try {
//            repository.update(record, Collections.singletonList(new MutationCondition(fieldType1.getName(), new Long(55))));
//            fail("Expected an exception");
//        } catch (ClassCastException e) {
//            // expected
//        }

        //
        // Test on system fields
        //

        final QName versionField = new QName("org.lilyproject.system", "version");

        // Create new record to be sure numbering starts from 1
        record = createDefaultRecord();

        record.setField(fieldType2.getName(), new Integer(55));
        record = repository.update(record,
                Collections.singletonList(new MutationCondition(versionField, CompareOp.EQUAL, new Long(2))));
        assertEquals(ResponseStatus.CONFLICT, record.getResponseStatus());

        record.setField(fieldType2.getName(), new Integer(55));
        record = repository.update(record,
                Collections.singletonList(new MutationCondition(versionField, CompareOp.EQUAL, new Long(1))));
        assertEquals(ResponseStatus.UPDATED, record.getResponseStatus());
        assertEquals(new Long(2), record.getVersion());
        assertEquals(new Integer(55), record.getField(fieldType2.getName()));

        // Test behavior in case of null version
        record = repository.newRecord();
        record.setRecordType(recordType1.getName(), recordType1.getVersion());
        record.setField(fieldType1.getName(), "value1");
        record = repository.create(record);

        record.setField(fieldType1.getName(), "value2");
        record = repository.update(record,
                Collections.singletonList(new MutationCondition(versionField, CompareOp.EQUAL, new Long(1))));
        assertEquals(ResponseStatus.CONFLICT, record.getResponseStatus());

        record.setField(fieldType1.getName(), "value2");
        record = repository.update(record,
                Collections.singletonList(new MutationCondition(versionField, CompareOp.EQUAL, null)));
        assertEquals(ResponseStatus.UPDATED, record.getResponseStatus());

        //
        // Test conditional update on update of version-mutable fields
        //
        record = createDefaultRecord();

        record.setField(fieldType3.getName(), false);
        record = repository.update(record, true, true,
                Collections.singletonList(new MutationCondition(fieldType3.getName(), CompareOp.EQUAL, Boolean.FALSE)));
        assertEquals(ResponseStatus.CONFLICT, record.getResponseStatus());

        record.setField(fieldType3.getName(), false);
        record = repository.update(record, true, true,
                Collections.singletonList(new MutationCondition(fieldType3.getName(), CompareOp.EQUAL, Boolean.TRUE)));
        assertEquals(ResponseStatus.UPDATED, record.getResponseStatus());

        // In case of versioned-mutable update, we can also add conditions on versioned and non-versioned fields
        conditions = new ArrayList<MutationCondition>();
        conditions.add(new MutationCondition(fieldType1.getName(), "value1")); // evals to true
        conditions.add(new MutationCondition(fieldType2.getName(), 124)); // evals to true

        record.setField(fieldType3.getName(), true);
        record = repository.update(record, true, true, conditions);
        assertEquals(ResponseStatus.CONFLICT, record.getResponseStatus());
    }

    @Test
    public void testConditionalDelete() throws Exception {
        Record record = createDefaultRecord();

        // perform delete with not-satisfied conditions
        record = repository.delete(record.getId(),
                Collections.singletonList(new MutationCondition(fieldType1.getName(), "xyz")));

        assertNotNull(record);
        assertEquals(ResponseStatus.CONFLICT, record.getResponseStatus());
        assertEquals("value1", record.getField(fieldType1.getName()));
        assertEquals(1, record.getFields().size());

        // check record was surely not deleted
        record = repository.read(record.getId());

        // perform delete with satisfied conditions
        record = repository.delete(record.getId(),
                Collections.singletonList(new MutationCondition(fieldType1.getName(), "value1")));
        assertNull(record);
    }

    @Test
    public void testRecordWithLinkFields() throws Exception {
        FieldType linkFieldType = typeManager.createFieldType(typeManager
                .newFieldType(typeManager.getValueType("LINK"), new QName("testRecordWithLinkFields", "linkFieldType"),
                        Scope.NON_VERSIONED));

        RecordType recordTypeWithLink = typeManager.newRecordType(new QName(namespace, "recordTypeWithLink"));
        recordTypeWithLink.addFieldTypeEntry(typeManager.newFieldTypeEntry(linkFieldType.getId(), false));
        recordTypeWithLink = typeManager.createRecordType(recordTypeWithLink);

        // Create records to link to
        Record record = createDefaultRecord();
        Record record2 = createDefaultRecord();

        // Create record with link to record
        Record recordWithLinks = repository.newRecord();
        recordWithLinks.setRecordType(recordTypeWithLink.getName());
        Link link = Link.newBuilder().recordId(record.getId()).copyAll(false).create();
        recordWithLinks.setField(linkFieldType.getName(), link);
        recordWithLinks = repository.create(recordWithLinks);

        // Validate link is created
        link = (Link) recordWithLinks.getField(linkFieldType.getName());
        assertEquals(record.getId(), link.getMasterRecordId());

        // Read record again and validate link is there
        recordWithLinks = repository.read(recordWithLinks.getId());
        link = (Link) recordWithLinks.getField(linkFieldType.getName());
        assertEquals(record.getId(), link.getMasterRecordId());

        // Update record with link to record2
        recordWithLinks = repository.newRecord(recordWithLinks.getId());
        link = Link.newBuilder().recordId(record2.getId()).copyAll(false).create();
        recordWithLinks.setField(linkFieldType.getName(), link);
        recordWithLinks = repository.update(recordWithLinks);

        // Validate link is updated
        link = (Link) recordWithLinks.getField(linkFieldType.getName());
        assertEquals(record2.getId(), link.getMasterRecordId());

        // Read record and validate link is still updated
        recordWithLinks = repository.read(recordWithLinks.getId());
        link = (Link) recordWithLinks.getField(linkFieldType.getName());
        assertEquals(record2.getId(), link.getMasterRecordId());
    }

    @Test
    public void testRecordBuilder() throws Exception {
        RecordBuilder builder = repository.recordBuilder();
        Record record = builder.recordType(recordType1.getName())
                .field(fieldType1.getName(), "abc")
                .field(fieldType2.getName(), 123)
                .field(fieldType3.getName(), true)
                .create();
        assertEquals(record, repository.read(record.getId()));

        builder.reset();
        Record record2 = builder.recordType(recordType2.getName())
                .field(fieldType4.getName(), 999)
                .field(fieldType5.getName(), true)
                .field(fieldType6.getName(), "xyz")
                .create();

        Record readRecord = repository.read(record2.getId());
        assertEquals(999, readRecord.getField(fieldType4.getName()));
        try {
            readRecord.getField(fieldType1.getName());
            fail("FieldType1 not expected. Builder should have been reset");
        } catch (FieldNotFoundException expected) {
        }
    }

    @Test
    public void testDefaultNamespace() throws Exception {
        RecordBuilder builder = repository.recordBuilder();
        Record record = builder.defaultNamespace(namespace)
                .recordType("RT1")
                .field("field1", "abc")
                .field("field2", 123)
                .field("field3", true)
                .create();
        Record readRecord = repository.read(record.getId());
        assertEquals(record, readRecord);

        assertEquals("abc", readRecord.getField("field1"));
        readRecord.setDefaultNamespace("anotherNamespace");
        try {
            readRecord.getField("field1");
        } catch (FieldNotFoundException expected) {
        }
    }

    @Test
    public void testRecordBuilderCreateOrUpdate() throws Exception {
        try {
            repository
                    .recordBuilder()
                    .defaultNamespace(namespace)
                    .recordType("RT1")
                    .field("field1", "abc")
                    .createOrUpdate();
            fail("expected exception");
        } catch (RecordException e) {
            // expected
        }

        Record record = repository
                .recordBuilder()
                .assignNewUuid()
                .defaultNamespace(namespace)
                .recordType("RT1")
                .field("field1", "abc")
                .createOrUpdate();

        repository
                .recordBuilder()
                .id(record.getId())
                .defaultNamespace(namespace)
                .field("field1", "def")
                .createOrUpdate();
    }

    @Test
    public void testRecordBuilderNestedRecords() throws Exception {
        String NS = "testRecordBuilderNestedRecords";

        typeManager
                .recordTypeBuilder()
                .defaultNamespace(NS)
                .name("recordType")
                .fieldEntry().defineField().name("field1").create().add()
                .fieldEntry().defineField().name("field2").type("RECORD").create().add()
                .fieldEntry().defineField().name("field3").type("LIST<RECORD>").create().add()
                .create();


        Record record = repository
                .recordBuilder()
                .defaultNamespace(NS)
                .recordType("recordType")
                .recordField("field2")
                .recordType("recordType")
                .field("field1", "value 1")
                .set()
                .recordListField("field3")
                .recordType("recordType")
                .field("field1", "value 2")
                .add()
                .field("field1", "value 3")
                .add()
                .field("field1", "value 4")
                .endList()
                .create();

        record = repository.read(record.getId());
        assertEquals("value 1", ((Record) record.getField("field2")).getField("field1"));
        assertEquals("value 2", ((List<Record>) record.getField("field3")).get(0).getField("field1"));
        assertEquals("value 3", ((List<Record>) record.getField("field3")).get(1).getField("field1"));
        assertEquals("value 4", ((List<Record>) record.getField("field3")).get(2).getField("field1"));

        // Calling create on a nested record should not work
        try {
            repository
                    .recordBuilder()
                    .defaultNamespace(NS)
                    .recordType("recordType")
                    .recordField("field2")
                    .recordType("recordType")
                    .field("field1", "value 1")
                    .create();
            fail("expected exception");
        } catch (IllegalStateException e) {
            // expected
        }
    }

    @Test
    public void testRecordValueType() throws Exception {
        String namespace = "testRecordValueType";
        QName rvtRTName = new QName(namespace, "rvtRT");
        QName rtName = new QName(namespace, "rt");
        QName ft1Name = new QName(namespace, "ft1");
        QName ft2Name = new QName(namespace, "ft2");
        QName ft3Name = new QName(namespace, "ft3");
        typeManager.recordTypeBuilder().name(rvtRTName).field(fieldType1.getId(), false).create();
        ValueType rvt = typeManager.getValueType("RECORD<" + rvtRTName.toString() + ">");
        FieldType ft1 = typeManager.createFieldType(typeManager.newFieldType(rvt, ft1Name, Scope.NON_VERSIONED));
        FieldType ft2 = typeManager.createFieldType(typeManager.newFieldType(rvt, ft2Name, Scope.VERSIONED));
        FieldType ft3 = typeManager.createFieldType(typeManager.newFieldType(rvt, ft3Name, Scope.VERSIONED_MUTABLE));
        typeManager.recordTypeBuilder().name(rtName).field(ft1.getId(), false).field(ft2.getId(), false)
                .field(ft3.getId(), false).create();

        Record ft1Value1 = repository.recordBuilder().field(fieldType1.getName(), "ft1abc").build();
        Record ft1Value2 = repository.recordBuilder().field(fieldType1.getName(), "ft1def").build();
        Record ft2Value1 = repository.recordBuilder().field(fieldType1.getName(), "ft2abc").build();
        Record ft2Value2 = repository.recordBuilder().field(fieldType1.getName(), "ft2def").build();
        Record ft3Value1 = repository.recordBuilder().field(fieldType1.getName(), "ft3abc").build();
        Record ft3Value2 = repository.recordBuilder().field(fieldType1.getName(), "ft3def").build();
        Record ft3Value3 = repository.recordBuilder().field(fieldType1.getName(), "ft3xyz").build();

        // Create record
        Record createdRecord =
                repository.recordBuilder().recordType(rtName).field(ft1Name, ft1Value1).field(ft2Name, ft2Value1)
                        .field(ft3Name, ft3Value1).create();
        Record readRecord = repository.read(createdRecord.getId());
        assertEquals("ft1abc", ((Record) readRecord.getField(ft1Name)).getField(fieldType1.getName()));
        assertEquals("ft2abc", ((Record) readRecord.getField(ft2Name)).getField(fieldType1.getName()));
        assertEquals("ft3abc", ((Record) readRecord.getField(ft3Name)).getField(fieldType1.getName()));

        // Update record
        repository.recordBuilder().id(createdRecord.getId()).field(ft1Name, ft1Value2).field(ft2Name, ft2Value2)
                .field(ft3Name, ft3Value2).update();
        readRecord = repository.read(createdRecord.getId());
        assertEquals(new Long(2), readRecord.getVersion());
        assertEquals("ft1def", ((Record) readRecord.getField(ft1Name)).getField(fieldType1.getName()));
        assertEquals("ft2def", ((Record) readRecord.getField(ft2Name)).getField(fieldType1.getName()));
        assertEquals("ft3def", ((Record) readRecord.getField(ft3Name)).getField(fieldType1.getName()));

        readRecord = repository.read(createdRecord.getId(), 1L);
        assertEquals(new Long(1), readRecord.getVersion());
        assertEquals("ft1def", ((Record) readRecord.getField(ft1Name)).getField(fieldType1.getName()));
        assertEquals("ft2abc", ((Record) readRecord.getField(ft2Name)).getField(fieldType1.getName()));
        assertEquals("ft3abc", ((Record) readRecord.getField(ft3Name)).getField(fieldType1.getName()));

        // Update mutable field record
        repository.recordBuilder().id(createdRecord.getId()).version(1L).field(ft3Name, ft3Value3).updateVersion(true)
                .update();
        readRecord = repository.read(createdRecord.getId());
        assertEquals(new Long(2), readRecord.getVersion());
        assertEquals("ft1def", ((Record) readRecord.getField(ft1Name)).getField(fieldType1.getName()));
        assertEquals("ft2def", ((Record) readRecord.getField(ft2Name)).getField(fieldType1.getName()));
        assertEquals("ft3def", ((Record) readRecord.getField(ft3Name)).getField(fieldType1.getName()));

        readRecord = repository.read(createdRecord.getId(), 1L);
        assertEquals(new Long(1), readRecord.getVersion());
        assertEquals("ft1def", ((Record) readRecord.getField(ft1Name)).getField(fieldType1.getName()));
        assertEquals("ft2abc", ((Record) readRecord.getField(ft2Name)).getField(fieldType1.getName()));
        assertEquals("ft3xyz", ((Record) readRecord.getField(ft3Name)).getField(fieldType1.getName()));
    }

    @Test
    public void testRecordNestedInItself() throws Exception {
        String namespace = "testRecordNestedInItself";
        QName rvtRTName = new QName(namespace, "rvtRT");
        QName rtName = new QName(namespace, "rt");
        QName ft1Name = new QName(namespace, "ft1");
        QName ft2Name = new QName(namespace, "ft2");

        typeManager.recordTypeBuilder().name(rvtRTName).field(fieldType1.getId(), false).create();
        ValueType rvt = typeManager.getValueType("RECORD");
        FieldType ft1 = typeManager.createFieldType(typeManager.newFieldType(rvt, ft1Name, Scope.NON_VERSIONED));
        FieldType ft2 = typeManager.createFieldType(typeManager.newFieldType(rvt, ft2Name, Scope.VERSIONED));
        typeManager.recordTypeBuilder().name(rtName).field(ft1.getId(), false).field(ft2.getId(), false).create();

        Record ft1Value1 = repository.recordBuilder().recordType(rvtRTName).field(fieldType1.getName(), "ft1abc")
                .build();

        // Create nested record

        Record record = repository.recordBuilder().recordType(rtName).field(ft1Name, ft1Value1).build();
        record.setField(ft2Name, record);
        try {
            repository.create(record);
            fail("Expecting a Record Exception since a record may not be nested in itself");
        } catch (RecordException expected) {
        }

        // Create with deep nesting

        Record ft2Value2 = repository.recordBuilder().recordType(rvtRTName).field(ft1Name, record).build();
        record.setField(ft2Name, ft2Value2);
        try {
            repository.create(record);
            fail("Expecting a Record Exception since a record may not be nested in itself");
        } catch (RecordException expected) {
        }

        // Update with nested record

        record = repository.recordBuilder().recordType(rtName).field(ft1Name, ft1Value1).build();
        record = repository.create(record);
        record.setField(ft2Name, record); // Nest record in itself
        try {
            repository.update(record);
            fail("Expecting a Record Exception since a record may not be nested in itself");
        } catch (RecordException expected) {
        }
    }

    @Test
    public void testByteArrayValueType() throws Exception {
        FieldType byteArrayValueType = typeManager.createFieldType("BYTEARRAY", new QName("testByteArrayValueType",
                "field1"), Scope.NON_VERSIONED);
        RecordType recordType = typeManager.recordTypeBuilder().defaultNamespace("testByteArrayValueType")
                .name("recordType1")
                .field(byteArrayValueType.getId(), false).create();
        Record record = repository.recordBuilder().defaultNamespace("testByteArrayValueType").recordType("recordType1")
                .field("field1", new ByteArray(Bytes.toBytes("some bytes"))).create();
        Record readRecord = repository.read(record.getId());
        ByteArray readValue = readRecord.getField(new QName("testByteArrayValueType", "field1"));
        assertEquals("some bytes", Bytes.toString(readValue.getBytesUnsafe()));
    }

    @Test
    public void testScannerBasics() throws Exception {
        List<String> fieldValues = new ArrayList<String>();
        for (int i = 'A'; i <= 'Z'; i++) {
            RecordId id = idGenerator.newRecordId("Z" + (char) i);
            Record record = repository.newRecord(id);
            record.setRecordType(recordType1.getName());
            String value = "field 1 - " + (char) i;
            fieldValues.add(value);
            record.setField(fieldType1.getName(), value);
            repository.create(record);
        }

        RecordScan scan = new RecordScan();
        scan.setStartRecordId(idGenerator.newRecordId("ZA"));
        scan.setStopRecordId(idGenerator.newRecordId("ZZ")); // stop row is exclusive

        RecordScanner scanner = repository.getScanner(scan);

        int i = 0;
        Record record;
        while ((record = scanner.next()) != null) {
            assertEquals(record.getField(fieldType1.getName()), fieldValues.get(i));
            i++;
        }
        scanner.close();
        assertEquals("Found 25 records", 25, i);

        // Same using for-each loop
        scanner = repository.getScanner(scan);
        i = 0;
        for (Record result : scanner) {
            assertEquals(result.getField(fieldType1.getName()), fieldValues.get(i));
            i++;
        }
        scanner.close();
        assertEquals("Found 25 records", 25, i);

        // Scan all records, this should give at least 26 results
        scan = new RecordScan();
        scanner = repository.getScanner(scan);
        i = 0;
        while (scanner.next() != null) {
            i++;
        }

        assertTrue("Found at least 26 records", i >= 26);
    }

    @Test
    public void testScannerWithIdRecords() throws Exception {
        RecordId id = idGenerator.newRecordId();
        Record record = repository.newRecord(id);
        record.setRecordType(recordType1.getName());
        record.setField(fieldType1.getName(), "dummy value");
        repository.create(record);

        RecordScan scan = new RecordScan();

        IdRecordScanner scanner = repository.getScannerWithIds(scan);
        final IdRecord next = scanner.next();
        assertNotNull(next);
        assertFalse(next.getFieldIdToNameMapping().isEmpty());
    }

    @Test(expected = ClassCastException.class)
    public void testScannerWithoutIdRecords() throws Exception {
        RecordId id = idGenerator.newRecordId();
        Record record = repository.newRecord(id);
        record.setRecordType(recordType1.getName());
        record.setField(fieldType1.getName(), "dummy value");
        repository.create(record);

        RecordScan scan = new RecordScan();

        // normal Record scanner!
        RecordScanner scanner = repository.getScanner(scan);
        final Record next = scanner.next();
        assertNotNull(next);

        // this cast will fail
        final IdRecord casted = (IdRecord) next;
    }

    @Test
    public void testRecordTypeFilter() throws Exception {
        RecordType rt1 = typeManager.recordTypeBuilder()
                .name("RecordTypeFilter", "rt1")
                .fieldEntry().use(fieldType1).add()
                .create();

        RecordType rt2 = typeManager.recordTypeBuilder()
                .name("RecordTypeFilter", "rt2")
                .fieldEntry().use(fieldType1).add()
                .create();

        // create second version of the record type
        rt2 = typeManager.recordTypeBuilder()
                .name("RecordTypeFilter", "rt2")
                .update();

        assertEquals(new Long(2), rt2.getVersion());

        repository.recordBuilder().recordType(rt1.getName()).field(fieldType1.getName(), "value").create();
        repository.recordBuilder().recordType(rt1.getName()).field(fieldType1.getName(), "value").create();
        repository.recordBuilder().recordType(rt2.getName()).field(fieldType1.getName(), "value").create();
        repository.recordBuilder().recordType(rt2.getName(), 1L).field(fieldType1.getName(), "value").create();

        RecordScan scan = new RecordScan();
        scan.setRecordFilter(new RecordTypeFilter(rt1.getName()));
        assertEquals(2, countResults(repository.getScanner(scan)));

        scan = new RecordScan();
        scan.setRecordFilter(new RecordTypeFilter(rt2.getName()));
        assertEquals(2, countResults(repository.getScanner(scan)));

        scan = new RecordScan();
        scan.setRecordFilter(new RecordTypeFilter(rt2.getName(), 2L));
        assertEquals(1, countResults(repository.getScanner(scan)));
    }

    @Test
    public void testRecordTypeFilterInstanceOf() throws Exception {
        // The following code creates the following record type hierarchy:
        //
        //          rtA
        //         /   \
        //      rtB     rtC    rtE
        //               |
        //              rtD
        //

        RecordType rtA = typeManager.recordTypeBuilder()
                .name("RecordTypeFilterInstanceOf", "rtA")
                .fieldEntry().use(fieldType1).add()
                .create();

        RecordType rtB = typeManager.recordTypeBuilder()
                .name("RecordTypeFilterInstanceOf", "rtB")
                .fieldEntry().use(fieldType1).add()
                .supertype().use(rtA).add()
                .create();

        RecordType rtC = typeManager.recordTypeBuilder()
                .name("RecordTypeFilterInstanceOf", "rtC")
                .fieldEntry().use(fieldType1).add()
                .supertype().use(rtA).add()
                .create();

        RecordType rtD = typeManager.recordTypeBuilder()
                .name("RecordTypeFilterInstanceOf", "rtD")
                .fieldEntry().use(fieldType1).add()
                .supertype().use(rtC).add()
                .create();

        RecordType rtE = typeManager.recordTypeBuilder()
                .name("RecordTypeFilterInstanceOf", "rtE")
                .fieldEntry().use(fieldType1).add()
                .create();

        // Create a record of each type
        repository.recordBuilder().recordType(rtA.getName()).field(fieldType1.getName(), "value").create();
        repository.recordBuilder().recordType(rtB.getName()).field(fieldType1.getName(), "value").create();
        repository.recordBuilder().recordType(rtC.getName()).field(fieldType1.getName(), "value").create();
        repository.recordBuilder().recordType(rtD.getName()).field(fieldType1.getName(), "value").create();
        repository.recordBuilder().recordType(rtE.getName()).field(fieldType1.getName(), "value").create();

        // Check that with "instance of" searches we get the expected number of results for each type in the hierarchy
        RecordScan scan = new RecordScan();
        scan.setRecordFilter(new RecordTypeFilter(rtA.getName(), RecordTypeFilter.Operator.INSTANCE_OF));
        assertEquals(4, countResults(repository.getScanner(scan)));

        scan = new RecordScan();
        scan.setRecordFilter(new RecordTypeFilter(rtB.getName(), RecordTypeFilter.Operator.INSTANCE_OF));
        assertEquals(1, countResults(repository.getScanner(scan)));

        scan = new RecordScan();
        scan.setRecordFilter(new RecordTypeFilter(rtC.getName(), RecordTypeFilter.Operator.INSTANCE_OF));
        assertEquals(2, countResults(repository.getScanner(scan)));

        scan = new RecordScan();
        scan.setRecordFilter(new RecordTypeFilter(rtD.getName(), RecordTypeFilter.Operator.INSTANCE_OF));
        assertEquals(1, countResults(repository.getScanner(scan)));
    }

    @Test
    public void testRecordTypeFilterInstanceOfRecursionLoop() throws Exception {
        // Create a record type hierarchy which contains some endless loop in it:
        //   - the base hierarchy is C extends from B extends from A
        //   - A also extends from A and from C
        //   - B also extends from C
        //

        // The expected behavior is that it does not go in an endless loop but instead just stops when
        // encountering a loop (i.e. it doesn't throw an exception either)

        RecordType rtA = typeManager.recordTypeBuilder()
                .name("RecordTypeFilterInstanceOfRecursionLoop", "rtA")
                .fieldEntry().use(fieldType1).add()
                .create();

        RecordType rtB = typeManager.recordTypeBuilder()
                .name("RecordTypeFilterInstanceOfRecursionLoop", "rtB")
                .fieldEntry().use(fieldType1).add()
                .create();

        RecordType rtC = typeManager.recordTypeBuilder()
                .name("RecordTypeFilterInstanceOfRecursionLoop", "rtC")
                .fieldEntry().use(fieldType1).add()
                .create();


        rtA = typeManager.recordTypeBuilder()
                .name("RecordTypeFilterInstanceOfRecursionLoop", "rtA")
                .fieldEntry().use(fieldType1).add()
                .supertype().id(rtA.getId()).version(2L).add()
                .supertype().id(rtC.getId()).version(2L).add()
                .update();

        rtB = typeManager.recordTypeBuilder()
                .name("RecordTypeFilterInstanceOfRecursionLoop", "rtB")
                .fieldEntry().use(fieldType1).add()
                .supertype().id(rtA.getId()).version(2L).add()
                .supertype().id(rtC.getId()).version(2L).add()
                .update();

        rtC = typeManager.recordTypeBuilder()
                .name("RecordTypeFilterInstanceOfRecursionLoop", "rtC")
                .fieldEntry().use(fieldType1).add()
                .supertype().id(rtB.getId()).version(2L).add()
                .update();


        // Create a record of each type
        repository.recordBuilder().recordType(rtA.getName()).field(fieldType1.getName(), "value").create();
        repository.recordBuilder().recordType(rtB.getName()).field(fieldType1.getName(), "value").create();
        repository.recordBuilder().recordType(rtC.getName()).field(fieldType1.getName(), "value").create();


        RecordScan scan = new RecordScan();
        scan.setRecordFilter(new RecordTypeFilter(rtA.getName(), RecordTypeFilter.Operator.INSTANCE_OF));
        assertEquals(3, countResults(repository.getScanner(scan)));

        scan = new RecordScan();
        scan.setRecordFilter(new RecordTypeFilter(rtB.getName(), RecordTypeFilter.Operator.INSTANCE_OF));
        assertEquals(3, countResults(repository.getScanner(scan)));

        scan = new RecordScan();
        scan.setRecordFilter(new RecordTypeFilter(rtB.getName(), RecordTypeFilter.Operator.INSTANCE_OF));
        assertEquals(3, countResults(repository.getScanner(scan)));
    }

    @Test
    public void testRecordTypeFilterInstanceOfVersionSpecifics() throws Exception {
        // The instance-of check for record types is version unaware, or rather, looks at the latest version
        // of each record type. This is explained in the TypeManager.findSubTypes(SchemaId) docs.

        // Below we create a record type A with two versions, and type B extends from the first (non-latest)
        // version, and type C extends from the second (latest) version of type A.
        //
        //        rtA-version1       rtA-version2     rtD-version1
        //          |                 |                |
        //        rtB-version1       rtC-version2     rtC-version1
        //

        RecordType rtA = typeManager.recordTypeBuilder()
                .name("RecordTypeFilterInstanceOfVersionSpecifics", "rtA")
                .fieldEntry().use(fieldType1).add()
                .create();

        rtA = typeManager.recordTypeBuilder()
                .name("RecordTypeFilterInstanceOfVersionSpecifics", "rtA")
                .fieldEntry().use(fieldType1).add()
                .fieldEntry().use(fieldType2).add()
                .update();

        RecordType rtB = typeManager.recordTypeBuilder()
                .name("RecordTypeFilterInstanceOfVersionSpecifics", "rtB")
                .fieldEntry().use(fieldType1).add()
                .supertype().id(rtA.getId()).version(1L).add()
                .create();

        RecordType rtD = typeManager.recordTypeBuilder()
                .name("RecordTypeFilterInstanceOfVersionSpecifics", "rtD")
                .fieldEntry().use(fieldType1).add()
                .create();

        RecordType rtC = typeManager.recordTypeBuilder()
                .name("RecordTypeFilterInstanceOfVersionSpecifics", "rtC")
                .fieldEntry().use(fieldType1).add()
                .supertype().id(rtD.getId()).version(1L).add()
                .create();

        rtC = typeManager.recordTypeBuilder()
                .name("RecordTypeFilterInstanceOfVersionSpecifics", "rtC")
                .fieldEntry().use(fieldType1).add()
                .supertype().id(rtA.getId()).version(2L).add()
                .update();

        repository.recordBuilder().recordType(rtB.getName()).field(fieldType1.getName(), "value").create();
        repository.recordBuilder().recordType(rtC.getName()).field(fieldType1.getName(), "value").create();


        RecordScan scan = new RecordScan();
        scan.setRecordFilter(new RecordTypeFilter(rtA.getName(), RecordTypeFilter.Operator.INSTANCE_OF));
        assertEquals(2, countResults(repository.getScanner(scan)));

        // Since it is not the latest version of C that extends from D, searching for records that are an
        // instance of D will not return any results, even though C points to the latest version of D (because
        // it is the latest version of C which counts).
        scan = new RecordScan();
        scan.setRecordFilter(new RecordTypeFilter(rtD.getName(), RecordTypeFilter.Operator.INSTANCE_OF));
        assertEquals(0, countResults(repository.getScanner(scan)));
    }

    @Test
    public void testRecordTypeFilterInstanceOfUpdate() throws Exception {
        // Verify correctness of scans with a "instance of" record type filter in case the record type
        // inheritance is updated.
        //
        //  Initial state:
        //
        //       rtA     rtC
        //          \
        //           rtB
        //
        //  After update of rtB:
        //
        //       rtA     rtC
        //              /
        //           rtB
        //

        RecordType rtA = typeManager.recordTypeBuilder()
                .name("RecordTypeFilterInstanceOfUpdate", "rtA")
                .fieldEntry().use(fieldType1).add()
                .create();

        RecordType rtB = typeManager.recordTypeBuilder()
                .name("RecordTypeFilterInstanceOfUpdate", "rtB")
                .fieldEntry().use(fieldType1).add()
                .supertype().use(rtA).add()
                .create();

        RecordType rtC = typeManager.recordTypeBuilder()
                .name("RecordTypeFilterInstanceOfUpdate", "rtC")
                .fieldEntry().use(fieldType1).add()
                .create();

        repository.recordBuilder().recordType(rtB.getName()).field(fieldType1.getName(), "value").create();

        RecordScan scan = new RecordScan();
        scan.setRecordFilter(new RecordTypeFilter(rtA.getName(), RecordTypeFilter.Operator.INSTANCE_OF));
        assertEquals(1, countResults(repository.getScanner(scan)));

        scan = new RecordScan();
        scan.setRecordFilter(new RecordTypeFilter(rtC.getName(), RecordTypeFilter.Operator.INSTANCE_OF));
        assertEquals(0, countResults(repository.getScanner(scan)));

        rtB = typeManager.recordTypeBuilder()
                .name("RecordTypeFilterInstanceOfUpdate", "rtB")
                .fieldEntry().use(fieldType1).add()
                .supertype().use(rtC).add()
                .update();

        scan = new RecordScan();
        scan.setRecordFilter(new RecordTypeFilter(rtC.getName(), RecordTypeFilter.Operator.INSTANCE_OF));
        assertEquals(1, countResults(repository.getScanner(scan)));

        scan = new RecordScan();
        scan.setRecordFilter(new RecordTypeFilter(rtA.getName(), RecordTypeFilter.Operator.INSTANCE_OF));
        assertEquals(0, countResults(repository.getScanner(scan)));
    }

    @Test
    public void testFieldValueFilter() throws Exception {
        FieldType fieldType =
                typeManager.createFieldType("STRING", new QName("FieldValueFilter", "field"), Scope.NON_VERSIONED);
        RecordType rt = typeManager.recordTypeBuilder()
                .defaultNamespace("FieldValueFilter")
                .name("rt1")
                .fieldEntry()
                .use(fieldType)
                .add()
                .create();

        Record record =
                repository.recordBuilder().recordType(rt.getName()).field(fieldType.getName(), "value1").create();
        repository.recordBuilder().recordType(rt.getName()).field(fieldType.getName(), "value1").create();
        repository.recordBuilder().recordType(rt.getName()).field(fieldType.getName(), "value2").create();
        repository.recordBuilder().recordType(rt.getName()).field(fieldType.getName(), "value2").create();
        repository.recordBuilder().recordType(rt.getName()).field(fieldType.getName(), "value2").create();

        RecordScan scan = new RecordScan();
        scan.setRecordFilter(new FieldValueFilter(fieldType.getName(), "value1"));
        assertEquals(2, countResults(repository.getScanner(scan)));

        scan = new RecordScan();
        scan.setRecordFilter(new FieldValueFilter(fieldType.getName(), "value2"));
        assertEquals(3, countResults(repository.getScanner(scan)));

        scan = new RecordScan();
        scan.setRecordFilter(new FieldValueFilter(fieldType.getName(), CompareOp.NOT_EQUAL, "value1"));
        assertEquals(3, countResults(repository.getScanner(scan)));

        // (At the time of this writing, ...) when non-versioned fields are deleted, a delete marker is
        // written rather than really deleting the field. This delete marker would then also be 'not equal'
        // to the value we search, and we'd get an extra result. This test verifies the implementation takes
        // care of that.
        record.getFieldsToDelete().add(fieldType.getName());
        record.setField(fieldType1.getName(), "whatever");
        record = repository.update(record);

        scan = new RecordScan();
        scan.setRecordFilter(new FieldValueFilter(fieldType.getName(), CompareOp.NOT_EQUAL, "value1"));
        assertEquals(3, countResults(repository.getScanner(scan)));
    }

    @Test
    public void testFilterList() throws Exception {
        FieldType f1 = typeManager.createFieldType("STRING", new QName("FilterList", "field1"), Scope.NON_VERSIONED);
        FieldType f2 = typeManager.createFieldType("STRING", new QName("FilterList", "field2"), Scope.NON_VERSIONED);

        RecordType rt = typeManager.recordTypeBuilder().defaultNamespace("FilterList").name("rt")
                .fieldEntry().use(f1).add().fieldEntry().use(f2).add().create();

        repository.recordBuilder()
                .recordType(rt.getName())
                .field(f1.getName(), "A")
                .field(f2.getName(), "B")
                .create();

        repository.recordBuilder()
                .recordType(rt.getName())
                .field(f1.getName(), "A")
                .field(f2.getName(), "C")
                .create();

        repository.recordBuilder()
                .recordType(rt.getName())
                .field(f1.getName(), "D")
                .field(f2.getName(), "B")
                .create();

        repository.recordBuilder()
                .recordType(rt.getName())
                .field(f1.getName(), "F")
                .create();

        // Test f1=A and f2=B
        RecordScan scan = new RecordScan();
        RecordFilterList filterList = new RecordFilterList();
        filterList.addFilter(new FieldValueFilter(f1.getName(), "A"));
        filterList.addFilter(new FieldValueFilter(f2.getName(), "B"));
        scan.setRecordFilter(filterList);
        assertEquals(1, countResults(repository.getScanner(scan)));

        // Test f1=A or f2=B
        scan = new RecordScan();
        filterList = new RecordFilterList(RecordFilterList.Operator.MUST_PASS_ONE);
        filterList.addFilter(new FieldValueFilter(f1.getName(), "A"));
        filterList.addFilter(new FieldValueFilter(f2.getName(), "B"));
        scan.setRecordFilter(filterList);
        assertEquals(3, countResults(repository.getScanner(scan)));

        // Test f1=A and (f2=B or f2=C)
        scan = new RecordScan();
        RecordFilterList filterList2 = new RecordFilterList(RecordFilterList.Operator.MUST_PASS_ONE);
        filterList2.addFilter(new FieldValueFilter(f2.getName(), "B"));
        filterList2.addFilter(new FieldValueFilter(f2.getName(), "C"));
        filterList = new RecordFilterList();
        filterList.addFilter(new FieldValueFilter(f1.getName(), "A"));
        filterList.addFilter(filterList2);
        scan.setRecordFilter(filterList);
        assertEquals(2, countResults(repository.getScanner(scan)));

        // Test f1=F and f2=Z
        scan = new RecordScan();
        filterList = new RecordFilterList();
        filterList.addFilter(new FieldValueFilter(f1.getName(), "F"));
        filterList.addFilter(new FieldValueFilter(f2.getName(), "Z"));
        scan.setRecordFilter(filterList);
        assertEquals(0, countResults(repository.getScanner(scan)));

        // Test f1=F and (f2=Z with filterIfMissing=false)
        scan = new RecordScan();
        filterList = new RecordFilterList();
        filterList.addFilter(new FieldValueFilter(f1.getName(), "F"));
        FieldValueFilter fvf = new FieldValueFilter(f2.getName(), "Z");
        fvf.setFilterIfMissing(false);
        filterList.addFilter(fvf);
        scan.setRecordFilter(filterList);
        assertEquals(1, countResults(repository.getScanner(scan)));
    }

    @Test
    public void testScanWithReturnFields() throws Exception {
        FieldType f1 = typeManager.createFieldType("STRING", new QName("ReturnFieldsScan", "f1"), Scope.NON_VERSIONED);
        FieldType f2 = typeManager.createFieldType("STRING", new QName("ReturnFieldsScan", "f2"), Scope.NON_VERSIONED);
        FieldType f3 = typeManager.createFieldType("STRING", new QName("ReturnFieldsScan", "f3"), Scope.NON_VERSIONED);
        FieldType f4 = typeManager.createFieldType("STRING", new QName("ReturnFieldsScan", "f4"), Scope.NON_VERSIONED);

        RecordType rt = typeManager.recordTypeBuilder().defaultNamespace("ReturnFieldsScan").name("rt")
                .fieldEntry().use(f1).add()
                .fieldEntry().use(f2).add()
                .fieldEntry().use(f3).add()
                .fieldEntry().use(f4).add()
                .create();

        repository.recordBuilder()
                .recordType(rt.getName())
                .field(f1.getName(), "A")
                .field(f2.getName(), "B")
                .field(f3.getName(), "C")
                .create();


        // Test ALL filter
        RecordScan scan = new RecordScan();
        scan.setRecordFilter(new RecordTypeFilter(rt.getName()));
        scan.setReturnFields(new ReturnFields(ReturnFields.Type.ALL));
        Record record = repository.getScanner(scan).next();
        assertEquals(3, record.getFields().size());

        // Test NONE filter
        scan = new RecordScan();
        scan.setRecordFilter(new RecordTypeFilter(rt.getName()));
        scan.setReturnFields(new ReturnFields(ReturnFields.Type.NONE));
        record = repository.getScanner(scan).next();
        assertEquals(0, record.getFields().size());

        // Test ENUM filter
        scan = new RecordScan();
        scan.setRecordFilter(new RecordTypeFilter(rt.getName()));
        scan.setReturnFields(new ReturnFields(f1.getName(), f2.getName()));
        record = repository.getScanner(scan).next();
        assertEquals(2, record.getFields().size());
        assertTrue(record.hasField(f1.getName()));
        assertTrue(record.hasField(f2.getName()));
        assertFalse(record.hasField(f3.getName()));

        // Test scanning on filtered field, should not work
        scan = new RecordScan();
        RecordFilterList filterList = new RecordFilterList();
        filterList.addFilter(new RecordTypeFilter(rt.getName()));
        filterList.addFilter(new FieldValueFilter(f1.getName(), "A"));
        scan.setRecordFilter(filterList);
        // without ReturnFields, we get a result
        assertNotNull(repository.getScanner(scan).next());
        // with ReturnFields that doesn't include f1, we don't get a result
        scan.setReturnFields(new ReturnFields(f2.getName()));
        // TODO disabled this test as it was sometimes failing, and sometimes not
        //      In the cases where it failed, it did bring up the row which is
        //      correct according to the filters, but which we didn't expect to
        //      receive because a filter was applied on a non-read column.
        //      The conclusion could be that while HBase can't guarantee the filter
        //      will work always, it sometimes will work? Needs more investigation.
        //assertNull(repository.getScanner(scan).next());
    }
    
    /**
     * Tests if record type is set when different settings of returnFields is used.
     * @throws Exception
     */
    @Test
    public void testScanWithReturnFieldsRecordType() throws Exception {
        String ns = "ReturnFieldsScan-RecordType";
        FieldType f1 = typeManager.createFieldType("STRING", new QName(ns, "f1"), Scope.NON_VERSIONED);
        FieldType f2 = typeManager.createFieldType("STRING", new QName(ns, "f2"), Scope.NON_VERSIONED);
        FieldType f3 = typeManager.createFieldType("STRING", new QName(ns, "f3"), Scope.NON_VERSIONED);
        FieldType f4 = typeManager.createFieldType("STRING", new QName(ns, "f4"), Scope.NON_VERSIONED);

        RecordType rt = typeManager.recordTypeBuilder().defaultNamespace(ns).name("rt")
                .fieldEntry().use(f1).add()
                .fieldEntry().use(f2).add()
                .fieldEntry().use(f3).add()
                .fieldEntry().use(f4).add()
                .create();

        repository.recordBuilder()
                .recordType(rt.getName())
                .field(f1.getName(), "A")
                .field(f2.getName(), "B")
                .field(f3.getName(), "C")
                .create();


        // Test ALL filter
        RecordScan scan = new RecordScan();
        scan.setRecordFilter(new RecordTypeFilter(rt.getName()));
        scan.setReturnFields(new ReturnFields(ReturnFields.Type.ALL));
        Record record = repository.getScanner(scan).next();
        assertNotNull(record.getRecordTypeName());
        assertEquals(ns, record.getRecordTypeName().getNamespace());

        // Test NONE filter
        scan = new RecordScan();
        scan.setRecordFilter(new RecordTypeFilter(rt.getName()));
        scan.setReturnFields(new ReturnFields(ReturnFields.Type.NONE));
        record = repository.getScanner(scan).next();
        assertNotNull(record.getRecordTypeName());
        assertEquals(ns, record.getRecordTypeName().getNamespace());

        // Test ENUM filter
        scan = new RecordScan();
        scan.setRecordFilter(new RecordTypeFilter(rt.getName()));
        scan.setReturnFields(new ReturnFields(f1.getName(), f2.getName()));
        record = repository.getScanner(scan).next();
        assertNotNull(record.getRecordTypeName());
        assertEquals(ns, record.getRecordTypeName().getNamespace());
    }

    @Test
    public void testPrefixScans() throws Exception {
        repository.recordBuilder()
                .id("PrefixScanTest")
                .recordType(recordType1.getName())
                .field(fieldType1.getName(), "foo")
                .create();

        repository.recordBuilder()
                .id("PrefixScanTest-suffix1")
                .recordType(recordType1.getName())
                .field(fieldType1.getName(), "foo")
                .create();

        repository.recordBuilder()
                .id("PrefixScanTest-suffix2")
                .recordType(recordType1.getName())
                .field(fieldType1.getName(), "foo")
                .create();

        repository.recordBuilder()
                .id("QPrefixScanTest")
                .recordType(recordType1.getName())
                .field(fieldType1.getName(), "foo")
                .create();

        RecordScan scan = new RecordScan();
        scan.setStartRecordId(idGenerator.newRecordId("PrefixScanTest"));

        RecordScanner scanner = repository.getScanner(scan);
        assertEquals(idGenerator.newRecordId("PrefixScanTest"), scanner.next().getId());
        assertEquals(idGenerator.newRecordId("PrefixScanTest-suffix1"), scanner.next().getId());
        assertEquals(idGenerator.newRecordId("PrefixScanTest-suffix2"), scanner.next().getId());
        // the scanner would run till the end of the table
        assertNotNull(scanner.next());
        scanner.close();

        scan.setRecordFilter(new RecordIdPrefixFilter(idGenerator.newRecordId("PrefixScanTest")));
        scanner = repository.getScanner(scan);
        assertEquals(idGenerator.newRecordId("PrefixScanTest"), scanner.next().getId());
        assertEquals(idGenerator.newRecordId("PrefixScanTest-suffix1"), scanner.next().getId());
        assertEquals(idGenerator.newRecordId("PrefixScanTest-suffix2"), scanner.next().getId());
        // due to the prefix filter, the scanner stops once there are no records left with the same prefix
        assertNull(scanner.next());
        scanner.close();

        //
        // When using UUID record ID's, prefix scans make less sense, except for retrieving
        // variants
        //
        RecordId uuid = idGenerator.newRecordId();
        RecordId varid1 = idGenerator.newRecordId(uuid, ImmutableMap.of("lang", "en", "year", "1999"));
        RecordId varid2 = idGenerator.newRecordId(uuid, ImmutableMap.of("lang", "fr"));

        repository.recordBuilder()
                .id(uuid)
                .recordType(recordType1.getName())
                .field(fieldType1.getName(), "foo")
                .create();

        repository.recordBuilder()
                .id(varid1)
                .recordType(recordType1.getName())
                .field(fieldType1.getName(), "foo")
                .create();

        repository.recordBuilder()
                .id(varid2)
                .recordType(recordType1.getName())
                .field(fieldType1.getName(), "foo")
                .create();

        scan = new RecordScan();
        scan.setStartRecordId(uuid);
        scan.setRecordFilter(new RecordIdPrefixFilter(uuid));
        scanner = repository.getScanner(scan);
        assertEquals(uuid, scanner.next().getId());
        assertEquals(varid1, scanner.next().getId());
        assertEquals(varid2, scanner.next().getId());
        assertNull(scanner.next());
        scanner.close();
    }

    /**
     * At the time of this writing, record deletion was implemented by marking records
     * as deleted. We need to make sure these are skipped when scanning, and that
     * this doesn't conflict with custom filters.
     */
    @Test
    public void testScannerAndDeletedRecords() throws Exception {
        for (int i = 0; i < 5; i++) {
            RecordId id = idGenerator.newRecordId("ScanDeleteTest-" + i);
            Record record = repository.newRecord(id);
            record.setRecordType(recordType1.getName());
            String value = "dummy";
            record.setField(fieldType1.getName(), value);
            repository.create(record);
        }

        RecordScan scan = new RecordScan();
        scan.setStartRecordId(idGenerator.newRecordId("ScanDeleteTest-"));
        scan.setRecordFilter(new RecordIdPrefixFilter(idGenerator.newRecordId("ScanDeleteTest-")));

        RecordScanner scanner = repository.getScanner(scan);
        assertEquals(5, countResults(scanner));
        scanner.close();

        // This is to make sure the filtering of deleted records also works when we don't
        // specify a filter on our scan.
        RecordScan singleScan = new RecordScan();
        singleScan.setStartRecordId(idGenerator.newRecordId("ScanDeleteTest-0"));
        singleScan.setStopRecordId(idGenerator.newRecordId("ScanDeleteTest-0"));

        scanner = repository.getScanner(singleScan);
        assertEquals(1, countResults(scanner));
        scanner.close();

        // Delete the records, verify the new scanner results
        repository.delete(idGenerator.newRecordId("ScanDeleteTest-0"));

        scanner = repository.getScanner(scan);
        assertEquals(4, countResults(scanner));
        scanner.close();

        scanner = repository.getScanner(singleScan);
        assertEquals(0, countResults(scanner));
        scanner.close();
    }

    private int countResults(RecordScanner scanner) throws RepositoryException, InterruptedException {
        int i = 0;
        while (scanner.next() != null) {
            i++;
        }
        return i;
    }

    @Test
    public void testVariantScansWithKeys() throws Exception {
        final RecordId master = idGenerator.newRecordId("VariantScanWithKeysTest");
        final RecordId variant = idGenerator.newRecordId(master, ImmutableMap.of("key1", "value1", "key2", "value2"));
        final RecordId variantWithOtherValues =
                idGenerator.newRecordId(master, ImmutableMap.of("key1", "other-value-1", "key2", "other-value-2"));
        final RecordId extendedVariant =
                idGenerator.newRecordId(master, ImmutableMap.of("key1", "value1", "key2", "value2", "key3", "value3"));

        repository.recordBuilder()
                .id(master)
                .recordType(recordType1.getName())
                .field(fieldType1.getName(), "foo")
                .create();

        repository.recordBuilder()
                .id(variant)
                .recordType(recordType1.getName())
                .field(fieldType1.getName(), "foo")
                .create();

        repository.recordBuilder()
                .id(extendedVariant)
                .recordType(recordType1.getName())
                .field(fieldType1.getName(), "foo")
                .create();

        repository.recordBuilder()
                .id(variantWithOtherValues)
                .recordType(recordType1.getName())
                .field(fieldType1.getName(), "foo")
                .create();

        {
            // master scan
            final RecordScan scan = new RecordScan();
            scan.setRecordFilter(new RecordVariantFilter(master, new HashMap<String, String>()));
            RecordScanner scanner = repository.getScanner(scan);
            assertEquals(master, scanner.next().getId());
            assertNull(scanner.next());
            scanner.close();
        }

        {
            // variant scan
            final HashMap<String, String> variantProperties = new HashMap<String, String>();
            variantProperties.put("foo", null);
            variantProperties.put("bar", null);

            final RecordScan scan = new RecordScan();
            scan.setRecordFilter(new RecordVariantFilter(variant.getMaster(), variantProperties));
            RecordScanner scanner = repository.getScanner(scan);
            assertNull(scanner.next()); // it doesn't match anything
            scanner.close();
        }

        {
            // variant scan for something completely different
            final HashMap<String, String> variantProperties = new HashMap<String, String>();
            variantProperties.put("key1", null);
            variantProperties.put("key2", null);

            final RecordScan scan = new RecordScan();
            scan.setRecordFilter(new RecordVariantFilter(variant.getMaster(), variantProperties));
            RecordScanner scanner = repository.getScanner(scan);
            assertEquals(variant, scanner.next().getId());
            assertEquals(variantWithOtherValues, scanner.next().getId());
            assertNull(scanner.next()); // it doesn't match the extendedVariant!
            scanner.close();
        }

        {
            // extended variant scan
            final HashMap<String, String> variantProperties = new HashMap<String, String>();
            variantProperties.put("key1", null);
            variantProperties.put("key2", null);
            variantProperties.put("key3", null);

            final RecordScan scan = new RecordScan();
            scan.setRecordFilter(
                    new RecordVariantFilter(extendedVariant.getMaster(), variantProperties));
            RecordScanner scanner = repository.getScanner(scan);
            assertEquals(extendedVariant, scanner.next().getId());
            assertNull(scanner.next());
            scanner.close();
        }
    }

    @Test
    public void testVariantScansWithKeysAndValues() throws Exception {
        final RecordId master = idGenerator.newRecordId("VariantScanWithKeysAndValuesTest");
        final RecordId variant = idGenerator.newRecordId(master, ImmutableMap.of("key1", "value1", "key2", "value2"));
        final RecordId variantWithOtherValues =
                idGenerator.newRecordId(master, ImmutableMap.of("key1", "other-value-1", "key2", "value2"));
        final RecordId extendedVariant =
                idGenerator.newRecordId(master, ImmutableMap.of("key1", "value1", "key2", "value2", "key3", "value3"));

        repository.recordBuilder()
                .id(master)
                .recordType(recordType1.getName())
                .field(fieldType1.getName(), "foo")
                .create();

        repository.recordBuilder()
                .id(variant)
                .recordType(recordType1.getName())
                .field(fieldType1.getName(), "foo")
                .create();

        repository.recordBuilder()
                .id(extendedVariant)
                .recordType(recordType1.getName())
                .field(fieldType1.getName(), "foo")
                .create();

        repository.recordBuilder()
                .id(variantWithOtherValues)
                .recordType(recordType1.getName())
                .field(fieldType1.getName(), "foo")
                .create();

        {
            // variant scan with all values
            final HashMap<String, String> variantProperties = new HashMap<String, String>();
            variantProperties.put("key1", "value1");
            variantProperties.put("key2", "value2");

            final RecordScan scan = new RecordScan();
            scan.setRecordFilter(new RecordVariantFilter(variant.getMaster(), variantProperties));
            RecordScanner scanner = repository.getScanner(scan);
            assertEquals(variant, scanner.next().getId());
            assertNull(scanner.next()); // it doesn't match the other variants
            scanner.close();
        }

        {
            // variant scan with specific value for key2 only
            final HashMap<String, String> variantProperties = new HashMap<String, String>();
            variantProperties.put("key1", null);
            variantProperties.put("key2", "value2");

            final RecordScan scan = new RecordScan();
            scan.setRecordFilter(new RecordVariantFilter(variant.getMaster(), variantProperties));
            RecordScanner scanner = repository.getScanner(scan);
            assertEquals(variant, scanner.next().getId());
            assertEquals(variantWithOtherValues, scanner.next().getId()); // has same value for key2
            assertNull(scanner.next()); // it doesn't match the other variants
            scanner.close();
        }

        {
            // variant scan with specific value for key1 only
            final HashMap<String, String> variantProperties = new HashMap<String, String>();
            variantProperties.put("key1", "value1");
            variantProperties.put("key2", null);

            final RecordScan scan = new RecordScan();
            scan.setRecordFilter(new RecordVariantFilter(variant.getMaster(), variantProperties));
            RecordScanner scanner = repository.getScanner(scan);
            assertEquals(variant, scanner.next().getId());
            assertNull(scanner.next()); // it doesn't match the other variants
            scanner.close();
        }
    }

    @Test
    public void testMetadataSimpleStoreLoad() throws Exception {
        Record record = repository.newRecord();
        record.setRecordType(recordType1.getName());
        record.setField(fieldType1.getName(), "field value");

        Metadata metadata = new MetadataBuilder().value("field1", "value1").value("field2", "value2").build();

        record.setMetadata(fieldType1.getName(), metadata);
        assertEquals(2, record.getMetadataMap().get(fieldType1.getName()).getMap().size());

        record = repository.create(record);

        // Check state of returned record object
        Metadata returnedMetadata = record.getMetadata(fieldType1.getName());
        assertNotNull(returnedMetadata);
        assertEquals(2, returnedMetadata.getMap().size());
        assertEquals("value1", returnedMetadata.get("field1"));
        assertEquals("value2", returnedMetadata.get("field2"));

        // Check state when freshly reading record
        record = repository.read(record.getId());

        assertEquals(1, record.getMetadataMap().size());
        Metadata readMetadata = record.getMetadataMap().get(fieldType1.getName());
        assertNotNull(readMetadata);
        assertEquals(2, readMetadata.getMap().size());
        assertEquals("value1", readMetadata.get("field1"));
        assertEquals("value2", readMetadata.get("field2"));
    }

    @Test
    public void testMetadataAllTypes() throws Exception {
        // This test verifies that the change detection logic works correctly for all data types

        List<Pair> testValues = new ArrayList<Pair>();
        testValues.add(Pair.create("string1", "string2"));
        testValues.add(Pair.create(new Integer(1), new Integer(2)));
        testValues.add(Pair.create(new Long(1), new Long(2)));
        testValues.add(Pair.create(new Float(1f), new Float(2f)));
        testValues.add(Pair.create(new Double(1d), new Double(2d)));
        testValues.add(Pair.create(Boolean.TRUE, Boolean.FALSE));
        testValues.add(Pair.create(new ByteArray("A".getBytes()), new ByteArray("B".getBytes())));

        for (Pair testValue : testValues) {
            String ctx = "testing " + testValue.getV1() + " of type " + testValue.getV1().getClass().getName();

            Record record = repository.newRecord();
            record.setRecordType(recordType1.getName());
            record.setField(fieldType2.getName(), new Integer(1));
            record.setMetadata(fieldType2.getName(), new MetadataBuilder()
                    .object("somefield", testValue.getV1())
                    .build());
            record = repository.create(record);

            // test the type of the metadata has been retained
            assertEquals(ctx, testValue.getV1().getClass(),
                    record.getMetadata(fieldType2.getName()).getMap().get("somefield").getClass());

            // do dummy update, should not create new version
            //  (apparently, this also works fine for the float & double tests)
            record.setMetadata(fieldType2.getName(), new MetadataBuilder()
                    .object("somefield", testValue.getV1())
                    .build());
            record = repository.update(record);
            assertEquals(ctx, 1L, record.getVersion().longValue());

            // do update, should create new version
            record.setMetadata(fieldType2.getName(), new MetadataBuilder()
                    .object("somefield", testValue.getV2())
                    .build());
            record = repository.update(record);
            assertEquals(ctx, 2L, record.getVersion().longValue());

            // test the type of the metadata has been retained
            record = repository.read(record.getId());
            assertEquals(ctx, testValue.getV1().getClass(),
                    record.getMetadata(fieldType2.getName()).getMap().get("somefield").getClass());
        }
    }

    @Test
    public void testMetadataPartialUpdate() throws Exception {
        Record record = repository.newRecord();
        record.setRecordType(recordType1.getName());
        record.setField(fieldType1.getName(), "field value");

        Metadata metadata = new MetadataBuilder()
                .value("field1", "value1")
                .value("field2", "value2")
                .value("field3", "value3").build();

        record.setMetadata(fieldType1.getName(), metadata);
        record = repository.create(record);
        RecordId recordId = record.getId();

        // Now update metadata:
        //   - field1 is left unchanged and not specified
        //   - field2 is updated
        //   - field3 is deleted
        //   - a new field4 is added
        metadata = new MetadataBuilder()
                .value("field2", "value2a")
                .delete("field3")
                .value("field4", "value4").build();

        record = repository.newRecord(recordId);
        record.setMetadata(fieldType1.getName(), metadata);
        record = repository.update(record);

        // Check state of returned record object
        Metadata returnedMetadata = record.getMetadata(fieldType1.getName());
        assertNotNull(returnedMetadata);
        assertEquals(2, returnedMetadata.getMap().size());
        assertEquals("value2a", returnedMetadata.get("field2"));
        assertEquals("value4", returnedMetadata.get("field4"));
        assertEquals(0, returnedMetadata.getFieldsToDelete().size());

        // Check state when freshly reading record
        record = repository.read(record.getId());

        Metadata readMetadata = record.getMetadataMap().get(fieldType1.getName());
        assertNotNull(readMetadata);
        assertEquals(3, readMetadata.getMap().size());
        assertEquals("value1", readMetadata.get("field1"));
        assertEquals("value2a", readMetadata.get("field2"));
        assertEquals("value4", readMetadata.get("field4"));
    }

    @Test
    public void testMetadataOnUndefinedField() throws Exception {
        Record record = repository.newRecord();
        record.setRecordType(recordType1.getName());
        record.setField(fieldType1.getName(), "field value");
        // Set metadata on a field which does not have a value, such metadata should be ignored
        record.setMetadata(fieldType2.getName(), new MetadataBuilder().value("field1", "value1").build());

        record = repository.create(record);

        // Check state of returned record object
        assertNull(record.getMetadata(fieldType1.getName()));
        assertNull(record.getMetadata(fieldType2.getName()));
        assertEquals(0, record.getMetadataMap().size());

        // Check state of freshly read record
        record = repository.read(record.getId());
        assertNull(record.getMetadata(fieldType1.getName()));
        assertNull(record.getMetadata(fieldType2.getName()));
        assertEquals(0, record.getMetadataMap().size());
    }

    @Test
    public void testMetadataVersionedField() throws Exception {
        Record record = repository.newRecord();
        record.setRecordType(recordType1.getName());
        record.setField(fieldType2.getName(), new Integer(5));

        record.setMetadata(fieldType2.getName(), new MetadataBuilder().value("field1", "value1").build());
        record = repository.create(record);

        record = repository.read(record.getId());

        Metadata readMetadata = record.getMetadataMap().get(fieldType2.getName());
        assertNotNull(readMetadata);
        assertEquals(1, readMetadata.getMap().size());
        assertEquals("value1", readMetadata.get("field1"));
    }

    @Test
    public void testMetadataUpdateFieldAndMetadata() throws Exception {
        Record record = repository.newRecord();
        record.setRecordType(recordType1.getName());
        record.setField(fieldType2.getName(), new Integer(5));
        record.setMetadata(fieldType2.getName(), new MetadataBuilder()
                .value("field1", "value1")
                .value("field2", "value2").build());
        record = repository.create(record);

        // do update to field and metadata
        record.setField(fieldType2.getName(), new Integer(6));
        record.setMetadata(fieldType2.getName(), new MetadataBuilder()
                .value("field1", "value1a")
                .value("field3", "value3").build()); // note that we leave field2 unchanged, this tests the merging
                                                     // of old and new metadata
        record = repository.update(record);

        // validate state of returned record object
        assertEquals(2L, record.getVersion().longValue());
        assertEquals(new Integer(6), record.getField(fieldType2.getName()));

        Metadata metadata = record.getMetadataMap().get(fieldType2.getName());
        assertNotNull(metadata);
        assertEquals(2, metadata.getMap().size());
        assertEquals("value1a", metadata.get("field1"));
        assertEquals("value3", metadata.get("field3"));

        // validate state of read record
        record = repository.read(record.getId());
        assertEquals(2L, record.getVersion().longValue());
        assertEquals(new Integer(6), record.getField(fieldType2.getName()));

        metadata = record.getMetadataMap().get(fieldType2.getName());
        assertNotNull(metadata);
        assertEquals(3, metadata.getMap().size());
        assertEquals("value1a", metadata.get("field1"));
        assertEquals("value2", metadata.get("field2"));
        assertEquals("value3", metadata.get("field3"));
    }

    @Test
    public void testMetadataUpdateFieldOnly() throws Exception {
        Record record = repository.newRecord();
        record.setRecordType(recordType1.getName());
        record.setField(fieldType2.getName(), new Integer(5));
        record.setMetadata(fieldType2.getName(), new MetadataBuilder().value("field1", "value1").build());
        record = repository.create(record);
        RecordId recordId = record.getId();

        // update only field value, metadata should not be lost be inherited from previous record state
        record = repository.newRecord();
        record.setId(recordId);
        record.setField(fieldType2.getName(), new Integer(7));
        record = repository.update(record);

        // validate state of read record
        record = repository.read(record.getId());
        assertEquals(2L, record.getVersion().longValue());
        assertEquals(new Integer(7), record.getField(fieldType2.getName()));

        Metadata readMetadata = record.getMetadataMap().get(fieldType2.getName());
        assertNotNull(readMetadata);
        assertEquals(1, readMetadata.getMap().size());
        assertEquals("value1", readMetadata.get("field1"));
    }

    @Test
    public void testMetadataUpdateMetadataOnly() throws Exception {
        Record record = repository.newRecord();
        record.setRecordType(recordType1.getName());
        record.setField(fieldType2.getName(), new Integer(5));
        record.setMetadata(fieldType2.getName(), new MetadataBuilder()
                .value("field1", "value1")
                .value("field2", "value2").build());
        record = repository.create(record);
        RecordId recordId = record.getId();

        // Update only metadata -- should create new version
        record = repository.newRecord();
        record.setId(recordId);
        record.setMetadata(fieldType2.getName(), new MetadataBuilder().value("field1", "value1a").build());
        record = repository.update(record);

        Metadata metadata;

        // validate state of returned record
        assertEquals(2L, record.getVersion().longValue());
        metadata = record.getMetadataMap().get(fieldType2.getName());
        assertNotNull(metadata);
        assertEquals(1, metadata.getMap().size());
        assertEquals("value1a", metadata.get("field1"));

        // validate state of read record
        record = repository.read(record.getId());
        assertEquals(2L, record.getVersion().longValue());
        metadata = record.getMetadataMap().get(fieldType2.getName());
        assertNotNull(metadata);
        assertEquals(2, metadata.getMap().size());
        assertEquals("value1a", metadata.get("field1"));
        assertEquals("value2", metadata.get("field2"));
    }

    @Test
    public void testMetadataNoUpdate() throws Exception {
        Record record = repository.newRecord();
        record.setRecordType(recordType1.getName());
        record.setField(fieldType2.getName(), new Integer(1));
        record.setMetadata(fieldType2.getName(), new MetadataBuilder().value("field1", "value1").build());
        record = repository.create(record);

        assertEquals(1, record.getVersion().longValue());
        assertEquals(1, record.getMetadata(fieldType2.getName()).getMap().size());

        // resubmit the same record object, this should not cause an update
        record = repository.update(record);
        assertEquals(1, record.getVersion().longValue());

        // Delete a non-existing field from the metadata, this should also not cause an update
        record.setMetadata(fieldType2.getName(), new MetadataBuilder().delete("field2").build());
        record = repository.update(record);
        assertEquals(1, record.getVersion().longValue());

        // Once more with an empty metadata object
        record.setMetadata(fieldType2.getName(), new MetadataBuilder().build());
        record = repository.update(record);
        assertEquals(1, record.getVersion().longValue());

        // But if we do a real update, we should get a new version
        record.setMetadata(fieldType2.getName(), new MetadataBuilder().value("field1", "value1a").build());
        record = repository.update(record);
        assertEquals(2, record.getVersion().longValue());
    }

    @Test
    public void testMetadataOnDeletedField() throws Exception {
        Record record = repository.newRecord();
        record.setRecordType(recordType1.getName());
        record.setField(fieldType1.getName(), "field value");
        record.setField(fieldType2.getName(), new Integer(1));
        record.setMetadata(fieldType1.getName(), new MetadataBuilder().value("field1", "value1").build());
        record = repository.create(record);
        RecordId recordId = record.getId();

        record = repository.newRecord(recordId);
        record.delete(fieldType1.getName(), true);
        record.setMetadata(fieldType1.getName(), new MetadataBuilder().value("field1", "value1a").build());
        record = repository.update(record);

        // validate state of returned record
        assertFalse(record.hasField(fieldType1.getName()));
        assertNull(record.getMetadata(fieldType1.getName()));

        // validate state of read record
        record = repository.read(recordId);
        assertFalse(record.hasField(fieldType1.getName()));
        assertNull(record.getMetadata(fieldType1.getName()));
        assertNotNull(record.getField(fieldType2.getName()));
    }

    @Test
    public void testMetadataSuperfluousUpdates() throws Exception {
        // This test verifies that we don't write metadata if there are zero fields in it

        // Record with empty Metadata object
        Record record = repository.newRecord();
        record.setRecordType(recordType1.getName());
        record.setField(fieldType1.getName(), "field value");
        record.setField(fieldType2.getName(), new Integer(1));
        record.setMetadata(fieldType1.getName(), new MetadataBuilder().build());
        record = repository.create(record);

        // validate state of read record
        record = repository.read(record.getId());
        assertNull(record.getMetadata(fieldType1.getName()));

        // Record with Metadata object containing only delets
        record = repository.newRecord();
        record.setRecordType(recordType1.getName());
        record.setField(fieldType1.getName(), "field value");
        record.setField(fieldType2.getName(), new Integer(1));
        record.setMetadata(fieldType1.getName(), new MetadataBuilder().delete("field1").build());
        record = repository.create(record);

        // validate state of read record
        record = repository.read(record.getId());
        assertNull(record.getMetadata(fieldType1.getName()));
    }

    @Test
    public void testMetadataNotSupportedOnVersionedMutableFields() throws Exception {
        Record record = repository.newRecord();
        record.setRecordType(recordType1.getName());
        record.setField(fieldType1.getName(), "field value");
        record.setField(fieldType3.getName(), Boolean.TRUE);
        record.setMetadata(fieldType3.getName(), new MetadataBuilder().value("field1", "value1").build());
        try {
            record = repository.create(record);
            fail("Expected an exception trying to set metadata on a versioned-mutable field");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Field metadata is currently not supported for versioned-mutable fields."));
        }
    }

    @Test
    public void testMetadataNotSupportedOnBlobFields() throws Exception {
        String[] types = new String[] {"BLOB", "LIST<BLOB>", "LIST<PATH<BLOB>>"};

        for (int i = 0; i < types.length; i++) {
            FieldType blobField = typeManager
                    .createFieldType("BLOB", new QName("metadata-blob", "blob" + i), Scope.NON_VERSIONED);

            Blob blob = new Blob("text/plain", 5L, "foo");
            OutputStream os = repository.getOutputStream(blob);
            os.write("12345".getBytes());
            os.close();

            Record record = repository.newRecord();
            record.setRecordType(recordType1.getName());
            record.setField(blobField.getName(), blob);
            record.setMetadata(blobField.getName(), new MetadataBuilder().value("field1", "value1").build());
            try {
                record = repository.create(record);
                fail("Expected an exception trying to set metadata on a blob field");
            } catch (Exception e) {
                assertTrue(e.getMessage().contains("Field metadata is currently not supported for BLOB fields."));
            }
        }
    }

    @Test
    public void testFieldValueFilterWhenFieldHasMetadata() throws Exception {
        // The purpose of this test is to verify that the FieldValueFilter also works when there
        // is metadata for the field (metadata is stored in the same cell as the value, and should be ignored
        // by the FieldValueFilter)
        Record record = repository.newRecord();
        record.setRecordType(recordType1.getName());
        record.setField(fieldType1.getName(), "stop drinking coke");
        record.setMetadata(fieldType1.getName(), new MetadataBuilder().value("field1", "foobar").build());
        repository.create(record);

        RecordScan scan = new RecordScan();
        scan.setRecordFilter(new FieldValueFilter(fieldType1.getName(), "stop drinking coke"));
        assertEquals(1, countResults(repository.getScanner(scan)));
    }

    @Test
    public void testMetadataViaRecordBuilder() throws Exception {
        Record record = repository.recordBuilder()
                .recordType(recordType1.getName())
                .field(fieldType1.getName(), "hi")
                .metadata(fieldType1.getName(), new MetadataBuilder().value("x", "y").build())
                .create();

        record = repository.read(record.getId());
        assertEquals("y", record.getMetadata(fieldType1.getName()).get("x"));
    }
}
