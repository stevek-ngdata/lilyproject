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
package org.lilycms.repository.impl.test;

import static org.easymock.EasyMock.anyLong;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.easymock.classextension.EasyMock.createControl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.easymock.classextension.IMocksControl;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilycms.repository.api.FieldDescriptor;
import org.lilycms.repository.api.FieldNotFoundException;
import org.lilycms.repository.api.IdGenerator;
import org.lilycms.repository.api.InvalidRecordException;
import org.lilycms.repository.api.Record;
import org.lilycms.repository.api.RecordExistsException;
import org.lilycms.repository.api.RecordId;
import org.lilycms.repository.api.RecordNotFoundException;
import org.lilycms.repository.api.RecordType;
import org.lilycms.repository.api.Repository;
import org.lilycms.repository.api.TypeManager;
import org.lilycms.repository.impl.FieldDescriptorImpl;
import org.lilycms.repository.impl.HBaseRepository;
import org.lilycms.repository.impl.IdGeneratorImpl;
import org.lilycms.repository.impl.RecordImpl;
import org.lilycms.repository.impl.StringValueType;
import org.lilycms.repository.impl.ValueTypeImpl;
import org.lilycms.testfw.TestHelper;

public class HBaseRepositoryTest {

    private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging();
        TEST_UTIL.startMiniCluster(1);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        TEST_UTIL.shutdownMiniCluster();
    }

    private Repository repository;
    private IMocksControl control;
    private TypeManager typeManager;
    private RecordType recordType;
    private FieldDescriptor versionableFieldDescriptor;
    private FieldDescriptor nonVersionableFieldDescriptor;
    private IdGenerator idGenerator;

    @Before
    public void setUp() throws Exception {
        control = createControl();
        typeManager = control.createMock(TypeManager.class);
        expect(typeManager.getValueType("STRING", false)).andReturn(new ValueTypeImpl(new StringValueType(), false)).anyTimes();
        recordType = control.createMock(RecordType.class);
        expect(typeManager.getRecordType(isA(String.class), anyLong())).andReturn(recordType).anyTimes();
        expect(typeManager.getRecordType(isA(String.class))).andReturn(recordType).anyTimes();
        expect(recordType.getId()).andReturn("dummyRecordType").anyTimes();
        expect(recordType.getVersion()).andReturn(Long.valueOf(0)).anyTimes();
        
        // Using FieldDescriptorImpl since typeManager is a mock
        versionableFieldDescriptor = new FieldDescriptorImpl("aVersionableFieldDescriptor", Long.valueOf(1), new ValueTypeImpl(new StringValueType(), false), true, true);
        nonVersionableFieldDescriptor = new FieldDescriptorImpl("aNonVersionableFieldDescriptor", Long.valueOf(1), new ValueTypeImpl(new StringValueType(), false), true, false);

        idGenerator = new IdGeneratorImpl();
        repository = new HBaseRepository(typeManager, idGenerator, RecordImpl.class, TEST_UTIL.getConfiguration());
    }

    @After
    public void tearDown() throws Exception {
        control.reset();
    }

    @Test
    public void testEmptyRecord() throws Exception {
        control.replay();
        Record record = generateRecord();

        try {
            repository.create(record);
            fail("A record should at least have some fields");
        } catch (InvalidRecordException expected) {
        }
        control.verify();
    }
    
    @Test
    public void testEmptyUpdateRecord() throws Exception {
        expect(recordType.getFieldDescriptor("aField")).andReturn(versionableFieldDescriptor);
        
        control.replay();
        Record record = generateRecord(new String[]{"aField" , "aValue", "false"});
        repository.create(record);
        
        Record emptyRecord = repository.newRecord(record.getId());

        try {
            repository.update(emptyRecord);
            fail("A record should at least have some fields");
        } catch (Exception expected) {
        }
        control.verify();
    }

    @Test
    public void testNonVersionedRecord() throws Exception {
        expect(recordType.getFieldDescriptor("aField")).andReturn(nonVersionableFieldDescriptor).times(2);
        
        control.replay();
        Record record = generateRecord(new String[] { "aField", "aValue"});
        repository.create(record);
        Record actualRecord = repository.read(record.getId());
        assertEquals(record, actualRecord);
        control.verify();
    }

    @Test
    public void testMultipleFields() throws Exception {
        expect(recordType.getFieldDescriptor(isA(String.class))).andReturn(nonVersionableFieldDescriptor).times(4);

        control.replay();
        Record record = generateRecord(new String[] { "aField", "aValue"}, new String[] {"aField2", "aValue2"});
        repository.create(record);
        Record actualRecord = repository.read(record.getId());
        assertEquals(record, actualRecord);
        control.verify();
    }

    @Test
    public void testCreateExistingRecord() throws Exception {
        expect(recordType.getFieldDescriptor(isA(String.class))).andReturn(nonVersionableFieldDescriptor);

        control.replay();
        Record record = generateRecord(new String[] { "aField", "aValue"});
        repository.create(record);
        try {
            repository.create(record);
            fail("Create of an existing record is not allowed");
        } catch (RecordExistsException expected) {
        }
        control.verify();
    }

    @Test
    public void testUpdateNonExistingRecord() throws Exception {
        control.replay();
        Record record = repository.newRecord(idGenerator.newRecordId("nonExistingRecordId"));

        try {
            repository.update(record);
            fail("Cannot update a non-existing document");
        } catch (RecordNotFoundException expected) {
        }
        control.verify();
    }

    @Test
    public void testUpdateRecord() throws Exception {
        expect(recordType.getFieldDescriptor(isA(String.class))).andReturn(nonVersionableFieldDescriptor).times(3);

        control.replay();
        Record record = generateRecord(new String[] { "aField", "aValue"});
        repository.create(record);

        record.setField("aField", "anotherValue");
        repository.update(record);

        assertEquals(record, repository.read(record.getId()));
        control.verify();
    }

    @Test
    public void testUpdateRecordWithExtraField() throws Exception {
        expect(recordType.getFieldDescriptor(isA(String.class))).andReturn(nonVersionableFieldDescriptor).anyTimes();

        control.replay();
        Record record = generateRecord(new String[] { "aField", "aValue"});
        repository.create(record);

        // TODO avoid updates of non-changed fields
        record.setField("anotherField", "anotherValue");
        repository.update(record);
        Record actualRecord = repository.read(record.getId());
        assertEquals(2, record.getFields().size());
        assertEquals(record, actualRecord);
        control.verify();
    }

    @Test
    public void testReadNonExistingRecord() throws Exception {
        control.replay();
        try {
            repository.read(idGenerator.newRecordId("readNonExistingRecordId"));
            fail("A RecordNotFoundException should be thrown");
        } catch (RecordNotFoundException expected) {
        }
        control.verify();
    }

    @Test
    public void testReadAll() throws Exception {
        expect(recordType.getFieldDescriptor(isA(String.class))).andReturn(nonVersionableFieldDescriptor).anyTimes();

        control.replay();
        Record record = generateRecord(new String[] { "field1", "value1"}, new String[] { "field2",
                "value2"}, new String[] { "field3", "value3"});
        repository.create(record);
        Record actualRecord = repository.read(record.getId());
        assertEquals(record, actualRecord);
        assertEquals(record.getId(), actualRecord.getId());
        assertEquals(3, actualRecord.getFields().size());
        assertEquals("value1", actualRecord.getField("field1"));
        assertEquals("value2", actualRecord.getField("field2"));
        assertEquals("value3", actualRecord.getField("field3"));
        control.verify();
    }

    @Test
    public void testRecordDelete() throws Exception {
        expect(recordType.getFieldDescriptor(isA(String.class))).andReturn(nonVersionableFieldDescriptor).anyTimes();

        control.replay();
        Record record = generateRecord(new String[] { "field1", "value1"}, new String[] { "field2","value2"}, new String[] { "field3", "value3"});
        repository.create(record);
        RecordId recordId = record.getId();
        Record actualRecord = repository.read(recordId);
        assertEquals(record, actualRecord);
        repository.delete(recordId);
        try {
            repository.read(recordId);
            fail("A RecordNotFoundException should be thrown");
        } catch(RecordNotFoundException expected) {
        }
        control.verify();
    }

    @Test
    public void testCreateVersionableAndNonVersionable() throws Exception {
        expect(recordType.getFieldDescriptor("field1")).andReturn(nonVersionableFieldDescriptor).times(2);
        expect(recordType.getFieldDescriptor("field2")).andReturn(versionableFieldDescriptor).times(2);

        control.replay();
        Record record = generateRecord(new String[] { "field1", "value1",}, new String[] { "field2","value2"});
        repository.create(record);
        Record actualRecord = repository.read(record.getId());
        assertEquals(record, actualRecord);
        control.verify();
    }

    @Test
    public void testReadVersionableAndNonVersionableField() throws Exception {
        expect(recordType.getFieldDescriptor("field1")).andReturn(nonVersionableFieldDescriptor).times(3);
        expect(recordType.getFieldDescriptor("field2")).andReturn(versionableFieldDescriptor).times(3);

        control.replay();
        Record record = generateRecord(new String[] { "field1", "value1"}, new String[] { "field2",
                "value2"});
        repository.create(record);
        Record actualRecord = repository.read(record.getId(), "field1", "field2");
        assertEquals(record, actualRecord);
        control.verify();
    }

    @Test
    public void testVersionableField() throws Exception {
        expect(recordType.getFieldDescriptor(isA(String.class))).andReturn(versionableFieldDescriptor).anyTimes();

        control.replay();
        Record record = generateRecord(new String[] { "versionableField1", "value1"});
        repository.create(record);
        record.setField("versionableField1", "value2");
        repository.update(record);

        Record actualRecord = repository.read(record.getId());
        assertEquals("value2", actualRecord.getField("versionableField1"));
        assertEquals(Long.valueOf(2), actualRecord.getVersion());
        
        actualRecord = repository.read(record.getId(), Long.valueOf(1));
        assertEquals("value1", actualRecord.getField("versionableField1"));
        assertEquals(Long.valueOf(1), actualRecord.getVersion());
        control.verify();
    }

    @Test
    public void testVersionableFields() throws Exception {
        expect(recordType.getFieldDescriptor(isA(String.class))).andReturn(versionableFieldDescriptor).anyTimes();
        
        control.replay();
        Record record = generateRecord(
                        new String[] { "versionableField1", "f1value1"},
                        new String[] { "versionableField2", "f2value1"}, 
                        new String[] { "versionableField3", "f3value1"});
        repository.create(record);
        Record update1 = generateRecord(
                        new String[] { "versionableField1", "f1value2"}, 
                        new String[] { "versionableField2", "f2value2"});
        RecordId recordId = record.getId();
        update1.setId(recordId);
        repository.update(update1);
        
        Record update2 = generateRecord(new String[] { "versionableField1", "f1value3"});
        update2.setId(recordId);
        repository.update(update2);
        
        Record expectedRecord = generateRecord(
                        new String[] { "versionableField1", "f1value3"},
                        new String[] { "versionableField2", "f2value2"}, 
                        new String[] { "versionableField3", "f3value1"});
        expectedRecord.setId(recordId);
        expectedRecord.setVersion(Long.valueOf(3));
        Record actualRecord = repository.read(recordId);
        assertEquals(expectedRecord, actualRecord);
        
        actualRecord = repository.read(recordId, Long.valueOf(3));
        assertEquals(expectedRecord, actualRecord);
        
        expectedRecord = generateRecord(
                        new String[] { "versionableField1", "f1value2"},
                        new String[] { "versionableField2", "f2value2"}, 
                        new String[] { "versionableField3", "f3value1"});
        expectedRecord.setId(recordId);
        expectedRecord.setVersion(Long.valueOf(2));
        actualRecord = repository.read(recordId, Long.valueOf(2));
        assertEquals(expectedRecord, actualRecord);
        
        expectedRecord = generateRecord(
                        new String[] { "versionableField1", "f1value1"},
                        new String[] { "versionableField2", "f2value1"}, 
                        new String[] { "versionableField3", "f3value1"});
        expectedRecord.setId(recordId);
        expectedRecord.setVersion(Long.valueOf(1));
        actualRecord = repository.read(recordId, Long.valueOf(1));
        assertEquals(expectedRecord, actualRecord);
        control.verify();
    }
    
    @Test
    public void testReadNonExistingversion() throws Exception {
        expect(recordType.getFieldDescriptor(isA(String.class))).andReturn(versionableFieldDescriptor).anyTimes();
        
        control.replay();
        Record record = generateRecord(new String[] { "versionableField1", "f1value1"});
        repository.create(record);
        record.setField("versionableField1", "f1value2");
        repository.update(record);
        try {
            repository.read(record.getId(), Long.valueOf(3));
        } catch(RecordNotFoundException expected) {
            assertEquals(record.getId(), expected.getRecord().getId());
            assertEquals(Long.valueOf(3), expected.getRecord().getVersion());
        }
        control.verify();
    }
    
    @Test
    public void testReadSpecificFields() throws Exception {
        expect(recordType.getFieldDescriptor("field1")).andReturn(nonVersionableFieldDescriptor).anyTimes();
        expect(recordType.getFieldDescriptor("field2")).andReturn(nonVersionableFieldDescriptor).anyTimes();
        expect(recordType.getFieldDescriptor("field3")).andReturn(versionableFieldDescriptor).anyTimes();
    
        control.replay();
        Record record = generateRecord(new String[] { "field1", "value1"}, new String[] { "field2",
                "value2"}, new String[] { "field3", "value3"});
        repository.create(record);
        Record actualRecord = repository.read(record.getId(), "field1", "field3");
        assertEquals(record.getId(), actualRecord.getId());
        assertEquals("value1", actualRecord.getField("field1"));
        try {
            actualRecord.getField("field2");
            fail("An exception should be thrown because the record does not contain the requested field");
        } catch (FieldNotFoundException expected) {
        }
        assertEquals("value3", actualRecord.getField("field3"));
        control.verify();
    }
    
    @Test
    public void testDeleteANonVersionableField() throws Exception {
        expect(recordType.getFieldDescriptor(isA(String.class))).andReturn(nonVersionableFieldDescriptor).anyTimes();

        control.replay();
        Record record = generateRecord(new String[] {"aField", "f1"});
        repository.create(record);
        Record deleteRecord = repository.newRecord(record.getId());
        deleteRecord.setRecordType("dummyRecordType", 1);
        deleteRecord.deleteField("aField");
        repository.update(deleteRecord);
        Record actualRecord = repository.read(record.getId());
        try { 
            actualRecord.getField("aField");
            fail("Getting a deleted field from a record should throw a FieldNotFoundException");
        } catch(FieldNotFoundException expected) {
        }
        control.verify();
    }
    
    @Test
    public void testDeleteAVersionableField() throws Exception {
        expect(recordType.getFieldDescriptor(isA(String.class))).andReturn(versionableFieldDescriptor).anyTimes();

        control.replay();
        Record record = generateRecord(new String[] {"aField", "f1"});
        repository.create(record);
        Record deleteRecord = repository.newRecord(record.getId());
        deleteRecord.setRecordType("dummyRecordType", 1);
        deleteRecord.deleteField("aField");
        repository.update(deleteRecord);
        Record actualRecord = repository.read(record.getId());
        try { 
            actualRecord.getField("aField");
            fail("Getting a deleted field from a record should throw a FieldNotFoundException");
        } catch(FieldNotFoundException expected) {
        }
        
        //TODO should we throw already at the moment of the read operation? i.e. validate that the requested fields are not null
        actualRecord = repository.read(record.getId(), "aField");
        try {
            actualRecord.getField("aField");
            fail("Getting a deleted field from a record should throw a FieldNotFoundException");
        } catch(FieldNotFoundException expected) {
        }
        control.verify();
    }
    
    @Test
    public void testDeleteAVersionableFieldWithOlderVersions() throws Exception {
        expect(recordType.getFieldDescriptor(isA(String.class))).andReturn(versionableFieldDescriptor).anyTimes();

        control.replay();
        Record record = generateRecord(new String[] {"aField", "f1"});
        repository.create(record);
        record.setField("aField", "f2");
        repository.update(record);
        
        Record deleteRecord = repository.newRecord(record.getId());
        deleteRecord.setRecordType("dummyRecordType", 1);
        deleteRecord.deleteField("aField");
        repository.update(deleteRecord);

        Record actualRecord = repository.read(record.getId());
        try { 
            actualRecord.getField("aField");
            fail("Getting a deleted field from a record should throw a FieldNotFoundException");
        } catch(FieldNotFoundException expected) {
        }
        control.verify();
    }
    
    @Test
    public void testCreateVariantRecord() throws Exception {
        expect(recordType.getFieldDescriptor(isA(String.class))).andReturn(versionableFieldDescriptor).anyTimes();

        control.replay();
        Record record = generateRecord(new String[] {"aField", "f1"});
        repository.create(record);
        
        Map<String, String> variantProperties = new HashMap<String, String>();
        variantProperties.put("dimension1", "dimensionValue1");
        Record variantRecord = generateRecord(new String[] {"aVariantField", "vf1"});
        RecordId variantRecordId = idGenerator.newRecordId(record.getId(), variantProperties);
        variantRecord.setId(variantRecordId);
        repository.create(variantRecord);
        
        Record actualVariantRecord = repository.read(variantRecordId);
        
        assertEquals(variantRecord, actualVariantRecord);
        
        control.verify();
    }
    
    @Test
    public void testCreateVariantRecordWithNonExistingRecord() throws Exception {
        expect(recordType.getFieldDescriptor(isA(String.class))).andReturn(versionableFieldDescriptor).anyTimes();

        control.replay();
        Map<String, String> variantProperties = new HashMap<String, String>();
        variantProperties.put("dimension1", "dimensionValue1");
        Record variantRecord = generateRecord(new String[] {"aVariantField", "vf1"});
        variantRecord.setId(idGenerator.newRecordId(idGenerator.newRecordId("nonExistingMasterRecordId"), variantProperties ));
        try {
            repository.create(variantRecord);
            fail();
        } catch (RecordNotFoundException expected) {
            assertEquals(variantRecord, expected.getRecord());
        }
        control.verify();
    }
    
    @Test
    public void testReadNonExistingVariantRecord() throws Exception {
        expect(recordType.getFieldDescriptor(isA(String.class))).andReturn(versionableFieldDescriptor).anyTimes();

        control.replay();
        Record record = generateRecord(new String[] {"aField", "f1"});
        repository.create(record);
        
        Record variantRecord = generateRecord(new String[] {"aVariantField", "vf1"});
        Map<String, String> variantProperties = new HashMap<String, String>();
        variantProperties.put("dimension1", "dimensionValue1");
        RecordId variantRecordId = idGenerator.newRecordId(record.getId(), variantProperties );
        variantRecord.setId(variantRecordId);
        
        try {
            repository.read(variantRecordId);
            fail("Reading a non-existing variant should throw an exception");
        } catch (RecordNotFoundException expected) {
            assertEquals(variantRecordId, expected.getRecord().getId());
        }
        repository.create(variantRecord);
        
        Map<String, String> variantProperties2 = new HashMap<String, String>();
        variantProperties2.put("dimension2", "dimensionValue1");
        RecordId variantRecordId2 = idGenerator.newRecordId(record.getId(), variantProperties2);
        try {
            repository.read(variantRecordId2);
            fail("Reading a non-existing variant should throw an exception");
        } catch (RecordNotFoundException expected) {
            assertEquals(variantRecordId2, expected.getRecord().getId());
        }
        control.verify();
    }
    
    @Test
    public void testReadNonExistingVariantRecordVersion() throws Exception {
        expect(recordType.getFieldDescriptor(isA(String.class))).andReturn(versionableFieldDescriptor).anyTimes();

        control.replay();
        Record record = generateRecord(new String[] {"aField", "f1"});
        repository.create(record);
        
        Record variantRecord = generateRecord(new String[] {"aVariantField", "vf1"});
        Map<String, String> variantProperties = new HashMap<String, String>();
        variantProperties.put("dimension1", "dimensionValue1");
        RecordId variantRecordId = idGenerator.newRecordId(record.getId(), variantProperties );
        variantRecord.setId(variantRecordId);
        repository.create(variantRecord);
        
        try {
            repository.read(variantRecordId, Long.valueOf(2));
            fail("Reading a non-existing variant should throw an exception");
        } catch (RecordNotFoundException expected) {
            assertEquals(variantRecordId, expected.getRecord().getId());
            assertEquals(Long.valueOf(2), expected.getRecord().getVersion());
        }
        
        control.verify();
    }
    
    @Test
    public void testVariantRecordVersions() throws Exception {
        expect(recordType.getFieldDescriptor(isA(String.class))).andReturn(versionableFieldDescriptor).anyTimes();

        control.replay();
        Record record = generateRecord(new String[] {"field1", "f1"});
        repository.create(record);
        RecordId recordId = record.getId();
        Map<String, String> variantProperties = new HashMap<String, String>();
        variantProperties.put("dimension1", "dimensionValue1");
        RecordId variantRecordId = idGenerator.newRecordId(recordId, variantProperties);

        Record variantRecord = generateRecord(new String[] {"field1", "vf1"});
        variantRecord.setId(variantRecordId);
        repository.create(variantRecord);
        
        variantRecord = generateRecord(new String[] {"field1", "vf1B"}, new String[] {"field2", "vf2"});
        variantRecord.setId(variantRecordId);
        repository.update(variantRecord);

        variantRecord = generateRecord(new String[] {"field2", "vf2B"}, new String[] {"field3", "vf3"});
        variantRecord.setId(variantRecordId);
        repository.update(variantRecord);
        
        variantRecord = generateRecord();
        variantRecord.setId(variantRecordId);
        variantRecord.deleteField("field1");
        repository.update(variantRecord);
        
        variantRecord = generateRecord(new String[] {"field1", "vf1B"});
        variantRecord.setId(variantRecordId);
        repository.update(variantRecord);

        Record actualMasterRecord = repository.read(recordId);
        assertEquals("f1", actualMasterRecord.getField("field1"));
        Record actualVariantRecord = repository.read(variantRecordId, Long.valueOf(1));
        assertEquals("vf1", actualVariantRecord.getField("field1"));
        
        actualVariantRecord = repository.read(variantRecordId, Long.valueOf(2));
        assertEquals("vf1B", actualVariantRecord.getField("field1"));
        assertEquals("vf2", actualVariantRecord.getField("field2"));

        actualVariantRecord = repository.read(variantRecordId, Long.valueOf(3));
        assertEquals("vf1B", actualVariantRecord.getField("field1"));
        assertEquals("vf2B", actualVariantRecord.getField("field2"));
        assertEquals("vf3", actualVariantRecord.getField("field3"));

        actualVariantRecord = repository.read(variantRecordId, Long.valueOf(4));
        try { 
            actualVariantRecord.getField("field1");
            fail();
        } catch (FieldNotFoundException expected) {
        }
        
        actualVariantRecord = repository.read(variantRecordId, Long.valueOf(5));
        assertEquals("vf1B", actualVariantRecord.getField("field1"));
        
        try {
            actualVariantRecord = repository.read(variantRecordId, Long.valueOf(6));
            fail();
        } catch(RecordNotFoundException expected) {
        }

        control.verify();

    }

    private Record generateRecord(String[]... fieldsAndValues) throws Exception {
        Record record = repository.newRecord();
        
        record.setRecordType(recordType.getId(), recordType.getVersion());
        for (String[] fieldInfo : fieldsAndValues) {
            record.setField(fieldInfo[0], fieldInfo[1]);
        }
        return record;
    }
}
