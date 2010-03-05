package org.lilycms.repository.impl.test;

import static org.easymock.EasyMock.anyLong;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.easymock.classextension.EasyMock.createControl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.util.Bytes;
import org.easymock.classextension.IMocksControl;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilycms.repository.api.Field;
import org.lilycms.repository.api.FieldDescriptor;
import org.lilycms.repository.api.FieldNotFoundException;
import org.lilycms.repository.api.InvalidRecordException;
import org.lilycms.repository.api.Record;
import org.lilycms.repository.api.RecordExistsException;
import org.lilycms.repository.api.RecordNotFoundException;
import org.lilycms.repository.api.RecordType;
import org.lilycms.repository.api.Repository;
import org.lilycms.repository.api.TypeManager;
import org.lilycms.repository.impl.FieldDescriptorImpl;
import org.lilycms.repository.impl.FieldImpl;
import org.lilycms.repository.impl.HBaseRepository;
import org.lilycms.repository.impl.RecordImpl;
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
    private FieldDescriptorImpl nonVersionableFieldDescriptor;

    @Before
    public void setUp() throws Exception {
        control = createControl();
        typeManager = control.createMock(TypeManager.class);
        recordType = control.createMock(RecordType.class);
        expect(typeManager.getRecordType(isA(String.class), anyLong())).andReturn(recordType).anyTimes();
        expect(recordType.getRecordTypeId()).andReturn("dummyRecordType").anyTimes();
        expect(recordType.getVersion()).andReturn(Long.valueOf(0)).anyTimes();
        

        versionableFieldDescriptor = new FieldDescriptorImpl("aFieldDescriptor", 1, "dummyFieldType", true, true);
        nonVersionableFieldDescriptor = new FieldDescriptorImpl("aFieldDescriptor", 1, "dummyFieldType", true, false);

        
        repository = new HBaseRepository(typeManager, TEST_UTIL.getConfiguration());
    }

    @After
    public void tearDown() throws Exception {
        control.reset();
    }

    @Test
    public void testEmptyRecord() throws Exception {
        control.replay();
        Record record = generateRecord("emptyRecordId");

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
        Record record = generateRecord("emptyUpdateRecordId", new String[]{"aField" , "aValue", "false"});
        repository.create(record);
        
        Record emptyRecord = generateRecord("emptyUpdateRecordId");

        try {
            repository.update(emptyRecord);
            fail("A record should at least have some fields");
        } catch (Exception expected) {
        }
        control.verify();
    }

    @Test
    public void testNonVersionedRecord() throws Exception {
        expect(recordType.getFieldDescriptor("aField")).andReturn(nonVersionableFieldDescriptor);
        
        control.replay();
        String recordId = "nonVersionedRecordId";
        Record record = generateRecord(recordId, new String[] { "aField", "aValue"});
        repository.create(record);
        Record actualRecord = repository.read(recordId);
        assertEquals(record, actualRecord);
        control.verify();
    }

    @Test
    public void testMultipleFields() throws Exception {
        expect(recordType.getFieldDescriptor(isA(String.class))).andReturn(nonVersionableFieldDescriptor).times(2);

        control.replay();
        String recordId = "multipleFieldsId";
        Record record = generateRecord(recordId, new String[] { "aField", "aValue"}, new String[] {
                "aField2", "aValue2"});
        repository.create(record);
        Record actualRecord = repository.read(recordId);
        assertEquals(record, actualRecord);
        control.verify();
    }

    @Test
    public void testCreateExistingRecord() throws Exception {
        expect(recordType.getFieldDescriptor(isA(String.class))).andReturn(nonVersionableFieldDescriptor);

        control.replay();
        String recordId = "createExistingRecordId";
        Record record = generateRecord(recordId, new String[] { "aField", "aValue"});
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
        String recordId = "createUpdateDocumentId";
        Record record = generateRecord(recordId);

        try {
            repository.update(record);
            fail("Cannot update a non-existing document");
        } catch (RecordNotFoundException expected) {
        }
        control.verify();
    }

    @Test
    public void testUpdateRecord() throws Exception {
        expect(recordType.getFieldDescriptor(isA(String.class))).andReturn(nonVersionableFieldDescriptor).times(2);

        control.replay();
        String recordId = "updateDocumentId";
        Record record = generateRecord(recordId, new String[] { "aField", "aValue"});
        repository.create(record);

        Field field = new FieldImpl("aField", Bytes.toBytes("anotherValue"));
        record.addField(field);
        repository.update(record);

        assertEquals(record, repository.read(recordId));
        control.verify();
    }

    @Test
    public void testUpdateRecordWithExtraField() throws Exception {
        expect(recordType.getFieldDescriptor(isA(String.class))).andReturn(nonVersionableFieldDescriptor).anyTimes();

        control.replay();
        String recordId = "updateDocumentWithExtraFieldId";
        Record record = generateRecord(recordId, new String[] { "aField", "aValue"});
        repository.create(record);

        Field field = new FieldImpl("anotherField", Bytes.toBytes("anotherValue"));
        // TODO avoid updates of non-changed fields
        record.addField(field);
        repository.update(record);
        Record actualRecord = repository.read(recordId);
        assertEquals(2, record.getFields().size());
        assertEquals(record, actualRecord);
        control.verify();
    }

    @Test
    public void testReadNonExistingRecord() throws Exception {
        control.replay();
        String recordId = "readNonExistingRecordId";
        try {
            repository.read(recordId);
            fail("A RecordNotFoundException should be thrown");
        } catch (RecordNotFoundException expected) {
        }
        control.verify();
    }

    @Test
    public void testReadAll() throws Exception {
        expect(recordType.getFieldDescriptor(isA(String.class))).andReturn(nonVersionableFieldDescriptor).anyTimes();

        control.replay();
        String recordId = "readAllId";
        Record record = generateRecord(recordId, new String[] { "field1", "value1"}, new String[] { "field2",
                "value2"}, new String[] { "field3", "value3"});
        repository.create(record);
        Record actualRecord = repository.read(recordId);
        assertEquals(record, actualRecord);
        assertEquals(recordId, actualRecord.getRecordId());
        assertEquals(3, actualRecord.getFields().size());
        assertEquals("value1", new String(actualRecord.getField("field1").getValue()));
        assertEquals("value2", new String(actualRecord.getField("field2").getValue()));
        assertEquals("value3", new String(actualRecord.getField("field3").getValue()));
        control.verify();
    }

    @Test
    public void testRecordDelete() throws Exception {
        expect(recordType.getFieldDescriptor(isA(String.class))).andReturn(nonVersionableFieldDescriptor).anyTimes();

        control.replay();
        String recordId = "RecordDeleteId";
        Record record = generateRecord(recordId, new String[] { "field1", "value1"}, new String[] { "field2",
                "value2"}, new String[] { "field3", "value3"});
        repository.create(record);
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
        expect(recordType.getFieldDescriptor("field1")).andReturn(nonVersionableFieldDescriptor).once();
        expect(recordType.getFieldDescriptor("field2")).andReturn(versionableFieldDescriptor).once();

        control.replay();
        String recordId = "createVersionableAndNonVersionableId";
        Record record = generateRecord(recordId, new String[] { "field1", "value1",}, new String[] { "field2",
                "value2"});
        repository.create(record);
        Record actualRecord = repository.read(recordId);
        assertEquals(record, actualRecord);
        control.verify();
    }

    @Test
    public void testReadVersionableAndNonVersionableField() throws Exception {
        expect(recordType.getFieldDescriptor("field1")).andReturn(nonVersionableFieldDescriptor).times(2);
        expect(recordType.getFieldDescriptor("field2")).andReturn(versionableFieldDescriptor).times(2);

        control.replay();
        String recordId = "readVersionableAndNonVersionableFieldId";
        Record record = generateRecord(recordId, new String[] { "field1", "value1"}, new String[] { "field2",
                "value2"});
        repository.create(record);
        Record actualRecord = repository.read(recordId, "field1", "field2");
        assertEquals(record, actualRecord);
        control.verify();
    }

    @Test
    public void testVersionableField() throws Exception {
        expect(recordType.getFieldDescriptor(isA(String.class))).andReturn(versionableFieldDescriptor).anyTimes();

        control.replay();
        String recordId = "versionableFieldId";
        Record recordVersion1 = generateRecord(recordId, new String[] { "versionableField1", "value1"});
        repository.create(recordVersion1);
        Record recordVersion2 = generateRecord(recordId, new String[] { "versionableField1", "value2"});
        repository.update(recordVersion2);
        Record actualRecord = repository.read(recordId);
        assertEquals("value2", new String(actualRecord.getField("versionableField1").getValue()));
        assertEquals(2, actualRecord.getRecordVersion());
        actualRecord = repository.read(recordId, 1);
        recordVersion1.setRecordVersion(Long.valueOf(1));
        assertEquals(recordVersion1, actualRecord);
        control.verify();
    }

    @Test
    public void testVersionableFields() throws Exception {
        expect(recordType.getFieldDescriptor(isA(String.class))).andReturn(versionableFieldDescriptor).anyTimes();
        
        control.replay();
        String recordId = "versionableFieldsId";
        Record recordVersion1 = generateRecord(recordId, 
                        new String[] { "versionableField1", "f1value1"},
                        new String[] { "versionableField2", "f2value1"}, 
                        new String[] { "versionableField3", "f3value1"});
        repository.create(recordVersion1);
        Record update1 = generateRecord(recordId, 
                        new String[] { "versionableField1", "f1value2"}, 
                        new String[] { "versionableField2", "f2value2"});
        repository.update(update1);
        Record update2 = generateRecord(recordId, new String[] { "versionableField1", "f1value3"});
        repository.update(update2);
        
        Record expectedRecord = generateRecord(recordId,
                        new String[] { "versionableField1", "f1value3"},
                        new String[] { "versionableField2", "f2value2"}, 
                        new String[] { "versionableField3", "f3value1"});
        expectedRecord.setRecordVersion(3);
        Record actualRecord = repository.read(recordId);
        assertEquals(expectedRecord, actualRecord);
        
        actualRecord = repository.read(recordId, 3);
        assertEquals(expectedRecord, actualRecord);
        
        expectedRecord = generateRecord(recordId,
                        new String[] { "versionableField1", "f1value2"},
                        new String[] { "versionableField2", "f2value2"}, 
                        new String[] { "versionableField3", "f3value1"});
        expectedRecord.setRecordVersion(2);
        actualRecord = repository.read(recordId, 2);
        assertEquals(expectedRecord, actualRecord);
        
        expectedRecord = generateRecord(recordId,
                        new String[] { "versionableField1", "f1value1"},
                        new String[] { "versionableField2", "f2value1"}, 
                        new String[] { "versionableField3", "f3value1"});
        expectedRecord.setRecordVersion(1);
        actualRecord = repository.read(recordId, 1);
        assertEquals(expectedRecord, actualRecord);
        control.verify();
    }
    
    @Test
    public void testReadNonExistingversion() throws Exception {
        expect(recordType.getFieldDescriptor(isA(String.class))).andReturn(versionableFieldDescriptor).anyTimes();
        
        control.replay();
        String recordId = "readNonExistingversionId";
        Record recordVersion1 = generateRecord(recordId, 
                        new String[] { "versionableField1", "f1value1"});
        repository.create(recordVersion1);
        Record update1 = generateRecord(recordId, 
                        new String[] { "versionableField1", "f1value2"});
        repository.update(update1);
        try {
            repository.read(recordId, 3);
        } catch(RecordNotFoundException expected) {
            assertEquals(recordId, expected.getRecordId());
            assertEquals(Long.valueOf(3), expected.getVersion());
        }
        control.verify();
    }
    
    @Test
    public void testReadSpecificFields() throws Exception {
        expect(recordType.getFieldDescriptor("field1")).andReturn(nonVersionableFieldDescriptor).anyTimes();
        expect(recordType.getFieldDescriptor("field2")).andReturn(nonVersionableFieldDescriptor).anyTimes();
        expect(recordType.getFieldDescriptor("field3")).andReturn(versionableFieldDescriptor).anyTimes();
    
        control.replay();
        String recordId = "readSpecificFieldsId";
        Record record = generateRecord(recordId, new String[] { "field1", "value1"}, new String[] { "field2",
                "value2"}, new String[] { "field3", "value3"});
        repository.create(record);
        Record actualRecord = repository.read(recordId, "field1", "field3");
        assertEquals(recordId, actualRecord.getRecordId());
        assertEquals("value1", new String(actualRecord.getField("field1").getValue()));
        try {
            actualRecord.getField("field2");
            fail("An exception should be thrown because the record does not contain the requested field");
        } catch (FieldNotFoundException expected) {
        }
        assertEquals("value3", new String(actualRecord.getField("field3").getValue()));
        control.verify();
    }
    
    @Test
    public void testDeleteANonVersionableField() throws Exception {
        expect(recordType.getFieldDescriptor(isA(String.class))).andReturn(nonVersionableFieldDescriptor).anyTimes();

        control.replay();
        String recordId = "deleteANonVersionableFieldId";
        Record record = generateRecord(recordId , new String[] {"aField", "f1"});
        repository.create(record);
        Record deleteRecord = new RecordImpl(recordId);
        deleteRecord.setRecordType("dummyRecordType", 1);
        deleteRecord.deleteField("aField");
        repository.update(deleteRecord);
        Record actualRecord = repository.read(recordId);
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
        String recordId = "deleteAVersionableFieldId";
        Record record = generateRecord(recordId , new String[] {"aField", "f1"});
        repository.create(record);
        Record deleteRecord = new RecordImpl(recordId);
        deleteRecord.setRecordType("dummyRecordType", 1);
        deleteRecord.deleteField("aField");
        repository.update(deleteRecord);
        Record actualRecord = repository.read(recordId);
        try { 
            actualRecord.getField("aField");
            fail("Getting a deleted field from a record should throw a FieldNotFoundException");
        } catch(FieldNotFoundException expected) {
        }
        
        //TODO should we throw already at the moment of the read operation? i.e. validate that the requested fields are not null
        actualRecord = repository.read(recordId, "aField");
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
        String recordId = "deleteAVersionableFieldWithOlderVersionsId";
        Record record = generateRecord(recordId , new String[] {"aField", "f1"});
        repository.create(record);
        record.addField(new FieldImpl("aField", Bytes.toBytes("f2")));
        repository.update(record);
        
        Record deleteRecord = new RecordImpl(recordId);
        deleteRecord.setRecordType("dummyRecordType", 1);
        deleteRecord.deleteField("aField");
        repository.update(deleteRecord);

        Record actualRecord = repository.read(recordId);
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
        String recordId = "createVariantRecordId";
        Record record = generateRecord(recordId, new String[] {"aField", "f1"});
        repository.create(record);
        
        Record variantRecord = generateRecord(recordId, new String[] {"aVariantField", "vf1"});
        variantRecord.addVariantProperty("dimension1", "dimensionValue1");
        repository.create(variantRecord);
        
        Map<String, String> variantProperties = new HashMap<String, String>();
        variantProperties.put("dimension1", "dimensionValue1");
        Record actualVariantRecord = repository.read(recordId, variantProperties);
        
        assertEquals(variantRecord, actualVariantRecord);
        
        control.verify();
    }
    
    @Test
    public void testCreateVariantRecordWithNonExistingRecord() throws Exception {
        expect(recordType.getFieldDescriptor(isA(String.class))).andReturn(versionableFieldDescriptor).anyTimes();

        control.replay();
        String recordId = "createVariantRecordWithNonExistingRecordId";
        
        Record variantRecord = generateRecord(recordId, new String[] {"aVariantField", "vf1"});
        variantRecord.addVariantProperty("dimension1", "dimensionValue1");
        try {
            repository.create(variantRecord);
            fail();
        } catch (RecordNotFoundException expected) {
            assertEquals(recordId, expected.getRecordId());
            assertNull(expected.getVersion());
            assertNull(expected.getVariantProperties());
        }
        control.verify();
    }
    
    @Test
    public void testReadNonExistingVariantRecord() throws Exception {
        expect(recordType.getFieldDescriptor(isA(String.class))).andReturn(versionableFieldDescriptor).anyTimes();

        control.replay();
        String recordId = "readNonExistingVariantRecordId";
        Record record = generateRecord(recordId, new String[] {"aField", "f1"});
        repository.create(record);
        
        Record variantRecord = generateRecord(recordId, new String[] {"aVariantField", "vf1"});
        variantRecord.addVariantProperty("dimension1", "dimensionValue1");
        repository.create(variantRecord);
        
        Map<String, String> variantProperties = new HashMap<String, String>();
        variantProperties.put("dimension1", "dimensionValue2");
        try {
            repository.read(recordId, variantProperties);
            fail("Reading a non-existing variant should throw an exception");
        } catch (RecordNotFoundException expected) {
            assertEquals(recordId, expected.getRecordId());
            assertEquals(variantProperties, expected.getVariantProperties());
        }
        
        Map<String, String> variantProperties2 = new HashMap<String, String>();
        variantProperties2.put("dimension2", "dimensionValue1");
        try {
            repository.read(recordId, variantProperties2);
            fail("Reading a non-existing variant should throw an exception");
        } catch (RecordNotFoundException expected) {
            assertEquals(recordId, expected.getRecordId());
            assertEquals(variantProperties2, expected.getVariantProperties());
        }
        control.verify();
    }
    
    @Test
    public void testReadNonExistingVariantRecordVersion() throws Exception {
        expect(recordType.getFieldDescriptor(isA(String.class))).andReturn(versionableFieldDescriptor).anyTimes();

        control.replay();
        String recordId = "readNonExistingVariantRecordVersionId";
        Record record = generateRecord(recordId, new String[] {"aField", "f1"});
        repository.create(record);
        
        Record variantRecord = generateRecord(recordId, new String[] {"aVariantField", "vf1"});
        variantRecord.addVariantProperty("dimension1", "dimensionValue1");
        repository.create(variantRecord);
        
        Map<String, String> variantProperties = new HashMap<String, String>();
        variantProperties.put("dimension1", "dimensionValue2");
        try {
            repository.read(recordId, 2, variantProperties);
            fail("Reading a non-existing variant should throw an exception");
        } catch (RecordNotFoundException expected) {
            assertEquals(recordId, expected.getRecordId());
            assertEquals(Long.valueOf(2), expected.getVersion());
            assertEquals(variantProperties, expected.getVariantProperties());
        }
        
        control.verify();
    }
    
    @Test
    public void testVariantRecordVersions() throws Exception {
        expect(recordType.getFieldDescriptor(isA(String.class))).andReturn(versionableFieldDescriptor).anyTimes();

        control.replay();
        String recordId = "variantVersionsRecordId";
        Record record = generateRecord(recordId, new String[] {"field1", "f1"});
        repository.create(record);

        Map<String, String> variantProperties = new HashMap<String, String>();
        variantProperties.put("dimension1", "dimensionValue1");

        Record variantRecord = generateRecord(recordId, new String[] {"field1", "vf1"});
        variantRecord.addVariantProperties(variantProperties);
        repository.create(variantRecord);
        
        variantRecord = generateRecord(recordId, new String[] {"field1", "vf1B"}, new String[] {"field2", "vf2"});
        variantRecord.addVariantProperties(variantProperties);
        repository.update(variantRecord);

        variantRecord = generateRecord(recordId, new String[] {"field2", "vf2B"}, new String[] {"field3", "vf3"});
        variantRecord.addVariantProperties(variantProperties);
        repository.update(variantRecord);
        
        variantRecord = generateRecord(recordId);
        variantRecord.addVariantProperties(variantProperties);
        variantRecord.deleteField("field1");
        repository.update(variantRecord);
        
        variantRecord = generateRecord(recordId, new String[] {"field1", "vf1B"});
        variantRecord.addVariantProperties(variantProperties);
        repository.update(variantRecord);

        Record actualMasterRecord = repository.read(recordId);
        assertEquals("f1", Bytes.toString(actualMasterRecord.getField("field1").getValue()));
        Record actualVariantRecord = repository.read(recordId, 1, variantProperties);
        assertEquals("vf1", Bytes.toString(actualVariantRecord.getField("field1").getValue()));
        
        actualVariantRecord = repository.read(recordId, 2, variantProperties);
        assertEquals("vf1B", Bytes.toString(actualVariantRecord.getField("field1").getValue()));
        assertEquals("vf2", Bytes.toString(actualVariantRecord.getField("field2").getValue()));

        actualVariantRecord = repository.read(recordId, 3, variantProperties);
        assertEquals("vf1B", Bytes.toString(actualVariantRecord.getField("field1").getValue()));
        assertEquals("vf2B", Bytes.toString(actualVariantRecord.getField("field2").getValue()));
        assertEquals("vf3", Bytes.toString(actualVariantRecord.getField("field3").getValue()));

        actualVariantRecord = repository.read(recordId, 4, variantProperties);
        try { 
            actualVariantRecord.getField("field1");
            fail();
        } catch (FieldNotFoundException expected) {
        }
        
        actualVariantRecord = repository.read(recordId, 5, variantProperties);
        assertEquals("vf1B", Bytes.toString(actualVariantRecord.getField("field1").getValue()));
        
        try {
            actualVariantRecord = repository.read(recordId, 6, variantProperties);
            fail();
        } catch(RecordNotFoundException expected) {
        }

        control.verify();

    }

    private Record generateRecord(String recordId, String[]... fieldsAndValues) {
        Record record = new RecordImpl(recordId);
        
        record.setRecordType(recordType.getRecordTypeId(), recordType.getVersion());
        for (String[] fieldInfo : fieldsAndValues) {
            record.addField(new FieldImpl(fieldInfo[0], Bytes.toBytes(fieldInfo[1])));
        }
        return record;
    }

    
}
