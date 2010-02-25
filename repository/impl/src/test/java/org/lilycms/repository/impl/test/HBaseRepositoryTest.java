package org.lilycms.repository.impl.test;

import static org.easymock.EasyMock.anyLong;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.easymock.classextension.EasyMock.createControl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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
        expect(recordType.getName()).andReturn("dummyRecordType").anyTimes();
        expect(recordType.getVersion()).andReturn(new Long(0)).anyTimes();
        

        versionableFieldDescriptor = new FieldDescriptorImpl("aFieldDescriptor", 1, "dummyFieldType", true, true);
        nonVersionableFieldDescriptor = new FieldDescriptorImpl("aFieldDescriptor", 1, "dummyFieldType", true, false);

        
        repository = new HBaseRepository(typeManager, TEST_UTIL.getConfiguration());
    }

    @After
    public void tearDown() throws Exception {
        control.reset();
    }

    @Test
    public void testEmptyRecord() {
        control.replay();
        Record record = generateRecord("emptyRecordId");

        try {
            repository.create(record);
            fail("A record should at least have some fields");
        } catch (Exception expected) {
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
    public void testReadSpecificFields() throws Exception {
        expect(recordType.getFieldDescriptor(isA(String.class))).andReturn(nonVersionableFieldDescriptor).anyTimes();

        control.replay();
        String recordId = "readSpecificFieldsId";
        Record record = generateRecord(recordId, new String[] { "field1", "value1"}, new String[] { "field2",
                "value2"}, new String[] { "field3", "value3"});
        repository.create(record);
        Record actualRecord = repository.read(recordId, recordType.getName(), recordType.getVersion(), "field1", "field3");
        assertEquals(recordId, actualRecord.getRecordId());
        assertEquals("value1", new String(actualRecord.getField("field1").getValue()));
        try {
            actualRecord.getField("field2");
            fail("An exception is thrown because the record does not contain the requested field");
        } catch (FieldNotFoundException expected) {
        }
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
        Record actualRecord = repository.read(recordId, recordType.getName(), recordType.getVersion(), "field1", "field2");
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
        actualRecord = repository.read(recordId, 1);
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
        Record actualRecord = repository.read(recordId);
        assertEquals(expectedRecord, actualRecord);
        
        actualRecord = repository.read(recordId, 3);
        assertEquals(expectedRecord, actualRecord);
        
        expectedRecord = generateRecord(recordId,
                        new String[] { "versionableField1", "f1value2"},
                        new String[] { "versionableField2", "f2value2"}, 
                        new String[] { "versionableField3", "f3value1"});
        actualRecord = repository.read(recordId, 2);
        assertEquals(expectedRecord, actualRecord);
        
        expectedRecord = generateRecord(recordId,
                        new String[] { "versionableField1", "f1value1"},
                        new String[] { "versionableField2", "f2value1"}, 
                        new String[] { "versionableField3", "f3value1"});
        actualRecord = repository.read(recordId, 1);
        assertEquals(expectedRecord, actualRecord);
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
        actualRecord = repository.read(recordId, recordType.getName(), recordType.getVersion(), "aField");
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
    
    private Record generateRecord(String recordId, String[]... fieldsAndValues) {
        Record record = new RecordImpl(recordId);
        record.setRecordType("dummyRecordType", 1);
        for (String[] fieldInfo : fieldsAndValues) {
            record.addField(new FieldImpl(fieldInfo[0], Bytes.toBytes(fieldInfo[1])));
        }
        return record;
    }

}
