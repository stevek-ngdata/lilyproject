package org.lilycms.repository.api.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilycms.repository.api.Field;
import org.lilycms.repository.api.HBaseRepository;
import org.lilycms.repository.api.NoSuchRecordException;
import org.lilycms.repository.api.Record;
import org.lilycms.repository.api.RecordExistsException;
import org.lilycms.repository.api.Repository;

public class HBaseRepositoryTest {

    private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TEST_UTIL.startMiniCluster(1);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        TEST_UTIL.shutdownMiniCluster();
    }

    private Repository repository;
    
    @Before
    public void setUp() throws Exception {
        repository = new HBaseRepository(TEST_UTIL.getConfiguration());
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testEmptyRecord() {
        Record record = generateRecord("emptyRecordId");

        try {
            repository.create(record);
            fail("A record should at least have some fields");
        } catch (Exception expected) {
        }
    }
    
    @Test
    public void testNonVersionedRecord() throws Exception {
        String recordId = "nonVersionedRecordId";
        Record record = generateRecord(recordId, new String[]{"aField", "aValue"});
        repository.create(record);
        Record actualRecord = repository.read(recordId);
        assertEquals(record, actualRecord);       
    }
    
    @Test
    public void testMultipleFields() throws Exception {
        String recordId = "multipleFieldsId";
        Record record = generateRecord(recordId, new String[]{"aField", "aValue"}, new String[]{"aField2" ,"aValue2"});
        repository.create(record);
        Record actualRecord = repository.read(recordId);
        assertEquals(record, actualRecord);       
    }

    @Test
    public void testCreateExistingRecord() throws Exception {
        String recordId = "createExistingRecordId";
        Record record = generateRecord(recordId, new String[]{"aField", "aValue"});
        repository.create(record);
        try {
            repository.create(record);
            fail("Create of an existing record is not allowed");
        } catch(RecordExistsException expected) {
        }
    }

    @Test
    // TODO automatically fallback on create?
    public void testUpdateNonExistingRecord() throws Exception {
        String recordId = "createUpdateDocumentId";
        Record record = generateRecord(recordId);
        
        try {
            repository.update(record);
            fail("Cannot update a non-existing document");
        } catch(NoSuchRecordException expected) {
        }
    }
    
    @Test
    public void testUpdateRecord() throws Exception {
        String recordId = "updateDocumentId";
        Record record = generateRecord(recordId, new String[]{"aField", "aValue"});
        repository.create(record);
        
        Field field = new Field("aField", Bytes.toBytes("anotherValue"));
        record.addField(field);
        repository.update(record);
        
        assertEquals(record, repository.read(recordId));
    }
    
    @Test
    public void testUpdateRecordWithExtraField() throws Exception {
        String recordId = "updateDocumentWithExtraFieldId";
        Record record = generateRecord(recordId, new String[]{"aField", "aValue"});
        repository.create(record);
        
        Field field = new Field("anotherField", Bytes.toBytes("anotherValue"));
        record.addField(field);
        repository.update(record);
        Record actualRecord = repository.read(recordId);
        assertEquals(2, record.getFields().size());
        assertEquals(record, actualRecord);
    }
    
    @Test
    public void testReadNonExistingRecord() throws Exception {
        String recordId = "readNonExistingRecordId";
        assertNull(repository.read(recordId));
    }
    
    @Test
    public void testReadAll() throws Exception {
        String recordId = "readAllId";
        Record record = generateRecord(recordId, new String[]{"field1", "value1"}, new String[]{"field2", "value2"}, new String[]{"field3", "value3"});
        repository.create(record);
        Record actualRecord = repository.read(recordId);
        assertEquals(record, actualRecord);
        assertEquals(recordId, actualRecord.getRecordId());
        assertEquals(3, actualRecord.getFields().size());
        assertEquals("value1", new String(actualRecord.getField("field1").getValue()));
        assertEquals("value2", new String(actualRecord.getField("field2").getValue()));
        assertEquals("value3", new String(actualRecord.getField("field3").getValue()));
    }
    
    @Test
    public void testReadSpecificFields() throws Exception {
        String recordId = "readSpecificFieldsId";
        Record record = generateRecord(recordId, new String[]{"field1", "value1"}, new String[]{"field2", "value2"}, new String[]{"field3", "value3"});
        repository.create(record);
        Set<String> fieldNames = new HashSet<String>();
        fieldNames.add("field1");
        fieldNames.add("field3");
        Record actualRecord = repository.read(recordId, fieldNames);
        assertEquals(recordId, actualRecord.getRecordId());
        assertEquals("value1", new String(actualRecord.getField("field1").getValue()));
        assertNull(actualRecord.getField("field2"));
        assertEquals("value3", new String(actualRecord.getField("field3").getValue()));
    }
    
    @Test
    public void testRecordDelete() throws Exception {
        String recordId = "RecordDeleteId";
        Record record = generateRecord(recordId, new String[]{"field1", "value1"}, new String[]{"field2", "value2"}, new String[]{"field3", "value3"});
        repository.create(record);
        Record actualRecord = repository.read(recordId);
        assertEquals(record, actualRecord);
        repository.delete(recordId);
        actualRecord = repository.read(recordId);
        assertNull(actualRecord);
    }
    
    private Record generateRecord(String recordId, String[] ... fieldsAndValues) {
        Record record = new Record(recordId);
        for (String[] fieldValuePair : fieldsAndValues) {
            record.addField(new Field(fieldValuePair[0], Bytes.toBytes(fieldValuePair[1])));
        }
        return record;
    }
    
}
