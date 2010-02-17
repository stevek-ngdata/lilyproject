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
        Record record = generateRecord(recordId, new String[] { "aField", "aValue", "false" });
        repository.create(record);
        Record actualRecord = repository.read(recordId);
        assertEquals(record, actualRecord);
    }

    @Test
    public void testMultipleFields() throws Exception {
        String recordId = "multipleFieldsId";
        Record record = generateRecord(recordId, new String[] { "aField", "aValue", "false" }, new String[] {
                "aField2", "aValue2", "false" });
        repository.create(record);
        Record actualRecord = repository.read(recordId);
        assertEquals(record, actualRecord);
    }

    @Test
    public void testCreateExistingRecord() throws Exception {
        String recordId = "createExistingRecordId";
        Record record = generateRecord(recordId, new String[] { "aField", "aValue", "false" });
        repository.create(record);
        try {
            repository.create(record);
            fail("Create of an existing record is not allowed");
        } catch (RecordExistsException expected) {
        }
    }

    @Test
    public void testUpdateNonExistingRecord() throws Exception {
        String recordId = "createUpdateDocumentId";
        Record record = generateRecord(recordId);

        try {
            repository.update(record);
            fail("Cannot update a non-existing document");
        } catch (NoSuchRecordException expected) {
        }
    }

    @Test
    public void testUpdateRecord() throws Exception {
        String recordId = "updateDocumentId";
        Record record = generateRecord(recordId, new String[] { "aField", "aValue", "false" });
        repository.create(record);

        Field field = new Field("aField", Bytes.toBytes("anotherValue"), false);
        record.addField(field);
        repository.update(record);

        assertEquals(record, repository.read(recordId));
    }

    @Test
    public void testUpdateRecordWithExtraField() throws Exception {
        String recordId = "updateDocumentWithExtraFieldId";
        Record record = generateRecord(recordId, new String[] { "aField", "aValue", "false" });
        repository.create(record);

        Field field = new Field("anotherField", Bytes.toBytes("anotherValue"), false);
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
        Record record = generateRecord(recordId, new String[] { "field1", "value1", "false" }, new String[] { "field2",
                "value2", "false" }, new String[] { "field3", "value3", "false" });
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
        Record record = generateRecord(recordId, new String[] { "field1", "value1", "false" }, new String[] { "field2",
                "value2", "false" }, new String[] { "field3", "value3", "false" });
        repository.create(record);
        Set<String> fieldNames = new HashSet<String>();
        fieldNames.add("field1");
        fieldNames.add("field3");
        Record actualRecord = repository.read(recordId, null, fieldNames);
        assertEquals(recordId, actualRecord.getRecordId());
        assertEquals("value1", new String(actualRecord.getField("field1").getValue()));
        assertNull(actualRecord.getField("field2"));
        assertEquals("value3", new String(actualRecord.getField("field3").getValue()));
    }

    @Test
    public void testRecordDelete() throws Exception {
        String recordId = "RecordDeleteId";
        Record record = generateRecord(recordId, new String[] { "field1", "value1", "false" }, new String[] { "field2",
                "value2", "false" }, new String[] { "field3", "value3", "false" });
        repository.create(record);
        Record actualRecord = repository.read(recordId);
        assertEquals(record, actualRecord);
        repository.delete(recordId);
        actualRecord = repository.read(recordId);
        assertNull(actualRecord);
    }

    @Test
    public void testCreateVersionableAndNonVersionable() throws Exception {
        String recordId = "createVersionableAndNonVersionableId";
        Record record = generateRecord(recordId, new String[] { "field1", "value1", "false" }, new String[] { "field2",
                "value2", "true" });
        repository.create(record);
        Record actualRecord = repository.read(recordId);
        assertEquals(record, actualRecord);
    }

    @Test
    public void testReadVersionableAndNonVersionableField() throws Exception {
        String recordId = "readVersionableAndNonVersionableFieldId";
        Record record = generateRecord(recordId, new String[] { "field1", "value1", "false" }, new String[] { "field2",
                "value2", "true" });
        repository.create(record);
        Set<String> versionableSet = new HashSet<String>();
        versionableSet.add("field2");
        Set<String> nonVersionableSet = new HashSet<String>();
        nonVersionableSet.add("field1");
        Record actualRecord = repository.read(recordId, versionableSet, nonVersionableSet);
        assertEquals(record, actualRecord);
    }

    @Test
    public void testVersionableField() throws Exception {
        String recordId = "versionableFieldId";
        Record recordVersion1 = generateRecord(recordId, new String[] { "versionableField1", "value1", "true" });
        repository.create(recordVersion1);
        Record recordVersion2 = generateRecord(recordId, new String[] { "versionableField1", "value2", "true" });
        repository.update(recordVersion2);
        Record actualRecord = repository.read(recordId);
        assertEquals("value2", new String(actualRecord.getField("versionableField1").getValue()));
        actualRecord = repository.read(recordId, 1);
        assertEquals(recordVersion1, actualRecord);
    }

    @Test
    public void testVersionableFields() throws Exception {
        String recordId = "versionableFieldsId";
        Record recordVersion1 = generateRecord(recordId, 
                        new String[] { "versionableField1", "f1value1", "true" },
                        new String[] { "versionableField2", "f2value1", "true" }, 
                        new String[] { "versionableField3", "f3value1", "true" });
        repository.create(recordVersion1);
        Record update1 = generateRecord(recordId, 
                        new String[] { "versionableField1", "f1value2", "true" }, 
                        new String[] { "versionableField2", "f2value2", "true" });
        repository.update(update1);
        Record update2 = generateRecord(recordId, new String[] { "versionableField1", "f1value3", "true" });
        repository.update(update2);
        
        Record expectedRecord = generateRecord(recordId,
                        new String[] { "versionableField1", "f1value3", "true" },
                        new String[] { "versionableField2", "f2value2", "true" }, 
                        new String[] { "versionableField3", "f3value1", "true" });
        Record actualRecord = repository.read(recordId);
        assertEquals(expectedRecord, actualRecord);
        
        actualRecord = repository.read(recordId, 3);
        assertEquals(expectedRecord, actualRecord);
        
        expectedRecord = generateRecord(recordId,
                        new String[] { "versionableField1", "f1value2", "true" },
                        new String[] { "versionableField2", "f2value2", "true" }, 
                        new String[] { "versionableField3", "f3value1", "true" });
        actualRecord = repository.read(recordId, 2);
        assertEquals(expectedRecord, actualRecord);
        
        expectedRecord = generateRecord(recordId,
                        new String[] { "versionableField1", "f1value1", "true" },
                        new String[] { "versionableField2", "f2value1", "true" }, 
                        new String[] { "versionableField3", "f3value1", "true" });
        actualRecord = repository.read(recordId, 1);
        assertEquals(expectedRecord, actualRecord);
    }

    private Record generateRecord(String recordId, String[]... fieldsAndValues) {
        Record record = new Record(recordId);
        for (String[] fieldInfo : fieldsAndValues) {
            record.addField(new Field(fieldInfo[0], Bytes.toBytes(fieldInfo[1]), Boolean.valueOf(fieldInfo[2])));
        }
        return record;
    }

}
