package org.lilycms.repository.impl.test;

import static org.junit.Assert.*;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilycms.repository.api.FieldDescriptor;
import org.lilycms.repository.api.RecordType;
import org.lilycms.repository.impl.FieldDescriptorImpl;
import org.lilycms.repository.impl.HBaseTypeManager;
import org.lilycms.repository.impl.RecordTypeImpl;
import org.lilycms.testfw.TestHelper;

public class HBaseTypeManagerTest {

    private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    
    private HBaseTypeManager typeManager;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging();
        TEST_UTIL.startMiniCluster(1);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        TEST_UTIL.shutdownMiniCluster();
    }


    @Before
    public void setUp() throws Exception {
        typeManager = new HBaseTypeManager(TEST_UTIL.getConfiguration());
    }

    @After
    public void tearDown() throws Exception {
    }


    @Test
    public void testCreateEmptyRecordType() throws Exception {
        String recordTypeId = "createEmptyRecordTypeId";
        RecordType recordType = new RecordTypeImpl(recordTypeId);
        typeManager.createRecordType(recordType);
        RecordType actualRecordType = typeManager.getRecordType(recordTypeId);
        assertEquals(recordTypeId, actualRecordType.getRecordTypeId());
        assertEquals(1, actualRecordType.getVersion());
        assertTrue(actualRecordType.getFieldDescriptors().isEmpty());
    }
    
    @Test
    public void testCreateRecordType() throws Exception {
        String recordTypeId = "createRecordTypeId";
        RecordType recordType = new RecordTypeImpl(recordTypeId);
        String fieldDescriptorId = "fieldDescriptorId";
        FieldDescriptorImpl fieldDescriptor = new FieldDescriptorImpl(fieldDescriptorId, "dummyFieldType", true, false);
        recordType.addFieldDescriptor(fieldDescriptor);
        typeManager.createRecordType(recordType);
        
        RecordType actualRecordType = typeManager.getRecordType(recordTypeId);
        fieldDescriptor.setVersion(1);
        assertEquals(fieldDescriptor, actualRecordType.getFieldDescriptor(fieldDescriptorId));
    }

    @Test
    public void testCreateRecordTypeMultipleFieldDescriptors() throws Exception {
        String recordTypeId = "createRecordTypeMultipleFieldDescriptorsId";
        RecordType recordType = new RecordTypeImpl(recordTypeId);
        String fieldDescriptorId1 = "fieldDescriptorId1";
        FieldDescriptorImpl fieldDescriptor1 = new FieldDescriptorImpl(fieldDescriptorId1, "dummyFieldType", true, false);
        recordType.addFieldDescriptor(fieldDescriptor1);
        String fieldDescriptorId2 = "fieldDescriptorId2";
        FieldDescriptorImpl fieldDescriptor2 = new FieldDescriptorImpl(fieldDescriptorId2, "dummyFieldType2", false, true);
        recordType.addFieldDescriptor(fieldDescriptor2);
        
        typeManager.createRecordType(recordType);
        
        RecordType actualRecordType = typeManager.getRecordType(recordTypeId);
        assertEquals(2, actualRecordType.getFieldDescriptors().size());
        fieldDescriptor1.setVersion(1);
        assertEquals(fieldDescriptor1, actualRecordType.getFieldDescriptor(fieldDescriptorId1));
        fieldDescriptor2.setVersion(1);
        assertEquals(fieldDescriptor2, actualRecordType.getFieldDescriptor(fieldDescriptorId2));
    }
    
    @Test
    public void testUpdateRecordTypeAddFieldDescriptor() throws Exception {
        String recordTypeId = "updateRecordTypeAddFieldDescriptorId";
        RecordType recordType = new RecordTypeImpl(recordTypeId);
        typeManager.createRecordType(recordType);
        
        String fieldDescriptorId = "fieldDescriptorId";
        FieldDescriptorImpl fieldDescriptor = new FieldDescriptorImpl(fieldDescriptorId, "dummyFieldType", true, false);
        recordType.addFieldDescriptor(fieldDescriptor);
        
        typeManager.updateRecordType(recordType);
        
        RecordType actualRecordType = typeManager.getRecordType(recordTypeId);
        assertEquals(2, actualRecordType.getVersion());
        assertEquals(1, actualRecordType.getFieldDescriptors().size());
        
        fieldDescriptor.setVersion(1);
        assertEquals(fieldDescriptor, actualRecordType.getFieldDescriptor(fieldDescriptorId));
    }
    
    @Test
    public void testUpdateRecordTypeRemoveFieldDescriptor() throws Exception {
        String recordTypeId = "updateRecordTypeRemoveFieldDescriptorId";
        RecordType recordType = new RecordTypeImpl(recordTypeId);
        
        String fieldDescriptorId = "fieldDescriptorId";
        FieldDescriptorImpl fieldDescriptor = new FieldDescriptorImpl(fieldDescriptorId, "dummyFieldType", true, false);
        recordType.addFieldDescriptor(fieldDescriptor);

        typeManager.createRecordType(recordType);
        
        RecordType recordTypeWithoutFieldDescriptor = new RecordTypeImpl(recordTypeId);
        typeManager.updateRecordType(recordTypeWithoutFieldDescriptor);
        
        RecordType actualRecordType = typeManager.getRecordType(recordTypeId);
        assertEquals(2, actualRecordType.getVersion());
        assertEquals(0, actualRecordType.getFieldDescriptors().size());
    }
    
    @Test
    public void testUpdateRecordTypeUpdateFieldDescriptor() throws Exception {
        String recordTypeId = "updateRecordTypeUpdateFieldDescriptorId";
        RecordType recordType = new RecordTypeImpl(recordTypeId);
        
        String fieldDescriptorId = "fieldDescriptorId";
        FieldDescriptorImpl fieldDescriptor = new FieldDescriptorImpl(fieldDescriptorId, "dummyFieldType", true, false);
        recordType.addFieldDescriptor(fieldDescriptor);
        typeManager.createRecordType(recordType);
        
        FieldDescriptorImpl changedFieldDescriptor = new FieldDescriptorImpl(fieldDescriptorId, "changedFieldType", false, true); 
        recordType.addFieldDescriptor(changedFieldDescriptor);
        typeManager.updateRecordType(recordType);
        
        RecordType actualRecordType = typeManager.getRecordType(recordTypeId);
        assertEquals(2, actualRecordType.getVersion());
        assertEquals(1, actualRecordType.getFieldDescriptors().size());
        FieldDescriptor actualFieldDescriptor = actualRecordType.getFieldDescriptor(fieldDescriptorId);
        assertEquals(2, actualFieldDescriptor.getVersion());
        assertEquals("changedFieldType", actualFieldDescriptor.getFieldType());
        assertFalse(actualFieldDescriptor.isMandatory());
        assertTrue(actualFieldDescriptor.isVersionable());
        
    }
    
    @Test
    public void testUpdatePartOfFieldDescriptor() throws Exception {
        String recordTypeId = "updatePartOfFieldDescriptorId";
        RecordType recordType = new RecordTypeImpl(recordTypeId);
        
        String fieldDescriptorId = "updatePartFieldDescriptorId";
        FieldDescriptorImpl fieldDescriptor = new FieldDescriptorImpl(fieldDescriptorId, "dummyFieldType", true, false);
        recordType.addFieldDescriptor(fieldDescriptor);
        typeManager.createRecordType(recordType);
        
        FieldDescriptorImpl changedFieldDescriptor = new FieldDescriptorImpl(fieldDescriptorId, "changedFieldType", true, false); 
        recordType.addFieldDescriptor(changedFieldDescriptor);
        typeManager.updateRecordType(recordType);
        
        RecordType actualRecordType = typeManager.getRecordType(recordTypeId);
        assertEquals(2, actualRecordType.getVersion());
        assertEquals(1, actualRecordType.getFieldDescriptors().size());
        FieldDescriptor actualFieldDescriptor = actualRecordType.getFieldDescriptor(fieldDescriptorId);
        assertEquals(2, actualFieldDescriptor.getVersion());
        assertEquals("changedFieldType", actualFieldDescriptor.getFieldType());
        assertTrue(actualFieldDescriptor.isMandatory());
        assertFalse(actualFieldDescriptor.isVersionable());
    }
    
    @Test
    public void testNoUpdateNeeded() throws Exception {
        String recordTypeId = "noUpdateNeededRecordTypeId";
        RecordType recordType = new RecordTypeImpl(recordTypeId);
        
        String fieldDescriptorId = "noUpdateNeededFieldDescriptorId";
        FieldDescriptorImpl fieldDescriptor = new FieldDescriptorImpl(fieldDescriptorId, "dummyFieldType", true, false);
        recordType.addFieldDescriptor(fieldDescriptor);
        typeManager.createRecordType(recordType);
        
        typeManager.updateRecordType(recordType);
        
        RecordType actualRecordType = typeManager.getRecordType(recordTypeId);
        assertEquals(1, actualRecordType.getVersion());
        assertEquals(1, actualRecordType.getFieldDescriptors().size());
        FieldDescriptor actualFieldDescriptor = actualRecordType.getFieldDescriptor(fieldDescriptorId);
        assertEquals(1, actualFieldDescriptor.getVersion());
        assertEquals("dummyFieldType", actualFieldDescriptor.getFieldType());
        assertTrue(actualFieldDescriptor.isMandatory());
        assertFalse(actualFieldDescriptor.isVersionable());
    }
    
    @Test
    public void testUpdateRecordTypeRemoveAddAndUpdateFieldDescriptor() throws Exception {
        String recordTypeId = "updateRecordTypeUpdateFieldDescriptorId";
        RecordType recordType = new RecordTypeImpl(recordTypeId);
        
        String removeFieldDescriptorId = "removeFieldDescriptorId";
        FieldDescriptorImpl removeFieldDescriptor = new FieldDescriptorImpl(removeFieldDescriptorId, "dummyFieldType", true, false);
        recordType.addFieldDescriptor(removeFieldDescriptor);
        String updateFieldDescriptorId = "updateFieldDescriptorId";
        FieldDescriptorImpl updateFieldDescriptor = new FieldDescriptorImpl(updateFieldDescriptorId, "dummyFieldType", true, false);
        recordType.addFieldDescriptor(updateFieldDescriptor);
        typeManager.createRecordType(recordType);
        
        recordType.removeFieldDescriptor(removeFieldDescriptorId);
        FieldDescriptorImpl changedFieldDescriptor = new FieldDescriptorImpl(updateFieldDescriptorId, "changedFieldType", false, true); 
        recordType.addFieldDescriptor(changedFieldDescriptor);
        String addFieldDescriptorId = "addFieldDescriptorId";
        FieldDescriptorImpl addFieldDescriptor = new FieldDescriptorImpl(addFieldDescriptorId, "addFieldType", true, false);
        recordType.addFieldDescriptor(addFieldDescriptor);
        typeManager.updateRecordType(recordType);
        
        RecordType actualRecordType = typeManager.getRecordType(recordTypeId);
        assertEquals(2, actualRecordType.getVersion());
        assertEquals(2, actualRecordType.getFieldDescriptors().size());
        
        FieldDescriptor actualFieldDescriptor = actualRecordType.getFieldDescriptor(updateFieldDescriptorId);
        assertEquals(2, actualFieldDescriptor.getVersion());
        assertEquals("changedFieldType", actualFieldDescriptor.getFieldType());
        assertFalse(actualFieldDescriptor.isMandatory());
        assertTrue(actualFieldDescriptor.isVersionable());

        actualFieldDescriptor = actualRecordType.getFieldDescriptor(addFieldDescriptorId);
        assertEquals(1, actualFieldDescriptor.getVersion());
        
        assertNull(actualRecordType.getFieldDescriptor(removeFieldDescriptorId));
    }
    
    @Test
    public void testReadOldVersionRecordType() throws Exception {
        String recordTypeId = "readOldVersionRecordTypeId";
        RecordType recordType = new RecordTypeImpl(recordTypeId);
        
        String fieldDescriptorId = "oldVersionFieldDescriptorId";
        FieldDescriptorImpl updateFieldDescriptor = new FieldDescriptorImpl(fieldDescriptorId, "dummyFieldType", true, false);
        recordType.addFieldDescriptor(updateFieldDescriptor);
        typeManager.createRecordType(recordType);
        
        FieldDescriptorImpl changedFieldDescriptor = new FieldDescriptorImpl(fieldDescriptorId, "changedFieldType", false, true); 
        recordType.addFieldDescriptor(changedFieldDescriptor);
        typeManager.updateRecordType(recordType);
        
        RecordType actualRecordType = typeManager.getRecordType(recordTypeId, 1);
        assertEquals(1, actualRecordType.getVersion());
        FieldDescriptor actualFieldDescriptor = actualRecordType.getFieldDescriptor(fieldDescriptorId);
        updateFieldDescriptor.setVersion(1);
        assertEquals(updateFieldDescriptor, actualFieldDescriptor);
    }
    
    @Test
    public void testRecordTypeVersionsAndDeletedFieldDescriptors() throws Exception {
        String recordTypeId = "recordTypeVersionsAndDeletedFieldDescriptors";
        RecordType recordType = new RecordTypeImpl(recordTypeId);
        
        String fdId1 = "fdId1";
        FieldDescriptorImpl fd1 = new FieldDescriptorImpl(fdId1, "ft1", true, false);
        recordType.addFieldDescriptor(fd1);
        String fdId2 = "fdId2";
        FieldDescriptorImpl fd2 = new FieldDescriptorImpl(fdId2, "ft2", true, false);
        recordType.addFieldDescriptor(fd2);
        typeManager.createRecordType(recordType);

        recordType = new RecordTypeImpl(recordTypeId);
        recordType.addFieldDescriptor(fd1);
        fd2 = new FieldDescriptorImpl(fdId2, "ft2", false, true);
        recordType.addFieldDescriptor(fd2);
        String fdId3 = "fdId3";
        FieldDescriptorImpl fd3 = new FieldDescriptorImpl(fdId3, "ft3", false, true);
        recordType.addFieldDescriptor(fd3);
        typeManager.updateRecordType(recordType);
        
        recordType = new RecordTypeImpl(recordTypeId);
        fd1 = new FieldDescriptorImpl(fdId1, "ft11", true, false);
        recordType.addFieldDescriptor(fd1);
        fd3 = new FieldDescriptorImpl(fdId3, "ft33", false, true);
        recordType.addFieldDescriptor(fd3);
        typeManager.updateRecordType(recordType);
        
        RecordType actualRecordType = typeManager.getRecordType(recordTypeId, 1);
        assertEquals(2, actualRecordType.getFieldDescriptors().size());
        assertEquals(1, actualRecordType.getFieldDescriptor(fdId1).getVersion());
        assertEquals("ft1", actualRecordType.getFieldDescriptor(fdId1).getFieldType());
        assertEquals(1, actualRecordType.getFieldDescriptor(fdId2).getVersion());
        assertTrue(actualRecordType.getFieldDescriptor(fdId2).isMandatory());
        assertFalse(actualRecordType.getFieldDescriptor(fdId2).isVersionable());
        assertNull(actualRecordType.getFieldDescriptor(fdId3));
        
        actualRecordType = typeManager.getRecordType(recordTypeId, 2);
        assertEquals(3, actualRecordType.getFieldDescriptors().size());
        assertEquals(1, actualRecordType.getFieldDescriptor(fdId1).getVersion());
        assertEquals("ft1", actualRecordType.getFieldDescriptor(fdId1).getFieldType());
        assertEquals(2, actualRecordType.getFieldDescriptor(fdId2).getVersion());
        assertFalse(actualRecordType.getFieldDescriptor(fdId2).isMandatory());
        assertTrue(actualRecordType.getFieldDescriptor(fdId2).isVersionable());
        assertEquals(1, actualRecordType.getFieldDescriptor(fdId3).getVersion());
        assertEquals("ft3", actualRecordType.getFieldDescriptor(fdId3).getFieldType());
        
        actualRecordType = typeManager.getRecordType(recordTypeId, 3);
        assertEquals(2, actualRecordType.getFieldDescriptors().size());
        assertEquals(2, actualRecordType.getFieldDescriptor(fdId1).getVersion());
        assertEquals("ft11", actualRecordType.getFieldDescriptor(fdId1).getFieldType());
        assertNull(actualRecordType.getFieldDescriptor(fdId2));
        assertEquals(2, actualRecordType.getFieldDescriptor(fdId3).getVersion());
        assertEquals("ft33", actualRecordType.getFieldDescriptor(fdId3).getFieldType());
    }
}
