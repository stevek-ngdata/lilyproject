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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilycms.repository.api.FieldDescriptor;
import org.lilycms.repository.api.RecordType;
import org.lilycms.repository.api.ValueType;
import org.lilycms.repository.impl.FieldDescriptorImpl;
import org.lilycms.repository.impl.HBaseTypeManager;
import org.lilycms.repository.impl.IdGeneratorImpl;
import org.lilycms.repository.impl.RecordTypeImpl;
import org.lilycms.testfw.TestHelper;

public class HBaseTypeManagerTest {

    private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    
    private HBaseTypeManager typeManager;

    private ValueType valueType1;
    private ValueType valueType2;

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
        typeManager = new HBaseTypeManager(new IdGeneratorImpl(), RecordTypeImpl.class, FieldDescriptorImpl.class, TEST_UTIL.getConfiguration());
        valueType1 = typeManager.getValueType("STRING", false, false);
        valueType2 = typeManager.getValueType("INTEGER", false, false);
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testCreateEmptyRecordType() throws Exception {
        String recordTypeId = "createEmptyRecordTypeId";
        RecordType recordType = typeManager.newRecordType(recordTypeId);
        typeManager.createRecordType(recordType);
        RecordType actualRecordType = typeManager.getRecordType(recordTypeId);
        assertEquals(recordTypeId, actualRecordType.getId());
        assertEquals(Long.valueOf(1), actualRecordType.getVersion());
        assertTrue(actualRecordType.getFieldDescriptors().isEmpty());
    }
    
    @Test
    public void testCreateRecordType() throws Exception {
        String recordTypeId = "createRecordTypeId";
        RecordType recordType = typeManager.newRecordType(recordTypeId);
        String fieldDescriptorId = "fieldDescriptorId";
        FieldDescriptor fieldDescriptor = typeManager.newFieldDescriptor(fieldDescriptorId, typeManager.getValueType("STRING", false, false), true, false);
        recordType.addFieldDescriptor(fieldDescriptor);
        typeManager.createRecordType(recordType);
        
        RecordType actualRecordType = typeManager.getRecordType(recordTypeId);
        fieldDescriptor = typeManager.newFieldDescriptor(fieldDescriptorId, Long.valueOf(1), typeManager.getValueType("STRING", false, false), true, false);
        assertEquals(fieldDescriptor, actualRecordType.getFieldDescriptor(fieldDescriptorId));
    }

    @Test
    public void testCreateRecordTypeMultipleFieldDescriptors() throws Exception {
        String recordTypeId = "createRecordTypeMultipleFieldDescriptorsId";
        RecordType recordType = typeManager.newRecordType(recordTypeId);
        String fieldDescriptorId1 = "fieldDescriptorId1";
        FieldDescriptor fieldDescriptor1 = typeManager.newFieldDescriptor(fieldDescriptorId1, valueType1, true, false);
        recordType.addFieldDescriptor(fieldDescriptor1);
        String fieldDescriptorId2 = "fieldDescriptorId2";
        FieldDescriptor fieldDescriptor2 = typeManager.newFieldDescriptor(fieldDescriptorId2, valueType2, false, true);
        recordType.addFieldDescriptor(fieldDescriptor2);
        
        typeManager.createRecordType(recordType);
        
        RecordType actualRecordType = typeManager.getRecordType(recordTypeId);
        assertEquals(2, actualRecordType.getFieldDescriptors().size());
        fieldDescriptor1 = typeManager.newFieldDescriptor(fieldDescriptorId1, Long.valueOf(1), valueType1, true, false);
        assertEquals(fieldDescriptor1, actualRecordType.getFieldDescriptor(fieldDescriptorId1));
        fieldDescriptor2 = typeManager.newFieldDescriptor(fieldDescriptorId2, Long.valueOf(1), valueType2, false, true);
        assertEquals(fieldDescriptor2, actualRecordType.getFieldDescriptor(fieldDescriptorId2));
    }
    
    @Test
    public void testUpdateRecordTypeAddFieldDescriptor() throws Exception {
        String recordTypeId = "updateRecordTypeAddFieldDescriptorId";
        RecordType recordType = typeManager.newRecordType(recordTypeId);
        typeManager.createRecordType(recordType);
        
        String fieldDescriptorId = "updateRecordTypeAddFieldDescriptorFieldDescriptorId";
        FieldDescriptor fieldDescriptor = typeManager.newFieldDescriptor(fieldDescriptorId, valueType1, true, false);
        recordType.addFieldDescriptor(fieldDescriptor);
        
        typeManager.updateRecordType(recordType);
        
        RecordType actualRecordType = typeManager.getRecordType(recordTypeId);
        assertEquals(Long.valueOf(2), actualRecordType.getVersion());
        assertEquals(1, actualRecordType.getFieldDescriptors().size());
        
        fieldDescriptor = typeManager.newFieldDescriptor(fieldDescriptorId, Long.valueOf(1), valueType1, true, false);
        assertEquals(fieldDescriptor, actualRecordType.getFieldDescriptor(fieldDescriptorId));
    }
    
    @Test
    public void testUpdateRecordTypeRemoveFieldDescriptor() throws Exception {
        String recordTypeId = "updateRecordTypeRemoveFieldDescriptorId";
        RecordType recordType = typeManager.newRecordType(recordTypeId);
        
        String fieldDescriptorId = "fieldDescriptorId";
        FieldDescriptor fieldDescriptor = typeManager.newFieldDescriptor(fieldDescriptorId, valueType1, true, false);
        recordType.addFieldDescriptor(fieldDescriptor);

        typeManager.createRecordType(recordType);
        
        RecordType recordTypeWithoutFieldDescriptor = typeManager.newRecordType(recordTypeId);
        typeManager.updateRecordType(recordTypeWithoutFieldDescriptor);
        
        RecordType actualRecordType = typeManager.getRecordType(recordTypeId);
        assertEquals(Long.valueOf(2), actualRecordType.getVersion());
        assertEquals(0, actualRecordType.getFieldDescriptors().size());
    }
    
    @Test
    public void testUpdateRecordTypeUpdateFieldDescriptor() throws Exception {
        String recordTypeId = "updateRecordTypeUpdateFieldDescriptorId";
        RecordType recordType = typeManager.newRecordType(recordTypeId);
        
        String fieldDescriptorId = "updateRecordTypeUpdateFieldDescriptorFieldDescriptorId";
        FieldDescriptor fieldDescriptor = typeManager.newFieldDescriptor(fieldDescriptorId, valueType1, true, false);
        recordType.addFieldDescriptor(fieldDescriptor);
        typeManager.createRecordType(recordType);
        
        FieldDescriptor changedFieldDescriptor = typeManager.newFieldDescriptor(fieldDescriptorId, valueType2, false, true); 
        recordType.addFieldDescriptor(changedFieldDescriptor);
        typeManager.updateRecordType(recordType);
        
        RecordType actualRecordType = typeManager.getRecordType(recordTypeId);
        assertEquals(Long.valueOf(2), actualRecordType.getVersion());
        assertEquals(1, actualRecordType.getFieldDescriptors().size());
        FieldDescriptor actualFieldDescriptor = actualRecordType.getFieldDescriptor(fieldDescriptorId);
        assertEquals(Long.valueOf(2), actualFieldDescriptor.getVersion());
        assertEquals(valueType2, actualFieldDescriptor.getValueType());
        assertFalse(actualFieldDescriptor.isMandatory());
        assertTrue(actualFieldDescriptor.isVersionable());
        
    }
    
    @Test
    public void testUpdatePartOfFieldDescriptor() throws Exception {
        String recordTypeId = "updatePartOfFieldDescriptorId";
        RecordType recordType = typeManager.newRecordType(recordTypeId);
        
        String fieldDescriptorId = "updatePartFieldDescriptorId";
        FieldDescriptor fieldDescriptor = typeManager.newFieldDescriptor(fieldDescriptorId, valueType1, true, false);
        recordType.addFieldDescriptor(fieldDescriptor);
        typeManager.createRecordType(recordType);
        
        FieldDescriptor changedFieldDescriptor = typeManager.newFieldDescriptor(fieldDescriptorId, valueType2, true, false); 
        recordType.addFieldDescriptor(changedFieldDescriptor);
        typeManager.updateRecordType(recordType);
        
        RecordType actualRecordType = typeManager.getRecordType(recordTypeId);
        assertEquals(Long.valueOf(2), actualRecordType.getVersion());
        assertEquals(1, actualRecordType.getFieldDescriptors().size());
        FieldDescriptor actualFieldDescriptor = actualRecordType.getFieldDescriptor(fieldDescriptorId);
        assertEquals(Long.valueOf(2), actualFieldDescriptor.getVersion());
        assertEquals(valueType2, actualFieldDescriptor.getValueType());
        assertTrue(actualFieldDescriptor.isMandatory());
        assertFalse(actualFieldDescriptor.isVersionable());
    }
    
    @Test
    public void testNoUpdateNeeded() throws Exception {
        String recordTypeId = "noUpdateNeededRecordTypeId";
        RecordType recordType = typeManager.newRecordType(recordTypeId);
        
        String fieldDescriptorId = "noUpdateNeededFieldDescriptorId";
        FieldDescriptor fieldDescriptor = typeManager.newFieldDescriptor(fieldDescriptorId, valueType1, true, false);
        recordType.addFieldDescriptor(fieldDescriptor);
        typeManager.createRecordType(recordType);
        
        typeManager.updateRecordType(recordType);
        
        RecordType actualRecordType = typeManager.getRecordType(recordTypeId);
        assertEquals(Long.valueOf(1), actualRecordType.getVersion());
        assertEquals(1, actualRecordType.getFieldDescriptors().size());
        FieldDescriptor actualFieldDescriptor = actualRecordType.getFieldDescriptor(fieldDescriptorId);
        assertEquals(Long.valueOf(1), actualFieldDescriptor.getVersion());
        assertEquals(valueType1, actualFieldDescriptor.getValueType());
        assertTrue(actualFieldDescriptor.isMandatory());
        assertFalse(actualFieldDescriptor.isVersionable());
    }
    
    @Test
    public void testUpdateRecordTypeRemoveAddAndUpdateFieldDescriptor() throws Exception {
        String recordTypeId = "updateRecordTypeUpdateFieldDescriptorId";
        RecordType recordType = typeManager.newRecordType(recordTypeId);
        
        String removeFieldDescriptorId = "removeFieldDescriptorId";
        FieldDescriptor removeFieldDescriptor = typeManager.newFieldDescriptor(removeFieldDescriptorId, valueType1, true, false);
        recordType.addFieldDescriptor(removeFieldDescriptor);
        String updateFieldDescriptorId = "updateFieldDescriptorId";
        FieldDescriptor updateFieldDescriptor = typeManager.newFieldDescriptor(updateFieldDescriptorId, valueType1, true, false);
        recordType.addFieldDescriptor(updateFieldDescriptor);
        typeManager.createRecordType(recordType);
        
        recordType.removeFieldDescriptor(removeFieldDescriptorId);
        FieldDescriptor changedFieldDescriptor = typeManager.newFieldDescriptor(updateFieldDescriptorId, valueType2, false, true); 
        recordType.addFieldDescriptor(changedFieldDescriptor);
        String addFieldDescriptorId = "addFieldDescriptorId";
        FieldDescriptor addFieldDescriptor = typeManager.newFieldDescriptor(addFieldDescriptorId, valueType1, true, false);
        recordType.addFieldDescriptor(addFieldDescriptor);
        typeManager.updateRecordType(recordType);
        
        RecordType actualRecordType = typeManager.getRecordType(recordTypeId);
        assertEquals(Long.valueOf(2), actualRecordType.getVersion());
        assertEquals(2, actualRecordType.getFieldDescriptors().size());
        
        FieldDescriptor actualFieldDescriptor = actualRecordType.getFieldDescriptor(updateFieldDescriptorId);
        assertEquals(Long.valueOf(2), actualFieldDescriptor.getVersion());
        assertEquals(valueType2, actualFieldDescriptor.getValueType());
        assertFalse(actualFieldDescriptor.isMandatory());
        assertTrue(actualFieldDescriptor.isVersionable());

        actualFieldDescriptor = actualRecordType.getFieldDescriptor(addFieldDescriptorId);
        assertEquals(Long.valueOf(1), actualFieldDescriptor.getVersion());
        
        assertNull(actualRecordType.getFieldDescriptor(removeFieldDescriptorId));
    }
    
    @Test
    public void testReadOldVersionRecordType() throws Exception {
        String recordTypeId = "readOldVersionRecordTypeId";
        RecordType recordType = typeManager.newRecordType(recordTypeId);
        
        String fieldDescriptorId = "oldVersionFieldDescriptorId";
        FieldDescriptor updateFieldDescriptor = typeManager.newFieldDescriptor(fieldDescriptorId, valueType1, true, false);
        recordType.addFieldDescriptor(updateFieldDescriptor);
        typeManager.createRecordType(recordType);
        
        FieldDescriptor changedFieldDescriptor = typeManager.newFieldDescriptor(fieldDescriptorId, valueType2, false, true); 
        recordType.addFieldDescriptor(changedFieldDescriptor);
        typeManager.updateRecordType(recordType);
        
        RecordType actualRecordType = typeManager.getRecordType(recordTypeId, Long.valueOf(1));
        assertEquals(Long.valueOf(1), actualRecordType.getVersion());
        FieldDescriptor actualFieldDescriptor = actualRecordType.getFieldDescriptor(fieldDescriptorId);
        updateFieldDescriptor = typeManager.newFieldDescriptor(fieldDescriptorId, Long.valueOf(1), valueType1, true, false);
        assertEquals(updateFieldDescriptor, actualFieldDescriptor);
    }
    
    @Test
    public void testRecordTypeVersionsAndDeletedFieldDescriptors() throws Exception {
        String recordTypeId = "recordTypeVersionsAndDeletedFieldDescriptors";
        RecordType recordType = typeManager.newRecordType(recordTypeId);
        
        // Create record type with field descriptor fdId1 and fdId2
        String fdId1 = "fdId1";
        FieldDescriptor fd1 = typeManager.newFieldDescriptor(fdId1, valueType1, true, false);
        recordType.addFieldDescriptor(fd1);
        String fdId2 = "fdId2";
        FieldDescriptor fd2 = typeManager.newFieldDescriptor(fdId2, valueType2, true, false);
        recordType.addFieldDescriptor(fd2);
        typeManager.createRecordType(recordType);

        // Change fdId2, add fdId3
        recordType = typeManager.newRecordType(recordTypeId);
        recordType.addFieldDescriptor(fd1);
        fd2 = typeManager.newFieldDescriptor(fdId2, valueType2, false, true);
        recordType.addFieldDescriptor(fd2);
        String fdId3 = "fdId3";
        FieldDescriptor fd3 = typeManager.newFieldDescriptor(fdId3, valueType1, false, true);
        recordType.addFieldDescriptor(fd3);
        typeManager.updateRecordType(recordType);
        
        // Delete fdId2, change fdId3
        recordType = typeManager.newRecordType(recordTypeId);
        fd1 = typeManager.newFieldDescriptor(fdId1, valueType2, true, false);
        recordType.addFieldDescriptor(fd1);
        fd3 = typeManager.newFieldDescriptor(fdId3, valueType2, false, true);
        recordType.addFieldDescriptor(fd3);
        typeManager.updateRecordType(recordType);
        
        // Add fdId2 again
        recordType = typeManager.newRecordType(recordTypeId);
        recordType.addFieldDescriptor(fd1);
        recordType.addFieldDescriptor(fd2);
        recordType.addFieldDescriptor(fd3);
        typeManager.updateRecordType(recordType);
        
        RecordType actualRecordType = typeManager.getRecordType(recordTypeId, Long.valueOf(1));
        assertEquals(2, actualRecordType.getFieldDescriptors().size());
        assertEquals(Long.valueOf(1), actualRecordType.getFieldDescriptor(fdId1).getVersion());
        assertEquals(valueType1, actualRecordType.getFieldDescriptor(fdId1).getValueType());
        assertEquals(Long.valueOf(1), actualRecordType.getFieldDescriptor(fdId2).getVersion());
        assertTrue(actualRecordType.getFieldDescriptor(fdId2).isMandatory());
        assertFalse(actualRecordType.getFieldDescriptor(fdId2).isVersionable());
        assertNull(actualRecordType.getFieldDescriptor(fdId3));
        
        actualRecordType = typeManager.getRecordType(recordTypeId, Long.valueOf(2));
        assertEquals(3, actualRecordType.getFieldDescriptors().size());
        assertEquals(Long.valueOf(1), actualRecordType.getFieldDescriptor(fdId1).getVersion());
        assertEquals(valueType1, actualRecordType.getFieldDescriptor(fdId1).getValueType());
        assertEquals(Long.valueOf(2), actualRecordType.getFieldDescriptor(fdId2).getVersion());
        assertFalse(actualRecordType.getFieldDescriptor(fdId2).isMandatory());
        assertTrue(actualRecordType.getFieldDescriptor(fdId2).isVersionable());
        assertEquals(Long.valueOf(1), actualRecordType.getFieldDescriptor(fdId3).getVersion());
        assertEquals(valueType1, actualRecordType.getFieldDescriptor(fdId3).getValueType());
        
        actualRecordType = typeManager.getRecordType(recordTypeId, Long.valueOf(3));
        assertEquals(2, actualRecordType.getFieldDescriptors().size());
        assertEquals(Long.valueOf(2), actualRecordType.getFieldDescriptor(fdId1).getVersion());
        assertEquals(valueType2, actualRecordType.getFieldDescriptor(fdId1).getValueType());
        assertNull(actualRecordType.getFieldDescriptor(fdId2));
        assertEquals(Long.valueOf(2), actualRecordType.getFieldDescriptor(fdId3).getVersion());
        assertEquals(valueType2, actualRecordType.getFieldDescriptor(fdId3).getValueType());
        
        actualRecordType = typeManager.getRecordType(recordTypeId, Long.valueOf(4));
        assertEquals(3, actualRecordType.getFieldDescriptors().size());
        assertEquals(Long.valueOf(2), actualRecordType.getFieldDescriptor(fdId1).getVersion());
        assertEquals(valueType2, actualRecordType.getFieldDescriptor(fdId1).getValueType());
        assertEquals(Long.valueOf(3), actualRecordType.getFieldDescriptor(fdId2).getVersion());
        assertFalse(actualRecordType.getFieldDescriptor(fdId2).isMandatory());
        assertTrue(actualRecordType.getFieldDescriptor(fdId2).isVersionable());
        assertEquals(Long.valueOf(2), actualRecordType.getFieldDescriptor(fdId3).getVersion());
        assertEquals(valueType2, actualRecordType.getFieldDescriptor(fdId3).getValueType());
    }
    
}
