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


import static junit.framework.Assert.*;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilycms.repository.api.FieldDescriptor;
import org.lilycms.repository.api.FieldGroup;
import org.lilycms.repository.api.FieldGroupNotFoundException;
import org.lilycms.repository.api.RecordType;
import org.lilycms.repository.api.RecordTypeNotFoundException;
import org.lilycms.repository.api.TypeManager;
import org.lilycms.repository.impl.HBaseTypeManager;
import org.lilycms.repository.impl.IdGeneratorImpl;
import org.lilycms.testfw.TestHelper;

/**
 *
 */
public class TypeManagerRecordTypeTest {

    private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    private static TypeManager typeManager;
    private static FieldDescriptor fieldDescriptor1;
    private static FieldDescriptor fieldDescriptor2;
    private static FieldDescriptor fieldDescriptor3;
    private static FieldGroup fieldGroup1;
    private static FieldGroup fieldGroup2;
    private static FieldGroup fieldGroup3;
    private static FieldGroup fieldGroup1B;
    private static FieldGroup fieldGroup2B;
    private static FieldGroup fieldGroup3B;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging();
        TEST_UTIL.startMiniCluster(1);
        typeManager = new HBaseTypeManager(new IdGeneratorImpl(), TEST_UTIL.getConfiguration());
        setupFieldDescriptors();
        setupFieldGroups();
    }
    
    private static void setupFieldDescriptors() throws Exception {
        fieldDescriptor1 = typeManager.createFieldDescriptor(typeManager.newFieldDescriptor("FD1", typeManager.getValueType("STRING", false, false), "GN1"));
        fieldDescriptor2 = typeManager.createFieldDescriptor(typeManager.newFieldDescriptor("FD2", typeManager.getValueType("INTEGER", false, false), "GN2"));
        fieldDescriptor3 = typeManager.createFieldDescriptor(typeManager.newFieldDescriptor("FD3", typeManager.getValueType("BOOLEAN", false, false), "GN3"));
    }
    
    private static void setupFieldGroups() throws Exception {
        FieldGroup fieldGroup = typeManager.newFieldGroup("FG1");
        fieldGroup.setFieldGroupEntry(typeManager.newFieldGroupEntry(fieldDescriptor1.getId(), fieldDescriptor1.getVersion(), false, "alias1"));
        fieldGroup1 = typeManager.createFieldGroup(fieldGroup);
        fieldGroup.setFieldGroupEntry(typeManager.newFieldGroupEntry(fieldDescriptor1.getId(), fieldDescriptor1.getVersion(), true, "alias1B"));
        fieldGroup1B = typeManager.updateFieldGroup(fieldGroup);
        
        fieldGroup = typeManager.newFieldGroup("FG2");
        fieldGroup.setFieldGroupEntry(typeManager.newFieldGroupEntry(fieldDescriptor2.getId(), fieldDescriptor2.getVersion(), false, "alias2"));
        fieldGroup2 = typeManager.createFieldGroup(fieldGroup);
        fieldGroup.setFieldGroupEntry(typeManager.newFieldGroupEntry(fieldDescriptor2.getId(), fieldDescriptor2.getVersion(), false, "alias2B"));
        fieldGroup2B = typeManager.updateFieldGroup(fieldGroup);

        fieldGroup = typeManager.newFieldGroup("FG3");
        fieldGroup.setFieldGroupEntry(typeManager.newFieldGroupEntry(fieldDescriptor3.getId(), fieldDescriptor3.getVersion(), false, "alias3"));
        fieldGroup3 = typeManager.createFieldGroup(fieldGroup);
        fieldGroup.setFieldGroupEntry(typeManager.newFieldGroupEntry(fieldDescriptor3.getId(), fieldDescriptor3.getVersion(), false, "alias3B"));
        fieldGroup3B = typeManager.updateFieldGroup(fieldGroup);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        TEST_UTIL.shutdownMiniCluster();
    }
 

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }
    
    @Test
    public void testCreateEmpty() throws Exception {
        String id = "testCreateEmpty";
        RecordType recordType = typeManager.newRecordType(id);
        assertEquals(Long.valueOf(1), typeManager.createRecordType(recordType).getVersion());
        
        recordType.setVersion(Long.valueOf(1));
        assertEquals(recordType, typeManager.getRecordType(id, null));
    }
    
    @Test
    public void testCreate() throws Exception {
        String id = "testCreate";
        RecordType recordType = typeManager.newRecordType(id);
        recordType.setNonVersionableFieldGroupId(fieldGroup1.getId());
        recordType.setNonVersionableFieldGroupVersion(fieldGroup1.getVersion());
        recordType.setVersionableFieldGroupId(fieldGroup2.getId());
        recordType.setVersionableFieldGroupVersion(fieldGroup2.getVersion());
        recordType.setVersionableMutableFieldGroupId(fieldGroup3.getId());
        recordType.setVersionableMutableFieldGroupVersion(fieldGroup3.getVersion());
        assertEquals(Long.valueOf(1), typeManager.createRecordType(recordType).getVersion());
        
        recordType.setVersion(Long.valueOf(1));
        assertEquals(recordType, typeManager.getRecordType(id, null));
    }

    @Test
    public void testUpdate() throws Exception {
        String id = "testUpdate";
        RecordType recordType = typeManager.newRecordType(id);
        RecordType recordTypeV1 = typeManager.createRecordType(recordType);
        assertEquals(Long.valueOf(1), recordTypeV1.getVersion());
        assertEquals(Long.valueOf(1), typeManager.updateRecordType(recordType).getVersion());
        
        recordType.setNonVersionableFieldGroupId(fieldGroup1.getId());
        recordType.setNonVersionableFieldGroupVersion(fieldGroup1.getVersion());
        RecordType recordTypeV2 = typeManager.updateRecordType(recordType);
        assertEquals(Long.valueOf(2), recordTypeV2.getVersion());
        assertEquals(Long.valueOf(2), typeManager.updateRecordType(recordType).getVersion());
        
        recordType.setVersionableFieldGroupId(fieldGroup2.getId());
        recordType.setVersionableFieldGroupVersion(fieldGroup2.getVersion());
        RecordType recordTypeV3 = typeManager.updateRecordType(recordType);
        assertEquals(Long.valueOf(3), recordTypeV3.getVersion());
        assertEquals(Long.valueOf(3), typeManager.updateRecordType(recordType).getVersion());

        recordType.setVersionableMutableFieldGroupId(fieldGroup3.getId());
        recordType.setVersionableMutableFieldGroupVersion(fieldGroup3.getVersion());
        RecordType recordTypeV4 = typeManager.updateRecordType(recordType);
        assertEquals(Long.valueOf(4), recordTypeV4.getVersion());
        assertEquals(Long.valueOf(4), typeManager.updateRecordType(recordType).getVersion());
        
        recordType.setVersion(Long.valueOf(4));
        assertEquals(recordType, typeManager.getRecordType(id, null));
        
        // Read old versions
        assertEquals(recordTypeV1, typeManager.getRecordType(id,Long.valueOf(1)));
        assertEquals(recordTypeV2, typeManager.getRecordType(id,Long.valueOf(2)));
        assertEquals(recordTypeV3, typeManager.getRecordType(id,Long.valueOf(3)));
        assertEquals(recordTypeV4, typeManager.getRecordType(id,Long.valueOf(4)));
    }
    
    @Test 
    public void testReadNonExistingRecordTypeFails() throws Exception {
        String id = "testReadNonExistingRecordTypeFails";
        try {
            typeManager.getRecordType(id, null);
            fail();
        } catch (RecordTypeNotFoundException expected) {
        }
        
        typeManager.createRecordType(typeManager.newRecordType(id));
        try {
            typeManager.getRecordType(id, Long.valueOf(2));
            fail();
        } catch (RecordTypeNotFoundException expected) {
        }
    }
    
    @Test 
    public void testUpdateNonExistingRecordTypeFails() throws Exception {
        String id = "testUpdateNonExistingRecordTypeFails";
        RecordType recordType = typeManager.newRecordType(id);
        try {
            typeManager.updateRecordType(recordType);
            fail();
        } catch (RecordTypeNotFoundException expected) {
        }
    }
    
    @Test
    public void testFieldGroupExistsOnCreate() throws Exception {
        String id = "testUpdateNonExistingRecordTypeFails";
        RecordType recordType = typeManager.newRecordType(id);
        recordType.setNonVersionableFieldGroupId("nonExistingFieldGroup");
        try {
            typeManager.createRecordType(recordType);
            fail();
        } catch (FieldGroupNotFoundException expected) {
        }
        recordType = typeManager.newRecordType(id);
        recordType.setVersionableFieldGroupId("nonExistingFieldGroup");
        try {
            typeManager.createRecordType(recordType);
            fail();
        } catch (FieldGroupNotFoundException expected) {
        }
        recordType = typeManager.newRecordType(id);
        recordType.setVersionableMutableFieldGroupId("nonExistingFieldGroup");
        try {
            typeManager.createRecordType(recordType);
            fail();
        } catch (FieldGroupNotFoundException expected) {
        }
    }

    @Test
    public void testFieldGroupVersionExistsOnCreate() throws Exception {
        String id = "testUpdateNonExistingRecordTypeFails";
        RecordType recordType = typeManager.newRecordType(id);
        recordType.setNonVersionableFieldGroupId(fieldGroup1.getId());
        recordType.setNonVersionableFieldGroupVersion(Long.valueOf(99));
        try {
            typeManager.createRecordType(recordType);
            fail();
        } catch (FieldGroupNotFoundException expected) {
        }
        recordType = typeManager.newRecordType(id);
        recordType.setVersionableFieldGroupId(fieldGroup1.getId());
        recordType.setVersionableFieldGroupVersion(Long.valueOf(99));
        try {
            typeManager.createRecordType(recordType);
            fail();
        } catch (FieldGroupNotFoundException expected) {
        }
        recordType = typeManager.newRecordType(id);
        recordType.setVersionableMutableFieldGroupId(fieldGroup1.getId());
        recordType.setVersionableMutableFieldGroupVersion(Long.valueOf(99));
        try {
            typeManager.createRecordType(recordType);
            fail();
        } catch (FieldGroupNotFoundException expected) {
        } 
    }
    
    @Test
    public void testFieldGroupExistsOnUpdate() throws Exception {
        String id = "testFieldGroupExistsOnUpdate";
        RecordType recordType = typeManager.newRecordType(id);
        typeManager.createRecordType(recordType);
        
        recordType.setNonVersionableFieldGroupId("nonExistingFieldGroup");
        try {
            typeManager.updateRecordType(recordType);
            fail();
        } catch (FieldGroupNotFoundException expected) {
        }
        recordType = typeManager.newRecordType(id);
        recordType.setVersionableFieldGroupId("nonExistingFieldGroup");
        try {
            typeManager.updateRecordType(recordType);
            fail();
        } catch (FieldGroupNotFoundException expected) {
        }
        recordType = typeManager.newRecordType(id);
        recordType.setVersionableMutableFieldGroupId("nonExistingFieldGroup");
        try {
            typeManager.updateRecordType(recordType);
            fail();
        } catch (FieldGroupNotFoundException expected) {
        } 
    }
    
    @Test
    public void testFieldGroupVersionExistsOnUpdate() throws Exception {
        String id = "testFieldGroupVersionExistsOnUpdate";
        RecordType recordType = typeManager.newRecordType(id);
        typeManager.createRecordType(recordType);
        
        recordType.setNonVersionableFieldGroupId(fieldGroup1.getId());
        recordType.setNonVersionableFieldGroupVersion(Long.valueOf(99));
        try {
            typeManager.updateRecordType(recordType);
            fail();
        } catch (FieldGroupNotFoundException expected) {
        }
        recordType = typeManager.newRecordType(id);
        recordType.setVersionableFieldGroupId(fieldGroup1.getId());
        recordType.setVersionableFieldGroupVersion(Long.valueOf(99));
        try {
            typeManager.updateRecordType(recordType);
            fail();
        } catch (FieldGroupNotFoundException expected) {
        }
        recordType = typeManager.newRecordType(id);
        recordType.setVersionableMutableFieldGroupId(fieldGroup1.getId());
        recordType.setVersionableMutableFieldGroupVersion(Long.valueOf(99));
        try {
            typeManager.updateRecordType(recordType);
            fail();
        } catch (FieldGroupNotFoundException expected) {
        } 
    }
    
    @Test
    public void testLatestFieldGroupIsTakenByDefaultOnCreate() throws Exception {
        String id = "testLatestFieldGroupIsTakenByDefaultOnCreate";
        RecordType recordType = typeManager.newRecordType(id);
        recordType.setNonVersionableFieldGroupId(fieldGroup1.getId());
        recordType.setVersionableFieldGroupId(fieldGroup2.getId());
        recordType.setVersionableMutableFieldGroupId(fieldGroup3.getId());
        assertEquals(Long.valueOf(1), typeManager.createRecordType(recordType).getVersion());
        RecordType actualRecordType = typeManager.getRecordType(id, null);
        assertEquals(Long.valueOf(2), actualRecordType.getNonVersionableFieldGroupVersion());
        assertEquals(Long.valueOf(2), actualRecordType.getVersionableFieldGroupVersion());
        assertEquals(Long.valueOf(2), actualRecordType.getVersionableMutableFieldGroupVersion());
    }
    
    @Test
    public void testLatestFieldGroupIsTakenByDefaultOnUpdate() throws Exception {
        String id = "testLatestFieldGroupIsTakenByDefaultOnUpdate";
        RecordType recordType = typeManager.newRecordType(id);
        recordType.setNonVersionableFieldGroupId(fieldGroup1.getId());
        recordType.setNonVersionableFieldGroupVersion(fieldGroup1.getVersion());
        recordType.setVersionableFieldGroupId(fieldGroup2.getId());
        recordType.setVersionableFieldGroupVersion(fieldGroup2.getVersion());
        recordType.setVersionableMutableFieldGroupId(fieldGroup3.getId());
        recordType.setVersionableMutableFieldGroupVersion(fieldGroup3.getVersion());
        assertEquals(Long.valueOf(1), typeManager.createRecordType(recordType).getVersion());
        
        RecordType actualRecordType = typeManager.getRecordType(id, null);
        assertEquals(Long.valueOf(1), actualRecordType.getNonVersionableFieldGroupVersion());
        assertEquals(Long.valueOf(1), actualRecordType.getVersionableFieldGroupVersion());
        assertEquals(Long.valueOf(1), actualRecordType.getVersionableMutableFieldGroupVersion());

        recordType = typeManager.newRecordType(id);
        recordType.setNonVersionableFieldGroupId(fieldGroup1B.getId());
        recordType.setVersionableFieldGroupId(fieldGroup2B.getId());
        recordType.setVersionableMutableFieldGroupId(fieldGroup3B.getId());
        typeManager.updateRecordType(recordType);
        actualRecordType = typeManager.getRecordType(id, null);
        assertEquals(Long.valueOf(2), actualRecordType.getNonVersionableFieldGroupVersion());
        assertEquals(Long.valueOf(2), actualRecordType.getVersionableFieldGroupVersion());
        assertEquals(Long.valueOf(2), actualRecordType.getVersionableMutableFieldGroupVersion());
    }
    
    
    @Test
    public void testRemove() throws Exception {
        String id = "testRemove";
        RecordType recordType = typeManager.newRecordType(id);
        recordType.setNonVersionableFieldGroupId(fieldGroup1.getId());
        recordType.setNonVersionableFieldGroupVersion(fieldGroup1.getVersion());
        recordType.setVersionableFieldGroupId(fieldGroup2.getId());
        recordType.setVersionableFieldGroupVersion(fieldGroup2.getVersion());
        recordType.setVersionableMutableFieldGroupId(fieldGroup3.getId());
        recordType.setVersionableMutableFieldGroupVersion(fieldGroup3.getVersion());
        typeManager.createRecordType(recordType);
        
        RecordType recordTypeRemovedGroups = typeManager.removeFieldGroups(id, true, true, true);
        assertEquals(Long.valueOf(2), recordTypeRemovedGroups.getVersion());
        assertNull(recordTypeRemovedGroups.getNonVersionableFieldGroupId());
        assertNull(recordTypeRemovedGroups.getNonVersionableFieldGroupVersion());
        assertNull(recordTypeRemovedGroups.getVersionableFieldGroupId());
        assertNull(recordTypeRemovedGroups.getVersionableFieldGroupVersion());
        assertNull(recordTypeRemovedGroups.getVersionableMutableFieldGroupId());
        assertNull(recordTypeRemovedGroups.getVersionableMutableFieldGroupVersion());
        
        assertEquals(recordTypeRemovedGroups, typeManager.getRecordType(id, null));
    }
    
    @Test
    public void testRemoveNonExisting() throws Exception {
        String id = "testRemoveNonExisting";
        RecordType recordType = typeManager.createRecordType(typeManager.newRecordType(id));
        
        assertEquals(recordType, typeManager.removeFieldGroups(id, true, true, true));
    }
    
    @Test
    public void testRemoveLeavesOlderVersionsUntouched() throws Exception {
        String id = "testRemoveLeavesOlderVersionsUntouched";
        RecordType recordType = typeManager.newRecordType(id);
        recordType.setNonVersionableFieldGroupId(fieldGroup1.getId());
        recordType.setNonVersionableFieldGroupVersion(fieldGroup1.getVersion());
        recordType.setVersionableFieldGroupId(fieldGroup2.getId());
        recordType.setVersionableFieldGroupVersion(fieldGroup2.getVersion());
        recordType.setVersionableMutableFieldGroupId(fieldGroup3.getId());
        recordType.setVersionableMutableFieldGroupVersion(fieldGroup3.getVersion());
        typeManager.createRecordType(recordType);
        typeManager.removeFieldGroups(id, true, true, true);
        
        RecordType recordTypeV1 = typeManager.getRecordType(id, Long.valueOf(1));
        assertEquals(fieldGroup1.getId(), recordTypeV1.getNonVersionableFieldGroupId());
        assertEquals(fieldGroup1.getVersion(), recordTypeV1.getNonVersionableFieldGroupVersion());
        assertEquals(fieldGroup2.getId(), recordTypeV1.getVersionableFieldGroupId());
        assertEquals(fieldGroup2.getVersion(), recordTypeV1.getVersionableFieldGroupVersion());
        assertEquals(fieldGroup3.getId(), recordTypeV1.getVersionableMutableFieldGroupId());
        assertEquals(fieldGroup3.getVersion(), recordTypeV1.getVersionableMutableFieldGroupVersion());
    }

    @Test
    public void testRemoveLeavesOtherFieldGroupEntriesAlone() throws Exception {
        String id = "testRemoveLeavesOtherFieldGroupEntriesAlone";
        RecordType recordType = typeManager.newRecordType(id);
        recordType.setNonVersionableFieldGroupId(fieldGroup1.getId());
        recordType.setNonVersionableFieldGroupVersion(fieldGroup1.getVersion());
        recordType.setVersionableFieldGroupId(fieldGroup2.getId());
        recordType.setVersionableFieldGroupVersion(fieldGroup2.getVersion());
        recordType.setVersionableMutableFieldGroupId(fieldGroup3.getId());
        recordType.setVersionableMutableFieldGroupVersion(fieldGroup3.getVersion());
        typeManager.createRecordType(recordType);
        RecordType recordTypeRemoved = typeManager.removeFieldGroups(id, true, false, false);
        assertNull(recordTypeRemoved.getNonVersionableFieldGroupId());
        assertNull(recordTypeRemoved.getNonVersionableFieldGroupVersion());
        assertEquals(fieldGroup2.getId(), recordTypeRemoved.getVersionableFieldGroupId());
        assertEquals(fieldGroup2.getVersion(), recordTypeRemoved.getVersionableFieldGroupVersion());
        assertEquals(fieldGroup3.getId(), recordTypeRemoved.getVersionableMutableFieldGroupId());
        assertEquals(fieldGroup3.getVersion(), recordTypeRemoved.getVersionableMutableFieldGroupVersion());
        assertEquals(recordTypeRemoved, typeManager.getRecordType(id, null));
    }
    
//    @Test
//    public void testCreateWithDuplicateFieldGroupFails() throws Exception {
//        throw new NotImplementedException();
//    }
}
