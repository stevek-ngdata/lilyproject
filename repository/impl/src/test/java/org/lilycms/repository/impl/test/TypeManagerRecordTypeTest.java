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

import java.util.Map;

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
import org.lilycms.repository.api.Record.Scope;
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
        recordType.setFieldGroupId(Scope.NON_VERSIONABLE,fieldGroup1.getId());
        recordType.setFieldGroupVersion(Scope.NON_VERSIONABLE,fieldGroup1.getVersion());
        recordType.setFieldGroupId(Scope.VERSIONABLE,fieldGroup2.getId());
        recordType.setFieldGroupVersion(Scope.VERSIONABLE,fieldGroup2.getVersion());
        recordType.setFieldGroupId(Scope.VERSIONABLE_MUTABLE,fieldGroup3.getId());
        recordType.setFieldGroupVersion(Scope.VERSIONABLE_MUTABLE,fieldGroup3.getVersion());
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
        
        recordType.setFieldGroupId(Scope.NON_VERSIONABLE,fieldGroup1.getId());
        recordType.setFieldGroupVersion(Scope.NON_VERSIONABLE,fieldGroup1.getVersion());
        RecordType recordTypeV2 = typeManager.updateRecordType(recordType);
        assertEquals(Long.valueOf(2), recordTypeV2.getVersion());
        assertEquals(Long.valueOf(2), typeManager.updateRecordType(recordType).getVersion());
        
        recordType.setFieldGroupId(Scope.VERSIONABLE,fieldGroup2.getId());
        recordType.setFieldGroupVersion(Scope.VERSIONABLE,fieldGroup2.getVersion());
        RecordType recordTypeV3 = typeManager.updateRecordType(recordType);
        assertEquals(Long.valueOf(3), recordTypeV3.getVersion());
        assertEquals(Long.valueOf(3), typeManager.updateRecordType(recordType).getVersion());

        recordType.setFieldGroupId(Scope.VERSIONABLE_MUTABLE,fieldGroup3.getId());
        recordType.setFieldGroupVersion(Scope.VERSIONABLE_MUTABLE,fieldGroup3.getVersion());
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
        recordType.setFieldGroupId(Scope.NON_VERSIONABLE,"nonExistingFieldGroup");
        try {
            typeManager.createRecordType(recordType);
            fail();
        } catch (FieldGroupNotFoundException expected) {
        }
        recordType = typeManager.newRecordType(id);
        recordType.setFieldGroupId(Scope.VERSIONABLE,"nonExistingFieldGroup");
        try {
            typeManager.createRecordType(recordType);
            fail();
        } catch (FieldGroupNotFoundException expected) {
        }
        recordType = typeManager.newRecordType(id);
        recordType.setFieldGroupId(Scope.VERSIONABLE_MUTABLE,"nonExistingFieldGroup");
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
        recordType.setFieldGroupId(Scope.NON_VERSIONABLE,fieldGroup1.getId());
        recordType.setFieldGroupVersion(Scope.NON_VERSIONABLE,Long.valueOf(99));
        try {
            typeManager.createRecordType(recordType);
            fail();
        } catch (FieldGroupNotFoundException expected) {
        }
        recordType = typeManager.newRecordType(id);
        recordType.setFieldGroupId(Scope.VERSIONABLE,fieldGroup1.getId());
        recordType.setFieldGroupVersion(Scope.VERSIONABLE,Long.valueOf(99));
        try {
            typeManager.createRecordType(recordType);
            fail();
        } catch (FieldGroupNotFoundException expected) {
        }
        recordType = typeManager.newRecordType(id);
        recordType.setFieldGroupId(Scope.VERSIONABLE_MUTABLE,fieldGroup1.getId());
        recordType.setFieldGroupVersion(Scope.VERSIONABLE_MUTABLE,Long.valueOf(99));
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
        
        recordType.setFieldGroupId(Scope.NON_VERSIONABLE,"nonExistingFieldGroup");
        try {
            typeManager.updateRecordType(recordType);
            fail();
        } catch (FieldGroupNotFoundException expected) {
        }
        recordType = typeManager.newRecordType(id);
        recordType.setFieldGroupId(Scope.VERSIONABLE,"nonExistingFieldGroup");
        try {
            typeManager.updateRecordType(recordType);
            fail();
        } catch (FieldGroupNotFoundException expected) {
        }
        recordType = typeManager.newRecordType(id);
        recordType.setFieldGroupId(Scope.VERSIONABLE_MUTABLE,"nonExistingFieldGroup");
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
        
        recordType.setFieldGroupId(Scope.NON_VERSIONABLE,fieldGroup1.getId());
        recordType.setFieldGroupVersion(Scope.NON_VERSIONABLE,Long.valueOf(99));
        try {
            typeManager.updateRecordType(recordType);
            fail();
        } catch (FieldGroupNotFoundException expected) {
        }
        recordType = typeManager.newRecordType(id);
        recordType.setFieldGroupId(Scope.VERSIONABLE,fieldGroup1.getId());
        recordType.setFieldGroupVersion(Scope.VERSIONABLE,Long.valueOf(99));
        try {
            typeManager.updateRecordType(recordType);
            fail();
        } catch (FieldGroupNotFoundException expected) {
        }
        recordType = typeManager.newRecordType(id);
        recordType.setFieldGroupId(Scope.VERSIONABLE_MUTABLE,fieldGroup1.getId());
        recordType.setFieldGroupVersion(Scope.VERSIONABLE_MUTABLE,Long.valueOf(99));
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
        recordType.setFieldGroupId(Scope.NON_VERSIONABLE,fieldGroup1.getId());
        recordType.setFieldGroupId(Scope.VERSIONABLE,fieldGroup2.getId());
        recordType.setFieldGroupId(Scope.VERSIONABLE_MUTABLE,fieldGroup3.getId());
        assertEquals(Long.valueOf(1), typeManager.createRecordType(recordType).getVersion());
        RecordType actualRecordType = typeManager.getRecordType(id, null);
        assertEquals(Long.valueOf(2), actualRecordType.getFieldGroupVersion(Scope.NON_VERSIONABLE));
        assertEquals(Long.valueOf(2), actualRecordType.getFieldGroupVersion(Scope.VERSIONABLE));
        assertEquals(Long.valueOf(2), actualRecordType.getFieldGroupVersion(Scope.VERSIONABLE_MUTABLE));
    }
    
    @Test
    public void testLatestFieldGroupIsTakenByDefaultOnUpdate() throws Exception {
        String id = "testLatestFieldGroupIsTakenByDefaultOnUpdate";
        RecordType recordType = typeManager.newRecordType(id);
        recordType.setFieldGroupId(Scope.NON_VERSIONABLE,fieldGroup1.getId());
        recordType.setFieldGroupVersion(Scope.NON_VERSIONABLE,fieldGroup1.getVersion());
        recordType.setFieldGroupId(Scope.VERSIONABLE,fieldGroup2.getId());
        recordType.setFieldGroupVersion(Scope.VERSIONABLE,fieldGroup2.getVersion());
        recordType.setFieldGroupId(Scope.VERSIONABLE_MUTABLE,fieldGroup3.getId());
        recordType.setFieldGroupVersion(Scope.VERSIONABLE_MUTABLE,fieldGroup3.getVersion());
        assertEquals(Long.valueOf(1), typeManager.createRecordType(recordType).getVersion());
        
        RecordType actualRecordType = typeManager.getRecordType(id, null);
        assertEquals(Long.valueOf(1), actualRecordType.getFieldGroupVersion(Scope.NON_VERSIONABLE));
        assertEquals(Long.valueOf(1), actualRecordType.getFieldGroupVersion(Scope.VERSIONABLE));
        assertEquals(Long.valueOf(1), actualRecordType.getFieldGroupVersion(Scope.VERSIONABLE_MUTABLE));

        recordType = typeManager.newRecordType(id);
        recordType.setFieldGroupId(Scope.NON_VERSIONABLE,fieldGroup1B.getId());
        recordType.setFieldGroupId(Scope.VERSIONABLE,fieldGroup2B.getId());
        recordType.setFieldGroupId(Scope.VERSIONABLE_MUTABLE,fieldGroup3B.getId());
        typeManager.updateRecordType(recordType);
        actualRecordType = typeManager.getRecordType(id, null);
        assertEquals(Long.valueOf(2), actualRecordType.getFieldGroupVersion(Scope.NON_VERSIONABLE));
        assertEquals(Long.valueOf(2), actualRecordType.getFieldGroupVersion(Scope.VERSIONABLE));
        assertEquals(Long.valueOf(2), actualRecordType.getFieldGroupVersion(Scope.VERSIONABLE_MUTABLE));
    }
    
    
    @Test
    public void testRemove() throws Exception {
        String id = "testRemove";
        RecordType recordType = typeManager.newRecordType(id);
        recordType.setFieldGroupId(Scope.NON_VERSIONABLE,fieldGroup1.getId());
        recordType.setFieldGroupVersion(Scope.NON_VERSIONABLE,fieldGroup1.getVersion());
        recordType.setFieldGroupId(Scope.VERSIONABLE,fieldGroup2.getId());
        recordType.setFieldGroupVersion(Scope.VERSIONABLE,fieldGroup2.getVersion());
        recordType.setFieldGroupId(Scope.VERSIONABLE_MUTABLE,fieldGroup3.getId());
        recordType.setFieldGroupVersion(Scope.VERSIONABLE_MUTABLE,fieldGroup3.getVersion());
        typeManager.createRecordType(recordType);
        
        RecordType recordTypeRemovedGroups = typeManager.removeFieldGroups(id, true, true, true);
        assertEquals(Long.valueOf(2), recordTypeRemovedGroups.getVersion());
        assertNull(recordTypeRemovedGroups.getFieldGroupId(Scope.NON_VERSIONABLE));
        assertNull(recordTypeRemovedGroups.getFieldGroupVersion(Scope.NON_VERSIONABLE));
        assertNull(recordTypeRemovedGroups.getFieldGroupId(Scope.VERSIONABLE));
        assertNull(recordTypeRemovedGroups.getFieldGroupVersion(Scope.VERSIONABLE));
        assertNull(recordTypeRemovedGroups.getFieldGroupId(Scope.VERSIONABLE_MUTABLE));
        assertNull(recordTypeRemovedGroups.getFieldGroupVersion(Scope.VERSIONABLE_MUTABLE));
        
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
        recordType.setFieldGroupId(Scope.NON_VERSIONABLE,fieldGroup1.getId());
        recordType.setFieldGroupVersion(Scope.NON_VERSIONABLE,fieldGroup1.getVersion());
        recordType.setFieldGroupId(Scope.VERSIONABLE,fieldGroup2.getId());
        recordType.setFieldGroupVersion(Scope.VERSIONABLE,fieldGroup2.getVersion());
        recordType.setFieldGroupId(Scope.VERSIONABLE_MUTABLE,fieldGroup3.getId());
        recordType.setFieldGroupVersion(Scope.VERSIONABLE_MUTABLE,fieldGroup3.getVersion());
        typeManager.createRecordType(recordType);
        typeManager.removeFieldGroups(id, true, true, true);
        
        RecordType recordTypeV1 = typeManager.getRecordType(id, Long.valueOf(1));
        assertEquals(fieldGroup1.getId(), recordTypeV1.getFieldGroupId(Scope.NON_VERSIONABLE));
        assertEquals(fieldGroup1.getVersion(), recordTypeV1.getFieldGroupVersion(Scope.NON_VERSIONABLE));
        assertEquals(fieldGroup2.getId(), recordTypeV1.getFieldGroupId(Scope.VERSIONABLE));
        assertEquals(fieldGroup2.getVersion(), recordTypeV1.getFieldGroupVersion(Scope.VERSIONABLE));
        assertEquals(fieldGroup3.getId(), recordTypeV1.getFieldGroupId(Scope.VERSIONABLE_MUTABLE));
        assertEquals(fieldGroup3.getVersion(), recordTypeV1.getFieldGroupVersion(Scope.VERSIONABLE_MUTABLE));
    }

    @Test
    public void testRemoveLeavesOtherFieldGroupEntriesAlone() throws Exception {
        String id = "testRemoveLeavesOtherFieldGroupEntriesAlone";
        RecordType recordType = typeManager.newRecordType(id);
        recordType.setFieldGroupId(Scope.NON_VERSIONABLE,fieldGroup1.getId());
        recordType.setFieldGroupVersion(Scope.NON_VERSIONABLE,fieldGroup1.getVersion());
        recordType.setFieldGroupId(Scope.VERSIONABLE,fieldGroup2.getId());
        recordType.setFieldGroupVersion(Scope.VERSIONABLE,fieldGroup2.getVersion());
        recordType.setFieldGroupId(Scope.VERSIONABLE_MUTABLE,fieldGroup3.getId());
        recordType.setFieldGroupVersion(Scope.VERSIONABLE_MUTABLE,fieldGroup3.getVersion());
        typeManager.createRecordType(recordType);
        RecordType recordTypeRemoved = typeManager.removeFieldGroups(id, true, false, false);
        assertNull(recordTypeRemoved.getFieldGroupId(Scope.NON_VERSIONABLE));
        assertNull(recordTypeRemoved.getFieldGroupVersion(Scope.NON_VERSIONABLE));
        assertEquals(fieldGroup2.getId(), recordTypeRemoved.getFieldGroupId(Scope.VERSIONABLE));
        assertEquals(fieldGroup2.getVersion(), recordTypeRemoved.getFieldGroupVersion(Scope.VERSIONABLE));
        assertEquals(fieldGroup3.getId(), recordTypeRemoved.getFieldGroupId(Scope.VERSIONABLE_MUTABLE));
        assertEquals(fieldGroup3.getVersion(), recordTypeRemoved.getFieldGroupVersion(Scope.VERSIONABLE_MUTABLE));
        assertEquals(recordTypeRemoved, typeManager.getRecordType(id, null));
    }
    
    @Test
    public void testMixin() throws Exception {
        String id = "testMixin";
        RecordType mixinType = typeManager.newRecordType(id+"MIX");
        mixinType.setFieldGroupId(Scope.NON_VERSIONABLE,fieldGroup1.getId());
        mixinType.setFieldGroupVersion(Scope.NON_VERSIONABLE,fieldGroup1.getVersion());
        mixinType.setFieldGroupId(Scope.VERSIONABLE,fieldGroup2.getId());
        mixinType.setFieldGroupVersion(Scope.VERSIONABLE,fieldGroup2.getVersion());
        mixinType.setFieldGroupId(Scope.VERSIONABLE_MUTABLE,fieldGroup3.getId());
        mixinType.setFieldGroupVersion(Scope.VERSIONABLE_MUTABLE,fieldGroup3.getVersion());
        mixinType = typeManager.createRecordType(mixinType);
        
        RecordType recordType = typeManager.newRecordType(id+"RT");
        recordType.addMixin(mixinType.getId(), mixinType.getVersion());
        assertEquals(Long.valueOf(1), typeManager.createRecordType(recordType).getVersion());
        recordType.setVersion(Long.valueOf(1));
        assertEquals(recordType, typeManager.getRecordType(recordType.getId(), null));
    }
    
    @Test
    public void testMixinUpdate() throws Exception {
        String id = "testMixinUpdate";
        RecordType mixinType1 = typeManager.newRecordType(id+"MIX");
        mixinType1.setFieldGroupId(Scope.NON_VERSIONABLE,fieldGroup1.getId());
        mixinType1.setFieldGroupVersion(Scope.NON_VERSIONABLE,fieldGroup1.getVersion());
        mixinType1.setFieldGroupId(Scope.VERSIONABLE,fieldGroup2.getId());
        mixinType1.setFieldGroupVersion(Scope.VERSIONABLE,fieldGroup2.getVersion());
        mixinType1.setFieldGroupId(Scope.VERSIONABLE_MUTABLE,fieldGroup3.getId());
        mixinType1.setFieldGroupVersion(Scope.VERSIONABLE_MUTABLE,fieldGroup3.getVersion());
        mixinType1 = typeManager.createRecordType(mixinType1);
        RecordType mixinType2 = typeManager.newRecordType(id+"MIX2");
        mixinType2.setFieldGroupId(Scope.NON_VERSIONABLE,fieldGroup1.getId());
        mixinType2.setFieldGroupVersion(Scope.NON_VERSIONABLE,fieldGroup1.getVersion());
        mixinType2.setFieldGroupId(Scope.VERSIONABLE,fieldGroup2.getId());
        mixinType2.setFieldGroupVersion(Scope.VERSIONABLE,fieldGroup2.getVersion());
        mixinType2.setFieldGroupId(Scope.VERSIONABLE_MUTABLE,fieldGroup3.getId());
        mixinType2.setFieldGroupVersion(Scope.VERSIONABLE_MUTABLE,fieldGroup3.getVersion());
        mixinType2 = typeManager.createRecordType(mixinType2);
        
        RecordType recordType = typeManager.newRecordType(id+"RT");
        recordType.addMixin(mixinType1.getId(), mixinType1.getVersion());
        typeManager.createRecordType(recordType);
        
        recordType.addMixin(mixinType2.getId(), mixinType2.getVersion());
        assertEquals(Long.valueOf(2), typeManager.updateRecordType(recordType).getVersion());
        recordType.setVersion(Long.valueOf(2));
        assertEquals(recordType, typeManager.getRecordType(recordType.getId(), null));
    }
    
    @Test
    public void testMixinRemove() throws Exception {
        String id = "testMixinRemove";
        RecordType mixinType1 = typeManager.newRecordType(id+"MIX");
        mixinType1.setFieldGroupId(Scope.NON_VERSIONABLE,fieldGroup1.getId());
        mixinType1.setFieldGroupVersion(Scope.NON_VERSIONABLE,fieldGroup1.getVersion());
        mixinType1.setFieldGroupId(Scope.VERSIONABLE,fieldGroup2.getId());
        mixinType1.setFieldGroupVersion(Scope.VERSIONABLE,fieldGroup2.getVersion());
        mixinType1.setFieldGroupId(Scope.VERSIONABLE_MUTABLE,fieldGroup3.getId());
        mixinType1.setFieldGroupVersion(Scope.VERSIONABLE_MUTABLE,fieldGroup3.getVersion());
        mixinType1 = typeManager.createRecordType(mixinType1);
        RecordType mixinType2 = typeManager.newRecordType(id+"MIX2");
        mixinType2.setFieldGroupId(Scope.NON_VERSIONABLE,fieldGroup1.getId());
        mixinType2.setFieldGroupVersion(Scope.NON_VERSIONABLE,fieldGroup1.getVersion());
        mixinType2.setFieldGroupId(Scope.VERSIONABLE,fieldGroup2.getId());
        mixinType2.setFieldGroupVersion(Scope.VERSIONABLE,fieldGroup2.getVersion());
        mixinType2.setFieldGroupId(Scope.VERSIONABLE_MUTABLE,fieldGroup3.getId());
        mixinType2.setFieldGroupVersion(Scope.VERSIONABLE_MUTABLE,fieldGroup3.getVersion());
        mixinType2 = typeManager.createRecordType(mixinType2);
        
        RecordType recordType = typeManager.newRecordType(id+"RT");
        recordType.addMixin(mixinType1.getId(), mixinType1.getVersion());
        typeManager.createRecordType(recordType);
        
        recordType.addMixin(mixinType2.getId(), mixinType2.getVersion());
        recordType.removeMixin(mixinType1.getId());
        assertEquals(Long.valueOf(2), typeManager.updateRecordType(recordType).getVersion());
        RecordType readRecordType = typeManager.getRecordType(recordType.getId(), null);
        Map<String, Long> mixins = readRecordType.getMixins();
        assertEquals(1, mixins.size());
        assertEquals(Long.valueOf(1), mixins.get(mixinType2.getId()));
    }
    
//    @Test
//    public void testCreateWithDuplicateFieldGroupFails() throws Exception {
//        throw new NotImplementedException();
//    }
}
