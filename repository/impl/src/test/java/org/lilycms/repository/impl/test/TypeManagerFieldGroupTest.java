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

import java.util.Arrays;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilycms.repository.api.FieldDescriptor;
import org.lilycms.repository.api.FieldDescriptorNotFoundException;
import org.lilycms.repository.api.FieldGroup;
import org.lilycms.repository.api.FieldGroupEntry;
import org.lilycms.repository.api.FieldGroupExistsException;
import org.lilycms.repository.api.FieldGroupNotFoundException;
import org.lilycms.repository.api.TypeManager;
import org.lilycms.repository.impl.HBaseTypeManager;
import org.lilycms.repository.impl.IdGeneratorImpl;
import org.lilycms.testfw.TestHelper;

public class TypeManagerFieldGroupTest {

    private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    private static TypeManager typeManager;
    private static FieldDescriptor fieldDescriptor1;
    private static FieldDescriptor fieldDescriptor2;
    private static FieldDescriptor fieldDescriptor3;
    private static FieldDescriptor fieldDescriptor3B;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging();
        TEST_UTIL.startMiniCluster(1);
        typeManager = new HBaseTypeManager(new IdGeneratorImpl(), TEST_UTIL.getConfiguration());
        setupFieldDescriptors();
    }
    
    private static void setupFieldDescriptors() throws Exception {
        fieldDescriptor1 = typeManager.createFieldDescriptor(typeManager.newFieldDescriptor("FD1", typeManager.getValueType("STRING", false, false), "GN1"));
        fieldDescriptor2 = typeManager.createFieldDescriptor(typeManager.newFieldDescriptor("FD2", typeManager.getValueType("STRING", false, false), "GN2"));
        fieldDescriptor3 = typeManager.createFieldDescriptor(typeManager.newFieldDescriptor("FD3", typeManager.getValueType("STRING", false, false), "GN3"));
        fieldDescriptor3B = typeManager.updateFieldDescriptor(typeManager.newFieldDescriptor("FD3", typeManager.getValueType("STRING", false, false), "GN3B"));
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
    public void testEmptyCreate() throws Exception {
        String id = "testEmptyCreate";
        FieldGroup fieldGroup = typeManager.newFieldGroup(id);
        fieldGroup = typeManager.createFieldGroup(fieldGroup);
        assertEquals(Long.valueOf(1), fieldGroup.getVersion());
        assertEquals(id, fieldGroup.getId());
    }
    
    @Test
    public void testCreateExistingFails() throws Exception {
        String id = "testCreateExistingFails";
        FieldGroup fieldGroup = typeManager.newFieldGroup(id);
        fieldGroup = typeManager.createFieldGroup(fieldGroup);

        try {
            typeManager.createFieldGroup(fieldGroup);
            fail("Creating an existing fieldGroup should not be possible.");
        } catch (FieldGroupExistsException expected) {
        }
    }
    
    @Test
    public void testCreate() throws Exception {
        String id = "testCreate";
        FieldGroup fieldGroup = typeManager.newFieldGroup(id);
        FieldGroupEntry fieldGroupEntry = typeManager.newFieldGroupEntry(fieldDescriptor1.getId(), fieldDescriptor1.getVersion(), false, "alias1");
        fieldGroup.setFieldGroupEntry(fieldGroupEntry);
        typeManager.createFieldGroup(fieldGroup);
        
        fieldGroup.setVersion(Long.valueOf(1));
        assertEquals(fieldGroup, typeManager.getFieldGroup(id, null));
    }
    
    @Test
    public void testCreateMultipleFiedDescriptors() throws Exception {
        String id = "testCreateMultipleFiedDescriptors";
        FieldGroup fieldGroup = typeManager.newFieldGroup(id);
        fieldGroup.setFieldGroupEntry(typeManager.newFieldGroupEntry(fieldDescriptor1.getId(), fieldDescriptor1.getVersion(), false, "alias1"));
        fieldGroup.setFieldGroupEntry(typeManager.newFieldGroupEntry(fieldDescriptor2.getId(), fieldDescriptor2.getVersion(), false, "alias2"));
        fieldGroup.setFieldGroupEntry(typeManager.newFieldGroupEntry(fieldDescriptor3.getId(), fieldDescriptor3.getVersion(), false, "alias3"));
        typeManager.createFieldGroup(fieldGroup);
        
        fieldGroup.setVersion(Long.valueOf(1));
        FieldGroup actualFieldGroup = typeManager.getFieldGroup(id, null);
        assertEquals(fieldGroup, actualFieldGroup);
        assertEquals(Long.valueOf(1), actualFieldGroup.getFieldGroupEntry("FD3").getFieldDescriptorVersion());
    }
    
    @Test
    public void testUpdate() throws Exception {
        String id = "testUpdate";
        FieldGroup fieldGroup = typeManager.newFieldGroup(id);
        typeManager.createFieldGroup(fieldGroup);
        
        fieldGroup.setFieldGroupEntry(typeManager.newFieldGroupEntry(fieldDescriptor1.getId(), fieldDescriptor1.getVersion(), false, "alias1"));
        typeManager.updateFieldGroup(fieldGroup);
        
        fieldGroup.setVersion(Long.valueOf(2));
        assertEquals(fieldGroup, typeManager.getFieldGroup(id, null));
    }
    
    @Test
    public void testUpdateVersions() throws Exception {
        String id = "testUpdateVersions";
        FieldGroup fieldGroup = typeManager.newFieldGroup(id);
        // Ignore version on create
        fieldGroup.setVersion(Long.valueOf(11));
        assertEquals(Long.valueOf(1), typeManager.createFieldGroup(fieldGroup).getVersion());
        
        // Ignore version on update
        fieldGroup.setVersion(Long.valueOf(99));
        fieldGroup.setFieldGroupEntry(typeManager.newFieldGroupEntry(fieldDescriptor1.getId(), fieldDescriptor1.getVersion(), false, "alias1"));
        assertEquals(Long.valueOf(2), typeManager.updateFieldGroup(fieldGroup).getVersion());
    }
    
    @Test
    public void testRetrieveOlderVersions() throws Exception {
        String id = "testRetrieveOlderVersions";
        FieldGroup fieldGroupCreate = typeManager.newFieldGroup(id);
        typeManager.createFieldGroup(fieldGroupCreate);
        
        FieldGroup fieldGroupUpdate = typeManager.newFieldGroup(id); 
        fieldGroupUpdate.setFieldGroupEntry(typeManager.newFieldGroupEntry(fieldDescriptor1.getId(), fieldDescriptor1.getVersion(), false, "alias1"));
        typeManager.updateFieldGroup(fieldGroupUpdate);
        
        fieldGroupCreate.setVersion(Long.valueOf(1));
        assertEquals(fieldGroupCreate, typeManager.getFieldGroup(id, Long.valueOf(1)));
    }
    
    @Test
    public void testIdempotentUpdate() throws Exception {
        String id = "testIdempotentUpdate";
        FieldGroup fieldGroupCreate = typeManager.newFieldGroup(id);
        typeManager.createFieldGroup(fieldGroupCreate);
        
        FieldGroup fieldGroupUpdate = typeManager.newFieldGroup(id); 
        fieldGroupUpdate.setFieldGroupEntry(typeManager.newFieldGroupEntry(fieldDescriptor1.getId(), fieldDescriptor1.getVersion(), false, "alias1"));
        typeManager.updateFieldGroup(fieldGroupUpdate);
        
        assertEquals(Long.valueOf(2), typeManager.updateFieldGroup(fieldGroupUpdate).getVersion());
        fieldGroupUpdate.setVersion(Long.valueOf(2));
        assertEquals(fieldGroupUpdate, typeManager.getFieldGroup(id, null));
    }
    
    @Test 
    public void testUpdateNonExistingFieldGroupFails() throws Exception {
        String id = "testUpdateNonExistingFieldGroupFails";
        
        FieldGroup fieldGroup = typeManager.newFieldGroup(id); 
        try {
            typeManager.updateFieldGroup(fieldGroup);
            fail();
        } catch (FieldGroupNotFoundException expected) {
        }
    }
    
    @Test
    public void testFieldDescriptorExistsOnCreate() throws Exception {
        String id = "testFieldDescriptorExistsOnCreate";
        FieldGroup fieldGroup = typeManager.newFieldGroup(id);
        FieldGroupEntry fieldGroupEntry = typeManager.newFieldGroupEntry("nonExistingFD", null, false, "alias1");
        fieldGroup.setFieldGroupEntry(fieldGroupEntry);
        try {
            typeManager.createFieldGroup(fieldGroup);
            fail();
        } catch (FieldDescriptorNotFoundException expected) {
        }
    }
    
    @Test
    public void testFieldDescriptorVersionExistsOnCreate() throws Exception {
        String id = "testFieldDescriptorVersionExistsOnCreate";
        FieldGroup fieldGroup = typeManager.newFieldGroup(id);
        FieldGroupEntry fieldGroupEntry = typeManager.newFieldGroupEntry(fieldDescriptor1.getId(), Long.valueOf(3), false, "alias1");
        fieldGroup.setFieldGroupEntry(fieldGroupEntry);
        try {
            typeManager.createFieldGroup(fieldGroup);
            fail();
        } catch (FieldDescriptorNotFoundException expected) {
        }
    }

    @Test
    public void testFieldDescriptorExistsOnUpdate() throws Exception {
        String id = "testFieldDescriptorExistsOnUpdate";
        FieldGroup fieldGroup = typeManager.newFieldGroup(id);
        typeManager.createFieldGroup(fieldGroup);
        FieldGroupEntry fieldGroupEntry = typeManager.newFieldGroupEntry("nonExistingFD", null, false, "alias1");
        fieldGroup.setFieldGroupEntry(fieldGroupEntry);
        try {
            typeManager.updateFieldGroup(fieldGroup);
            fail();
        } catch (FieldDescriptorNotFoundException expected) {
        }
    }
    
    @Test
    public void testFieldDescriptorVersionExistsOnUpdate() throws Exception {
        String id = "testFieldDescriptorVersionExistsOnCreate";
        FieldGroup fieldGroup = typeManager.newFieldGroup(id);
        typeManager.createFieldGroup(fieldGroup);
        FieldGroupEntry fieldGroupEntry = typeManager.newFieldGroupEntry(fieldDescriptor1.getId(), Long.valueOf(3), false, "alias1");
        fieldGroup.setFieldGroupEntry(fieldGroupEntry);
        try {
            typeManager.updateFieldGroup(fieldGroup);
            fail();
        } catch (FieldDescriptorNotFoundException expected) {
        }
    }

    @Test
    public void testLatestFieldDescriptorIsTakenByDefault() throws Exception {
        String id = "testLatestFieldDescriptorIsTakenByDefault";
        FieldGroup fieldGroup = typeManager.newFieldGroup(id);
        FieldGroupEntry fieldGroupEntry = typeManager.newFieldGroupEntry(fieldDescriptor1.getId(), null, false, "alias1");
        fieldGroup.setFieldGroupEntry(fieldGroupEntry);
        FieldGroup createdFieldgroup = typeManager.createFieldGroup(fieldGroup);
        assertEquals(fieldDescriptor1.getVersion(), createdFieldgroup.getFieldGroupEntry("FD1").getFieldDescriptorVersion());
        
        fieldGroupEntry = typeManager.newFieldGroupEntry(fieldDescriptor2.getId(), null, false, "alias2");
        fieldGroup.setFieldGroupEntry(fieldGroupEntry);
        FieldGroup updatedFieldgroup = typeManager.updateFieldGroup(fieldGroup);
        assertEquals(fieldDescriptor2.getVersion(), updatedFieldgroup.getFieldGroupEntry("FD2").getFieldDescriptorVersion());
        
        fieldGroupEntry = typeManager.newFieldGroupEntry(fieldDescriptor3.getId(), null, false, "alias3");
        fieldGroup.setFieldGroupEntry(fieldGroupEntry);
        updatedFieldgroup = typeManager.updateFieldGroup(fieldGroup);
        assertEquals(fieldDescriptor3B.getVersion(), updatedFieldgroup.getFieldGroupEntry("FD3").getFieldDescriptorVersion());
    }
    
    @Test
    public void testRemove() throws Exception {
        String id = "testRemove";
        FieldGroup fieldGroup = typeManager.newFieldGroup(id);
        FieldGroupEntry fieldGroupEntry = typeManager.newFieldGroupEntry(fieldDescriptor1.getId(), fieldDescriptor1.getVersion(), false, "alias1");
        fieldGroup.setFieldGroupEntry(fieldGroupEntry);
        typeManager.createFieldGroup(fieldGroup);
        
        FieldGroup fieldGroupAfterRemove = typeManager.removeFieldDescriptors(id, Arrays.asList(new String[]{ fieldDescriptor1.getId()}));
        assertEquals(Long.valueOf(2), fieldGroupAfterRemove.getVersion());
        assertTrue(fieldGroupAfterRemove.getFieldGroupEntries().isEmpty());
        assertEquals(fieldGroupAfterRemove, typeManager.getFieldGroup(id, null));
    }
    
    @Test
    public void testRemoveNonExisting() throws Exception {
        String id = "testRemoveNonExisting";
        FieldGroup fieldGroup = typeManager.newFieldGroup(id);
        FieldGroup createdFieldGroup = typeManager.createFieldGroup(fieldGroup);
        
        assertEquals(createdFieldGroup, typeManager.removeFieldDescriptors(id, Arrays.asList(new String[] {"nonExistingFieldDescriptorId"})));
    }
    
    @Test
    public void testRemoveLeavesOlderVersionsUntouched() throws Exception {
        String id = "testRemoveLeavesOlderVersionsUntouched";
        FieldGroup fieldGroup = typeManager.newFieldGroup(id);
        FieldGroupEntry fieldGroupEntry = typeManager.newFieldGroupEntry(fieldDescriptor1.getId(), fieldDescriptor1.getVersion(), false, "alias1");
        fieldGroup.setFieldGroupEntry(fieldGroupEntry);
        typeManager.createFieldGroup(fieldGroup);
        
        fieldGroupEntry = typeManager.newFieldGroupEntry(fieldDescriptor1.getId(), fieldDescriptor1.getVersion(), false, "alias2");
        fieldGroup.setFieldGroupEntry(fieldGroupEntry);
        typeManager.updateFieldGroup(fieldGroup);
        
        FieldGroup fieldGroupAfterRemove = typeManager.removeFieldDescriptors(id, Arrays.asList(new String[]{ fieldDescriptor1.getId()}));
        assertEquals(Long.valueOf(3), fieldGroupAfterRemove.getVersion());
        assertTrue(fieldGroupAfterRemove.getFieldGroupEntries().isEmpty());
        assertEquals(fieldGroupAfterRemove, typeManager.getFieldGroup(id, null));
        
        FieldGroup fieldGroupV1 = typeManager.getFieldGroup(id, Long.valueOf(1));
        assertEquals("alias1", fieldGroupV1.getFieldGroupEntry(fieldDescriptor1.getId()).getAlias());
        FieldGroup fieldGroupV2 = typeManager.getFieldGroup(id, Long.valueOf(2));
        assertEquals("alias2", fieldGroupV2.getFieldGroupEntry(fieldDescriptor1.getId()).getAlias());
    }

    @Test
    public void testRemoveLeavesOtherFieldGroupEntriesAlone() throws Exception {
        String id = "testRemoveLeavesOtherFieldGroupEntriesAlone";
        FieldGroup fieldGroup = typeManager.newFieldGroup(id);
        fieldGroup.setFieldGroupEntry(typeManager.newFieldGroupEntry(fieldDescriptor1.getId(), fieldDescriptor1.getVersion(), false, "alias1"));
        fieldGroup.setFieldGroupEntry(typeManager.newFieldGroupEntry(fieldDescriptor2.getId(), fieldDescriptor2.getVersion(), false, "alias2"));
        typeManager.createFieldGroup(fieldGroup);
        
        FieldGroup fieldGroupAfterRemove = typeManager.removeFieldDescriptors(id, Arrays.asList(new String[]{ fieldDescriptor1.getId()}));
        assertEquals(Long.valueOf(2), fieldGroupAfterRemove.getVersion());
        assertEquals("alias2", fieldGroupAfterRemove.getFieldGroupEntry(fieldDescriptor2.getId()).getAlias());
        assertNull(fieldGroupAfterRemove.getFieldGroupEntry(fieldDescriptor1.getId()));
        
        assertEquals(fieldGroupAfterRemove, typeManager.getFieldGroup(id, null));
    }

}
