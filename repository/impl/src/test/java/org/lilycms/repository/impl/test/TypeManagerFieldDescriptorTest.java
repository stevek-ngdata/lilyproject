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


import static org.junit.Assert.*;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilycms.repository.api.FieldDescriptor;
import org.lilycms.repository.api.FieldDescriptorNotFoundException;
import org.lilycms.repository.api.FieldDescriptorUpdateException;
import org.lilycms.repository.api.TypeManager;
import org.lilycms.repository.api.ValueType;
import org.lilycms.repository.impl.HBaseTypeManager;
import org.lilycms.repository.impl.IdGeneratorImpl;
import org.lilycms.testfw.TestHelper;

public class TypeManagerFieldDescriptorTest {

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

    private TypeManager typeManager;

    @Before
    public void setUp() throws Exception {
        typeManager = new HBaseTypeManager(new IdGeneratorImpl(), TEST_UTIL.getConfiguration());
    }

    @After
    public void tearDown() throws Exception {
    }
    
    @Test
    public void testCreate() throws Exception {
        String name = "testCreate";
        ValueType valueType = typeManager.getValueType("STRING", false, false);
        FieldDescriptor fieldDescriptor = typeManager.newFieldDescriptor(valueType , name);
        fieldDescriptor = typeManager.createFieldDescriptor(fieldDescriptor);
        assertEquals(Long.valueOf(1), fieldDescriptor.getVersion());
        assertEquals(fieldDescriptor, typeManager.getFieldDescriptor(fieldDescriptor.getId(), null));
    }
    
    @Test
    public void testCreateIgnoresVersion() throws Exception {
        String name = "testCreateIgnoresVersion";
        ValueType valueType = typeManager.getValueType("STRING", false, false);
        FieldDescriptor fieldDescriptor = typeManager.newFieldDescriptor(valueType , name);
        fieldDescriptor.setVersion(Long.valueOf(5));
        fieldDescriptor = typeManager.createFieldDescriptor(fieldDescriptor);
        assertEquals(Long.valueOf(1), fieldDescriptor.getVersion());
        assertEquals(fieldDescriptor, typeManager.getFieldDescriptor(fieldDescriptor.getId(), null));
    }
    
    @Test
    public void testCreateIgnoresGivenId() throws Exception {
        String id = "testCreateExistingFails";
        ValueType valueType = typeManager.getValueType("STRING", false, false);
        FieldDescriptor fieldDescriptor = typeManager.newFieldDescriptor(id, valueType , "aGlobalName");
        fieldDescriptor = typeManager.createFieldDescriptor(fieldDescriptor);
        assertFalse(fieldDescriptor.getId().equals(typeManager.createFieldDescriptor(fieldDescriptor.clone())));
    }

    @Test
    public void testUpdate() throws Exception {
        String name = "testUpdate";
        ValueType valueType = typeManager.getValueType("STRING", false, false);
        FieldDescriptor fieldDescriptorCreate = typeManager.newFieldDescriptor(valueType , name);
        fieldDescriptorCreate = typeManager.createFieldDescriptor(fieldDescriptorCreate);
        
        FieldDescriptor fieldDescriptorNewName = typeManager.newFieldDescriptor(fieldDescriptorCreate.getId(), valueType , "newName");
        fieldDescriptorNewName = typeManager.updateFieldDescriptor(fieldDescriptorNewName);
        assertEquals(fieldDescriptorCreate.getId(), fieldDescriptorNewName.getId());
        assertEquals(Long.valueOf(2), fieldDescriptorNewName.getVersion());
        assertEquals(fieldDescriptorNewName, typeManager.getFieldDescriptor(fieldDescriptorCreate.getId(), null));
        
    }
    
    @Test
    public void testUpdateVersions() throws Exception {
        String name = "testUpdateVersions";
        ValueType valueType = typeManager.getValueType("STRING", false, false);
        FieldDescriptor fieldDescriptorCreate = typeManager.newFieldDescriptor(valueType , name);
        fieldDescriptorCreate = typeManager.createFieldDescriptor(fieldDescriptorCreate);
        
        FieldDescriptor fieldDescriptorNewName = typeManager.newFieldDescriptor(fieldDescriptorCreate.getId(), valueType , "newName");
        // Update should ignore version
        fieldDescriptorNewName.setVersion(Long.valueOf(99));
        FieldDescriptor updatedFieldDescriptor = typeManager.updateFieldDescriptor(fieldDescriptorNewName);
        assertEquals(Long.valueOf(2), updatedFieldDescriptor.getVersion());
        assertEquals(updatedFieldDescriptor, typeManager.getFieldDescriptor(fieldDescriptorCreate.getId(), null));

    }
    
    @Test
    public void testRetrieveOlderVersions() throws Exception {
        String name = "testRetrieveOlderVersions";
        ValueType valueType = typeManager.getValueType("STRING", false, false);
        FieldDescriptor fieldDescriptorCreate = typeManager.newFieldDescriptor(valueType , name);
        fieldDescriptorCreate = typeManager.createFieldDescriptor(fieldDescriptorCreate);
        FieldDescriptor fieldDescriptorNewName = typeManager.newFieldDescriptor(fieldDescriptorCreate.getId(), valueType , "newName");
        fieldDescriptorNewName = typeManager.updateFieldDescriptor(fieldDescriptorNewName);
        
        assertEquals(Long.valueOf(2), fieldDescriptorNewName.getVersion());
        assertEquals(fieldDescriptorNewName, typeManager.getFieldDescriptor(fieldDescriptorCreate.getId(), Long.valueOf(2)));

        assertEquals(fieldDescriptorCreate, typeManager.getFieldDescriptor(fieldDescriptorCreate.getId(), Long.valueOf(1)));
    }
    
    @Test
    public void testRetrieveTooRecentVersionFails() throws Exception {
        String name = "testRetrieveTooRecentVersionFails";
        ValueType valueType = typeManager.getValueType("STRING", false, false);
        FieldDescriptor fieldDescriptorCreate = typeManager.newFieldDescriptor(valueType , name);
        fieldDescriptorCreate = typeManager.createFieldDescriptor(fieldDescriptorCreate);
        FieldDescriptor fieldDescriptorNewName = typeManager.newFieldDescriptor(fieldDescriptorCreate.getId(), valueType , "newName");
        typeManager.updateFieldDescriptor(fieldDescriptorNewName);
        
        try {
            typeManager.getFieldDescriptor(fieldDescriptorCreate.getId(), Long.valueOf(3));
            fail("Retrieving a too recent/non-existing version should fail.");
        } catch (FieldDescriptorNotFoundException expected) {
        }

    }
    
    @Test
    public void testIdempotentUpdateReturnsLatestVersionNumber() throws Exception {
        String name = "testIdempotentUpdateReturnsLatestVersionNumber";
        ValueType valueType = typeManager.getValueType("STRING", false, false);
        FieldDescriptor fieldDescriptorCreate = typeManager.newFieldDescriptor(valueType , name);
        fieldDescriptorCreate = typeManager.createFieldDescriptor(fieldDescriptorCreate);
        
        FieldDescriptor fieldDescriptorNewName = typeManager.newFieldDescriptor(fieldDescriptorCreate.getId(), valueType , "newName");
        typeManager.updateFieldDescriptor(fieldDescriptorNewName);
        
        FieldDescriptor idempotentUpdate = typeManager.updateFieldDescriptor(fieldDescriptorNewName);
        assertEquals(Long.valueOf(2), idempotentUpdate.getVersion());
        assertEquals(idempotentUpdate, typeManager.getFieldDescriptor(fieldDescriptorCreate.getId(), null));
    }
    
    @Test
    public void testUpdateValueTypeFails() throws Exception {
        String name = "testUpdateValueTypeFails";
        ValueType valueType = typeManager.getValueType("STRING", false, false);
        FieldDescriptor fieldDescriptor = typeManager.newFieldDescriptor(valueType , name);
        fieldDescriptor = typeManager.createFieldDescriptor(fieldDescriptor);
        
        fieldDescriptor.setValueType(typeManager.getValueType("INTEGER", false, false));
        try {
            typeManager.updateFieldDescriptor(fieldDescriptor);
            fail("Changing the valueType of a fieldDescriptor is not allowed.");
        } catch (FieldDescriptorUpdateException e) {
        }
    }

}
