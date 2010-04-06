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


import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilycms.repository.api.FieldDescriptor;
import org.lilycms.repository.api.FieldDescriptorExistsException;
import org.lilycms.repository.api.FieldDescriptorNotFoundException;
import org.lilycms.repository.api.FieldDescriptorUpdateException;
import org.lilycms.repository.api.TypeManager;
import org.lilycms.repository.api.ValueType;
import org.lilycms.repository.impl.FieldDescriptorImpl;
import org.lilycms.repository.impl.HBaseTypeManager;
import org.lilycms.repository.impl.IdGeneratorImpl;
import org.lilycms.repository.impl.RecordTypeImpl;
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
        String id = "testCreate";
        ValueType valueType = typeManager.getValueType("STRING", false, false);
        FieldDescriptor fieldDescriptor = typeManager.newFieldDescriptor(id, valueType , "aGlobalName");
        typeManager.createFieldDescriptor(fieldDescriptor);
        fieldDescriptor.setVersion(Long.valueOf(1));
        assertEquals(fieldDescriptor, typeManager.getFieldDescriptor(fieldDescriptor.getId(), null));
    }
    
    @Test
    public void testCreateIgnoresVersion() throws Exception {
        String id = "testCreateIgnoresVersion";
        ValueType valueType = typeManager.getValueType("STRING", false, false);
        FieldDescriptor fieldDescriptor = typeManager.newFieldDescriptor(id, valueType , "aGlobalName");
        fieldDescriptor.setVersion(Long.valueOf(5));
        typeManager.createFieldDescriptor(fieldDescriptor);
        fieldDescriptor.setVersion(Long.valueOf(1));
        assertEquals(fieldDescriptor, typeManager.getFieldDescriptor(fieldDescriptor.getId(), null));
    }
    
    @Test
    public void testCreateExistingFails() throws Exception {
        String id = "testCreateExistingFails";
        ValueType valueType = typeManager.getValueType("STRING", false, false);
        FieldDescriptor fieldDescriptor = typeManager.newFieldDescriptor(id, valueType , "aGlobalName");
        typeManager.createFieldDescriptor(fieldDescriptor);
        try {
            typeManager.createFieldDescriptor(fieldDescriptor);
            fail("Creating an existing fieldDescriptor is not allowed. Use update instead.");
        } catch (FieldDescriptorExistsException expected) {
        }
    }

    @Test
    public void testUpdate() throws Exception {
        String id = "testUpdate";
        ValueType valueType = typeManager.getValueType("STRING", false, false);
        FieldDescriptor fieldDescriptorCreate = typeManager.newFieldDescriptor(id, valueType , "aGlobalName");
        typeManager.createFieldDescriptor(fieldDescriptorCreate);
        
        FieldDescriptor fieldDescriptorGlobalName = typeManager.newFieldDescriptor(id, valueType , "newGlobalName");
        typeManager.updateFieldDescriptor(fieldDescriptorGlobalName);
        fieldDescriptorGlobalName.setVersion(Long.valueOf(2));
        assertEquals(fieldDescriptorGlobalName, typeManager.getFieldDescriptor(id, null));
        
    }
    
    @Test
    public void testUpdateVersions() throws Exception {
        String id = "testUpdateVersions";
        ValueType valueType = typeManager.getValueType("STRING", false, false);
        FieldDescriptor fieldDescriptorCreate = typeManager.newFieldDescriptor(id, valueType , "aGlobalName");
        typeManager.createFieldDescriptor(fieldDescriptorCreate);
        
        FieldDescriptor fieldDescriptorGlobalName = typeManager.newFieldDescriptor(id, valueType , "newGlobalName");
        // Update should ignore version
        fieldDescriptorGlobalName.setVersion(Long.valueOf(99));
        FieldDescriptor updatedFieldDescriptor = typeManager.updateFieldDescriptor(fieldDescriptorGlobalName);
        assertEquals(Long.valueOf(2), updatedFieldDescriptor.getVersion());
        fieldDescriptorGlobalName.setVersion(Long.valueOf(2));
        assertEquals(fieldDescriptorGlobalName, typeManager.getFieldDescriptor(id, null));

    }
    
    @Test
    public void testRetrieveOlderVersions() throws Exception {
        String id = "testRetrieveOlderVersions";
        ValueType valueType = typeManager.getValueType("STRING", false, false);
        FieldDescriptor fieldDescriptorCreate = typeManager.newFieldDescriptor(id, valueType , "aGlobalName");
        typeManager.createFieldDescriptor(fieldDescriptorCreate);
        FieldDescriptor fieldDescriptorGlobalName = typeManager.newFieldDescriptor(id, valueType , "newGlobalName");
        typeManager.updateFieldDescriptor(fieldDescriptorGlobalName);
        
        fieldDescriptorGlobalName.setVersion(Long.valueOf(2));
        assertEquals(fieldDescriptorGlobalName, typeManager.getFieldDescriptor(id, Long.valueOf(2)));

        fieldDescriptorCreate.setVersion(Long.valueOf(1));
        assertEquals(fieldDescriptorCreate, typeManager.getFieldDescriptor(id, Long.valueOf(1)));
    }
    
    @Test
    public void testRetrieveTooRecentVersionFails() throws Exception {
        String id = "testRetrieveTooRecentVersionFails";
        ValueType valueType = typeManager.getValueType("STRING", false, false);
        FieldDescriptor fieldDescriptorCreate = typeManager.newFieldDescriptor(id, valueType , "aGlobalName");
        typeManager.createFieldDescriptor(fieldDescriptorCreate);
        FieldDescriptor fieldDescriptorGlobalName = typeManager.newFieldDescriptor(id, valueType , "newGlobalName");
        typeManager.updateFieldDescriptor(fieldDescriptorGlobalName);
        
        try {
            typeManager.getFieldDescriptor(id, Long.valueOf(3));
            fail("Retrieving a too recent/non-existing version should fail.");
        } catch (FieldDescriptorNotFoundException expected) {
        }

    }
    
    @Test
    public void testIdempotentUpdateReturnsLatestVersionNumber() throws Exception {
        String id = "testIdempotentUpdateReturnsLatestVersionNumber";
        ValueType valueType = typeManager.getValueType("STRING", false, false);
        FieldDescriptor fieldDescriptorCreate = typeManager.newFieldDescriptor(id, valueType , "aGlobalName");
        typeManager.createFieldDescriptor(fieldDescriptorCreate);
        
        FieldDescriptor fieldDescriptorGlobalName = typeManager.newFieldDescriptor(id, valueType , "newGlobalName");
        typeManager.updateFieldDescriptor(fieldDescriptorGlobalName);
        
        FieldDescriptor idempotentUpdate = typeManager.updateFieldDescriptor(fieldDescriptorGlobalName);
        assertEquals(Long.valueOf(2), idempotentUpdate.getVersion());
        fieldDescriptorGlobalName.setVersion(Long.valueOf(2));
        assertEquals(fieldDescriptorGlobalName, typeManager.getFieldDescriptor(id, null));
    }
    
    @Test
    public void testUpdateValueTypeFails() throws Exception {
        String id = "testUpdateValueTypeFails";
        ValueType valueType = typeManager.getValueType("STRING", false, false);
        FieldDescriptor fieldDescriptor = typeManager.newFieldDescriptor(id, valueType , "aGlobalName");
        typeManager.createFieldDescriptor(fieldDescriptor);
        
        fieldDescriptor.setValueType(typeManager.getValueType("INTEGER", false, false));
        try {
            typeManager.updateFieldDescriptor(fieldDescriptor);
            fail("Changing the valueType of a fieldDescriptor is not allowed.");
        } catch (FieldDescriptorUpdateException e) {
        }
    }

}
