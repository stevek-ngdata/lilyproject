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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilycms.repository.api.FieldDescriptor;
import org.lilycms.repository.api.FieldGroup;
import org.lilycms.repository.api.FieldNotFoundException;
import org.lilycms.repository.api.IdGenerator;
import org.lilycms.repository.api.InvalidRecordException;
import org.lilycms.repository.api.Record;
import org.lilycms.repository.api.RecordExistsException;
import org.lilycms.repository.api.RecordId;
import org.lilycms.repository.api.RecordNotFoundException;
import org.lilycms.repository.api.RecordType;
import org.lilycms.repository.api.RecordTypeNotFoundException;
import org.lilycms.repository.api.Repository;
import org.lilycms.repository.api.TypeManager;
import org.lilycms.repository.api.Record.Scope;
import org.lilycms.repository.impl.HBaseRepository;
import org.lilycms.repository.impl.HBaseTypeManager;
import org.lilycms.repository.impl.IdGeneratorImpl;
import org.lilycms.testfw.TestHelper;

public class HBaseRepositoryTest {

    private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    private static IdGenerator idGenerator = new IdGeneratorImpl();
    private static TypeManager typeManager;
    private static Repository repository;
    private static FieldDescriptor fieldDescriptor1;
    private static FieldDescriptor fieldDescriptor1B;
    private static FieldDescriptor fieldDescriptor2;
    private static FieldDescriptor fieldDescriptor3;
    private static FieldGroup fieldGroup1;
    private static FieldGroup fieldGroup1B;
    private static FieldGroup fieldGroup2;
    private static FieldGroup fieldGroup3;
    private static RecordType recordType1;
    private static RecordType recordType1B;
    private static RecordType recordType2;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging();
        TEST_UTIL.startMiniCluster(1);
        typeManager = new HBaseTypeManager(idGenerator, TEST_UTIL.getConfiguration());
        repository = new HBaseRepository(typeManager, idGenerator, TEST_UTIL.getConfiguration());
        setupTypes();
    }

    private static void setupTypes() throws Exception {
        setupFieldDescriptors();
        setupFieldGroups();
        setupRecordTypes();
    }

    private static void setupFieldDescriptors() throws Exception {
        fieldDescriptor1 = typeManager.createFieldDescriptor(typeManager.newFieldDescriptor(typeManager.getValueType("STRING", false, false), "field1"));
        fieldDescriptor1B = typeManager.createFieldDescriptor(typeManager.newFieldDescriptor(typeManager.getValueType("STRING", false, false), "field1B"));
        fieldDescriptor2 = typeManager.createFieldDescriptor(typeManager.newFieldDescriptor(typeManager.getValueType("INTEGER", false, false), "field2"));
        fieldDescriptor3 = typeManager.createFieldDescriptor(typeManager.newFieldDescriptor(typeManager.getValueType("BOOLEAN", false, false), "field3"));
    }

    private static void setupFieldGroups() throws Exception {
        FieldGroup fieldGroup = typeManager.newFieldGroup("FG1");
        fieldGroup.setFieldGroupEntry(typeManager.newFieldGroupEntry(fieldDescriptor1.getId(), fieldDescriptor1.getVersion(), false, "alias1"));
        fieldGroup1 = typeManager.createFieldGroup(fieldGroup);
        fieldGroup = typeManager.newFieldGroup("FG1");
        fieldGroup.setFieldGroupEntry(typeManager.newFieldGroupEntry(fieldDescriptor1B.getId(), fieldDescriptor1B.getVersion(), false, "alias1B"));
        fieldGroup1B = typeManager.updateFieldGroup(fieldGroup);
        
        fieldGroup = typeManager.newFieldGroup("FG2");
        fieldGroup.setFieldGroupEntry(typeManager.newFieldGroupEntry(fieldDescriptor2.getId(), fieldDescriptor2.getVersion(), false, "alias2"));
        fieldGroup2 = typeManager.createFieldGroup(fieldGroup);
        fieldGroup = typeManager.newFieldGroup("FG3");
        fieldGroup.setFieldGroupEntry(typeManager.newFieldGroupEntry(fieldDescriptor3.getId(), fieldDescriptor3.getVersion(), false, "alias3"));
        fieldGroup3 = typeManager.createFieldGroup(fieldGroup);
    }
    
    private static void setupRecordTypes() throws Exception {
        recordType1 = typeManager.newRecordType("RT1");
        recordType1.setFieldGroupId(Scope.NON_VERSIONABLE,fieldGroup1.getId());
        recordType1.setFieldGroupVersion(Scope.NON_VERSIONABLE,fieldGroup1.getVersion());
        recordType1.setFieldGroupId(Scope.VERSIONABLE,fieldGroup2.getId());
        recordType1.setFieldGroupVersion(Scope.VERSIONABLE,fieldGroup2.getVersion());
        recordType1.setFieldGroupId(Scope.VERSIONABLE_MUTABLE,fieldGroup3.getId());
        recordType1.setFieldGroupVersion(Scope.VERSIONABLE_MUTABLE,fieldGroup3.getVersion());
        recordType1 = typeManager.createRecordType(recordType1);
        
        recordType1B = recordType1.clone();
        recordType1B.setFieldGroupId(Scope.NON_VERSIONABLE,fieldGroup1B.getId());
        recordType1B.setFieldGroupVersion(Scope.NON_VERSIONABLE,fieldGroup1B.getVersion());
        recordType1B = typeManager.updateRecordType(recordType1B);
        
        recordType2 = typeManager.newRecordType("RT2");
        recordType2.setFieldGroupId(Scope.NON_VERSIONABLE,fieldGroup2.getId());
        recordType2.setFieldGroupVersion(Scope.NON_VERSIONABLE,fieldGroup2.getVersion());
        recordType2.setFieldGroupId(Scope.VERSIONABLE,fieldGroup3.getId());
        recordType2.setFieldGroupVersion(Scope.VERSIONABLE,fieldGroup3.getVersion());
        recordType2.setFieldGroupId(Scope.VERSIONABLE_MUTABLE,fieldGroup1.getId());
        recordType2.setFieldGroupVersion(Scope.VERSIONABLE_MUTABLE,fieldGroup1.getVersion());
        recordType2 = typeManager.createRecordType(recordType2);
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
    public void testRecordCreateWithoutRecordType() throws Exception {
        Record record = repository.newRecord(idGenerator.newRecordId());
        try {
            record = repository.create(record);
        } catch (InvalidRecordException expected){
        }
    }

    @Test
    public void testRecordUpdateWithoutRecordType() throws Exception {
        Record record = createDefaultRecord(idGenerator.newRecordId());
        Record updateRecord = repository.newRecord(record.getId());
        try {
            record = repository.update(updateRecord);
        } catch (InvalidRecordException expected){
        }
    }

    @Test
    public void testEmptyRecordCreate() throws Exception {
        Record record = repository.newRecord(idGenerator.newRecordId());
        record.setRecordType(recordType1.getId(), null);
        try {
            record = repository.create(record);
        } catch (InvalidRecordException expected){
        }
    }
    
    @Test
    public void testCreate() throws Exception {
        RecordId recordId = idGenerator.newRecordId();
        Record createdRecord = createDefaultRecord(recordId);
        
        assertEquals(Long.valueOf(1), createdRecord.getVersion());
        assertEquals("value1", createdRecord.getField(Scope.NON_VERSIONABLE,fieldDescriptor1.getName()));
        assertEquals(123, createdRecord.getField(Scope.VERSIONABLE,fieldDescriptor2.getName()));
        assertTrue((Boolean)createdRecord.getField(Scope.VERSIONABLE_MUTABLE,fieldDescriptor3.getName()));
        assertEquals(recordType1.getId(), createdRecord.getRecordTypeId());
        assertEquals(Long.valueOf(1), createdRecord.getRecordTypeVersion());
        assertEquals(recordType1.getId(), createdRecord.getRecordTypeId(Scope.NON_VERSIONABLE));
        assertEquals(Long.valueOf(1), createdRecord.getRecordTypeVersion(Scope.NON_VERSIONABLE));
        assertEquals(recordType1.getId(), createdRecord.getRecordTypeId(Scope.VERSIONABLE));
        assertEquals(Long.valueOf(1), createdRecord.getRecordTypeVersion(Scope.VERSIONABLE));
        assertEquals(recordType1.getId(), createdRecord.getRecordTypeId(Scope.VERSIONABLE_MUTABLE));
        assertEquals(Long.valueOf(1), createdRecord.getRecordTypeVersion(Scope.VERSIONABLE_MUTABLE));
        
        assertEquals(createdRecord, repository.read(recordId));
    }
    
    private Record createDefaultRecord(RecordId recordId) throws Exception {
        Record record = repository.newRecord(recordId);
        record.setRecordType(recordType1.getId(), recordType1.getVersion());
        record.setField(Scope.NON_VERSIONABLE,fieldDescriptor1.getName(), "value1");
        record.setField(Scope.VERSIONABLE,fieldDescriptor2.getName(), 123);
        record.setField(Scope.VERSIONABLE_MUTABLE,fieldDescriptor3.getName(), true);
        return repository.create(record);
    }
    
    @Test 
    public void testCreateExistingRecordFails() throws Exception {
        Record record = createDefaultRecord(idGenerator.newRecordId());
        
        try {
            repository.create(record);
            fail();
        } catch (RecordExistsException expected) {
        }
    }
    
    @Test
    public void testCreateWithNonExistingRecordTypeFails() throws Exception {
        Record record = repository.newRecord(idGenerator.newRecordId());
        record.setRecordType("nonExistingRecordType", null);
        record.setField(Scope.NON_VERSIONABLE,fieldDescriptor1.getId(), "value1");
        try {
            repository.create(record);
            fail();
        } catch (RecordTypeNotFoundException expected) {
        }
    }
    
    @Test
    public void testCreateUsesLatestRecordType() throws Exception {
        Record record = repository.newRecord(idGenerator.newRecordId());
        record.setRecordType(recordType1.getId(), null);
        record.setField(Scope.NON_VERSIONABLE,fieldDescriptor1.getName(), "value1");
        Record createdRecord = repository.create(record);
        assertEquals(recordType1.getId(), createdRecord.getRecordTypeId());
        assertEquals(Long.valueOf(2), createdRecord.getRecordTypeVersion());
        assertEquals(recordType1.getId(), createdRecord.getRecordTypeId(Scope.NON_VERSIONABLE));
        assertEquals(Long.valueOf(2), createdRecord.getRecordTypeVersion(Scope.NON_VERSIONABLE));
        assertNull(createdRecord.getRecordTypeId(Scope.VERSIONABLE));
        assertNull(createdRecord.getRecordTypeVersion(Scope.VERSIONABLE));
        assertNull(createdRecord.getRecordTypeId(Scope.VERSIONABLE_MUTABLE));
        assertNull(createdRecord.getRecordTypeVersion(Scope.VERSIONABLE_MUTABLE));
        
        assertEquals(createdRecord, repository.read(record.getId()));
    }
    
    @Test
    public void testCreateVariant() throws Exception {
        Record record = createDefaultRecord(idGenerator.newRecordId());
        
        Map<String, String> variantProperties = new HashMap<String, String>();
        variantProperties.put("dimension1", "dimval1");
        Record variant = repository.newRecord(idGenerator.newRecordId(record.getId(), variantProperties ));
        variant.setRecordType(recordType1.getId(), null);
        variant.setField(Scope.NON_VERSIONABLE,fieldDescriptor1.getName(), "value2");
        variant.setField(Scope.VERSIONABLE,fieldDescriptor2.getName(), 567);
        variant.setField(Scope.VERSIONABLE_MUTABLE,fieldDescriptor3.getName(), false);
        
        Record createdVariant = repository.create(variant);
        
        assertEquals(Long.valueOf(1), createdVariant.getVersion());
        assertEquals("value2", createdVariant.getField(Scope.NON_VERSIONABLE,fieldDescriptor1.getName()));
        assertEquals(567, createdVariant.getField(Scope.VERSIONABLE,fieldDescriptor2.getName()));
        assertFalse((Boolean)createdVariant.getField(Scope.VERSIONABLE_MUTABLE,fieldDescriptor3.getName()));
        
        assertEquals(createdVariant, repository.read(variant.getId()));
    }
    
    @Test
    public void testUpdate() throws Exception {
        Record record = createDefaultRecord(idGenerator.newRecordId());
        Record updateRecord = record.clone();
        updateRecord.setField(Scope.NON_VERSIONABLE,fieldDescriptor1.getName(), "value2");
        updateRecord.setField(Scope.VERSIONABLE,fieldDescriptor2.getName(), 789);
        updateRecord.setField(Scope.VERSIONABLE_MUTABLE,fieldDescriptor3.getName(), false);
        
        Record updatedRecord = repository.update(updateRecord);
        
        assertEquals(Long.valueOf(2), updatedRecord.getVersion());
        assertEquals("value2", updatedRecord.getField(Scope.NON_VERSIONABLE,fieldDescriptor1.getName()));
        assertEquals(789, updatedRecord.getField(Scope.VERSIONABLE,fieldDescriptor2.getName()));
        assertEquals(false, updatedRecord.getField(Scope.VERSIONABLE_MUTABLE,fieldDescriptor3.getName()));
        
        assertEquals(updatedRecord, repository.read(record.getId()));
    }
    
    @Test
    public void testUpdateOnlyOneField() throws Exception {
        Record record = createDefaultRecord(idGenerator.newRecordId());
        Record updateRecord = repository.newRecord(record.getId());
        updateRecord.setRecordType(record.getRecordTypeId(), record.getRecordTypeVersion());
        updateRecord.setField(Scope.NON_VERSIONABLE,fieldDescriptor1.getName(), "value2");
        
        Record updatedRecord = repository.update(updateRecord);
        
        assertEquals(Long.valueOf(1), updatedRecord.getVersion());
        assertEquals("value2", updatedRecord.getField(Scope.NON_VERSIONABLE,fieldDescriptor1.getName()));
        try {
            updatedRecord.getField(Scope.VERSIONABLE,fieldDescriptor2.getName());
            fail();
        } catch (FieldNotFoundException expected) {
        }
        try {
            updatedRecord.getField(Scope.VERSIONABLE_MUTABLE,fieldDescriptor3.getName());
            fail();
        } catch (FieldNotFoundException expected) {
        }
        
        updatedRecord = repository.read(record.getId());
        assertEquals("value2", updatedRecord.getField(Scope.NON_VERSIONABLE,fieldDescriptor1.getName()));
        assertEquals(123, updatedRecord.getField(Scope.VERSIONABLE,fieldDescriptor2.getName()));
        assertEquals(true, updatedRecord.getField(Scope.VERSIONABLE_MUTABLE,fieldDescriptor3.getName()));
    }
    
    @Test
    public void testEmptyUpdate() throws Exception {
        Record record = createDefaultRecord(idGenerator.newRecordId());
        Record updateRecord = repository.newRecord(record.getId());
        updateRecord.setRecordType(record.getRecordTypeId(), record.getRecordTypeVersion());
        
        Record updatedRecord = repository.update(updateRecord);
        
        assertEquals(Long.valueOf(1), updatedRecord.getVersion());
        try {
            updatedRecord.getField(Scope.NON_VERSIONABLE,fieldDescriptor1.getId());
            fail();
        } catch (FieldNotFoundException expected) {
        }
        try {
            updatedRecord.getField(Scope.VERSIONABLE,fieldDescriptor2.getId());
            fail();
        } catch (FieldNotFoundException expected) {
        }
        try {
            updatedRecord.getField(Scope.VERSIONABLE_MUTABLE,fieldDescriptor3.getId());
            fail();
        } catch (FieldNotFoundException expected) {
        }
        
        assertEquals(record, repository.read(record.getId()));
    }
    
    @Test
    public void testIdempotentUpdate() throws Exception {
        Record record = createDefaultRecord(idGenerator.newRecordId());
        Record updateRecord = record.clone();
        
        Record updatedRecord = repository.update(updateRecord);
        
        assertEquals(Long.valueOf(1), updatedRecord.getVersion());
        assertEquals("value1", updatedRecord.getField(Scope.NON_VERSIONABLE,fieldDescriptor1.getName()));
        assertEquals(123, updatedRecord.getField(Scope.VERSIONABLE,fieldDescriptor2.getName()));
        assertEquals(true, updatedRecord.getField(Scope.VERSIONABLE_MUTABLE,fieldDescriptor3.getName()));
        
        assertEquals(record, repository.read(record.getId()));
    }
    
    @Test
    public void testUpdateIgnoresVersion() throws Exception {
        Record record = createDefaultRecord(idGenerator.newRecordId());
        Record updateRecord = record.clone();
        updateRecord.setVersion(Long.valueOf(99));
        updateRecord.setField(Scope.NON_VERSIONABLE,fieldDescriptor1.getName(), "value2");
        
        Record updatedRecord = repository.update(updateRecord);
        
        assertEquals(Long.valueOf(1), updatedRecord.getVersion());
        
        assertEquals(updatedRecord, repository.read(record.getId()));
    }
    
    @Test
    public void testUpdateNonVersionable() throws Exception {
        Record record = createDefaultRecord(idGenerator.newRecordId());
        Record updateRecord = repository.newRecord(record.getId());
        updateRecord.setRecordType(record.getRecordTypeId(), null);
        updateRecord.setField(Scope.NON_VERSIONABLE,fieldDescriptor1.getName(), "aNewValue");
        repository.update(updateRecord);
        
        Record readRecord = repository.read(record.getId());
        assertEquals(Long.valueOf(1), readRecord.getVersion());
        assertEquals("aNewValue", readRecord.getField(Scope.NON_VERSIONABLE,fieldDescriptor1.getName()));
    }
    
    
    @Test
    public void testReadOlderVersions() throws Exception {
        Record record = createDefaultRecord(idGenerator.newRecordId());
        Record updateRecord = record.clone();
        updateRecord.setField(Scope.NON_VERSIONABLE,fieldDescriptor1.getName(), "value2");
        updateRecord.setField(Scope.VERSIONABLE,fieldDescriptor2.getName(), 789);
        updateRecord.setField(Scope.VERSIONABLE_MUTABLE,fieldDescriptor3.getName(), false);
        
        repository.update(updateRecord);
        
        record.setField(Scope.NON_VERSIONABLE,fieldDescriptor1.getName(), "value2");
        assertEquals(record, repository.read(record.getId(), Long.valueOf(1)));
    }
    
    @Test
    public void testReadNonExistingRecord() throws Exception {
        try {
            repository.read(idGenerator.newRecordId());
            fail();
        } catch (RecordNotFoundException expected) {
        }
    }

    @Test
    public void testReadTooRecentRecord() throws Exception {
        Record record = createDefaultRecord(idGenerator.newRecordId());
        try {
            repository.read(record.getId(), Long.valueOf(2));
            fail();
        } catch (RecordNotFoundException expected) {
        }
    }
    
    @Test
    public void testReadSpecificFields() throws Exception {
        Record record = createDefaultRecord(idGenerator.newRecordId());
        Record readRecord = repository.read(record.getId(), Arrays.asList(new String[]{fieldDescriptor1.getId()}), Arrays.asList(new String[]{fieldDescriptor2.getId()}), Arrays.asList(new String[]{fieldDescriptor3.getId()}));
        assertEquals(repository.read(record.getId()), readRecord);
    }
    
    
    @Test
    public void testUpdateWithNewRecordTypeVersion() throws Exception {
        Record record = createDefaultRecord(idGenerator.newRecordId());
        Record updateRecord = repository.newRecord(record.getId());
        updateRecord.setRecordType(recordType1B.getId(), recordType1B.getVersion());
        updateRecord.setField(Scope.NON_VERSIONABLE,fieldDescriptor1.getName(), "value2");
        updateRecord.setField(Scope.VERSIONABLE,fieldDescriptor2.getName(), 789);
        updateRecord.setField(Scope.VERSIONABLE_MUTABLE,fieldDescriptor3.getName(), false);
        
        Record updatedRecord = repository.update(updateRecord);
        assertEquals(recordType1B.getId(),updatedRecord.getRecordTypeId());
        assertEquals(recordType1B.getVersion(),updatedRecord.getRecordTypeVersion());
        assertEquals(recordType1B.getId(),updatedRecord.getRecordTypeId(Scope.NON_VERSIONABLE));
        assertEquals(recordType1B.getVersion(),updatedRecord.getRecordTypeVersion(Scope.NON_VERSIONABLE));
        assertEquals(recordType1B.getId(),updatedRecord.getRecordTypeId(Scope.VERSIONABLE));
        assertEquals(recordType1B.getVersion(),updatedRecord.getRecordTypeVersion(Scope.VERSIONABLE));
        assertEquals(recordType1B.getId(),updatedRecord.getRecordTypeId(Scope.VERSIONABLE_MUTABLE));
        assertEquals(recordType1B.getVersion(),updatedRecord.getRecordTypeVersion(Scope.VERSIONABLE_MUTABLE));
        
        Record recordV1 = repository.read(record.getId(), Long.valueOf(1));
        assertEquals(recordType1B.getId(),recordV1.getRecordTypeId());
        assertEquals(recordType1B.getVersion(),recordV1.getRecordTypeVersion());
        assertEquals(recordType1B.getId(),recordV1.getRecordTypeId(Scope.NON_VERSIONABLE));
        assertEquals(recordType1B.getVersion(),recordV1.getRecordTypeVersion(Scope.NON_VERSIONABLE));
        assertEquals(recordType1.getId(),recordV1.getRecordTypeId(Scope.VERSIONABLE));
        assertEquals(recordType1.getVersion(),recordV1.getRecordTypeVersion(Scope.VERSIONABLE));
        assertEquals(recordType1.getId(),recordV1.getRecordTypeId(Scope.VERSIONABLE_MUTABLE));
        assertEquals(recordType1.getVersion(),recordV1.getRecordTypeVersion(Scope.VERSIONABLE_MUTABLE));
    }
    
    @Test
    public void testUpdateWithNewRecordTypeVersionOnlyOneFieldUpdated() throws Exception {
        Record record = createDefaultRecord(idGenerator.newRecordId());
        Record updateRecord = repository.newRecord(record.getId());
        updateRecord.setRecordType(recordType1B.getId(), recordType1B.getVersion());
        updateRecord.setField(Scope.VERSIONABLE,fieldDescriptor2.getName(), 789);
        
        Record updatedRecord = repository.update(updateRecord);
        assertEquals(recordType1B.getId(),updatedRecord.getRecordTypeId());
        assertEquals(recordType1B.getVersion(),updatedRecord.getRecordTypeVersion());
        assertEquals(recordType1B.getId(),updatedRecord.getRecordTypeId(Scope.VERSIONABLE));
        assertEquals(recordType1B.getVersion(),updatedRecord.getRecordTypeVersion(Scope.VERSIONABLE));
        
        Record readRecord = repository.read(record.getId());
        assertEquals(recordType1B.getId(),updatedRecord.getRecordTypeId());
        assertEquals(recordType1B.getVersion(),updatedRecord.getRecordTypeVersion());
        assertEquals(recordType1.getId(),readRecord.getRecordTypeId(Scope.NON_VERSIONABLE));
        assertEquals(recordType1.getVersion(),readRecord.getRecordTypeVersion(Scope.NON_VERSIONABLE));
        assertEquals(recordType1B.getId(),updatedRecord.getRecordTypeId(Scope.VERSIONABLE));
        assertEquals(recordType1B.getVersion(),updatedRecord.getRecordTypeVersion(Scope.VERSIONABLE));
        assertEquals(recordType1.getId(),readRecord.getRecordTypeId(Scope.VERSIONABLE_MUTABLE));
        assertEquals(recordType1.getVersion(),readRecord.getRecordTypeVersion(Scope.VERSIONABLE_MUTABLE));
    }
    
    @Test
    public void testUpdateWithNewRecordType() throws Exception {
        Record record = createDefaultRecord(idGenerator.newRecordId());
        Record updateRecord = repository.newRecord(record.getId());
        updateRecord.setRecordType(recordType2.getId(), recordType2.getVersion());
        updateRecord.setField(Scope.NON_VERSIONABLE,fieldDescriptor2.getName(), 789);
        updateRecord.setField(Scope.VERSIONABLE,fieldDescriptor3.getName(), false);
        updateRecord.setField(Scope.VERSIONABLE_MUTABLE,fieldDescriptor1.getName(), "value2");
        
        Record updatedRecord = repository.update(updateRecord);
        assertEquals(recordType2.getId(),updatedRecord.getRecordTypeId());
        assertEquals(recordType2.getVersion(),updatedRecord.getRecordTypeVersion());
        assertEquals(recordType2.getId(),updatedRecord.getRecordTypeId(Scope.NON_VERSIONABLE));
        assertEquals(recordType2.getVersion(),updatedRecord.getRecordTypeVersion(Scope.NON_VERSIONABLE));
        assertEquals(recordType2.getId(),updatedRecord.getRecordTypeId(Scope.VERSIONABLE));
        assertEquals(recordType2.getVersion(),updatedRecord.getRecordTypeVersion(Scope.VERSIONABLE));
        assertEquals(recordType2.getId(),updatedRecord.getRecordTypeId(Scope.VERSIONABLE_MUTABLE));
        assertEquals(recordType2.getVersion(),updatedRecord.getRecordTypeVersion(Scope.VERSIONABLE_MUTABLE));
        
        assertEquals(1, updatedRecord.getFields(Scope.NON_VERSIONABLE).size());
        assertEquals(1, updatedRecord.getFields(Scope.VERSIONABLE).size());
        assertEquals(1, updatedRecord.getFields(Scope.VERSIONABLE_MUTABLE).size());

        Record readRecord = repository.read(record.getId());
        // Nothing got deleted
        assertEquals(2, readRecord.getFields(Scope.NON_VERSIONABLE).size());
        assertEquals(2, readRecord.getFields(Scope.VERSIONABLE).size());
        assertEquals(2, readRecord.getFields(Scope.VERSIONABLE_MUTABLE).size());
        assertEquals("value1", readRecord.getField(Scope.NON_VERSIONABLE,fieldDescriptor1.getName()));
        assertEquals(789, readRecord.getField(Scope.NON_VERSIONABLE,fieldDescriptor2.getName()));
        assertEquals(123, readRecord.getField(Scope.VERSIONABLE,fieldDescriptor2.getName()));
        assertFalse((Boolean)readRecord.getField(Scope.VERSIONABLE,fieldDescriptor3.getName()));
        assertTrue((Boolean)readRecord.getField(Scope.VERSIONABLE_MUTABLE,fieldDescriptor3.getName()));
        assertEquals("value2", readRecord.getField(Scope.VERSIONABLE_MUTABLE,fieldDescriptor1.getName()));
    }
    
    @Test
    public void testDeleteField() throws Exception {
        Record record = createDefaultRecord(idGenerator.newRecordId());
        Record deleteRecord = repository.newRecord(record.getId());
        deleteRecord.setRecordType(record.getRecordTypeId(), null);
        deleteRecord.addFieldsToDelete(Scope.NON_VERSIONABLE,Arrays.asList(new String[]{fieldDescriptor1.getName()}));
        deleteRecord.addFieldsToDelete(Scope.VERSIONABLE,Arrays.asList(new String[]{fieldDescriptor2.getName()}));
        deleteRecord.addFieldsToDelete(Scope.VERSIONABLE_MUTABLE,Arrays.asList(new String[]{fieldDescriptor3.getName()}));
        
        repository.update(deleteRecord);
        Record readRecord = repository.read(record.getId());
        assertTrue(readRecord.getFields(Scope.NON_VERSIONABLE).isEmpty());
        assertTrue(readRecord.getFields(Scope.VERSIONABLE).isEmpty());
        assertTrue(readRecord.getFields(Scope.VERSIONABLE_MUTABLE).isEmpty());
    }
    
    @Test
    public void testDeleteFieldsNoLongerInRecordType() throws Exception {
        Record record = createDefaultRecord(idGenerator.newRecordId());
        Record updateRecord = repository.newRecord(record.getId());
        updateRecord.setRecordType(recordType2.getId(), recordType2.getVersion());
        updateRecord.setField(Scope.NON_VERSIONABLE,fieldDescriptor2.getName(), 789);
        updateRecord.setField(Scope.VERSIONABLE,fieldDescriptor3.getName(), false);
        updateRecord.setField(Scope.VERSIONABLE_MUTABLE,fieldDescriptor1.getName(), "value2");
        
        repository.update(updateRecord);
        
        Record deleteRecord = repository.newRecord(record.getId());
        deleteRecord.setRecordType(recordType1.getId(), recordType1.getVersion());
        deleteRecord.addFieldsToDelete(Scope.NON_VERSIONABLE,Arrays.asList(new String[]{fieldDescriptor1.getName()}));
        repository.update(deleteRecord);
        
        Record readRecord = repository.read(record.getId());
        assertEquals(Long.valueOf(2), readRecord.getVersion());
        assertEquals(1, readRecord.getFields(Scope.NON_VERSIONABLE).size());
        try {
            readRecord.getField(Scope.NON_VERSIONABLE,fieldDescriptor1.getName());
            fail();
        } catch (FieldNotFoundException expected) {
        }
        assertEquals("value2", readRecord.getField(Scope.VERSIONABLE_MUTABLE,fieldDescriptor1.getName()));
        assertEquals(789, readRecord.getField(Scope.NON_VERSIONABLE,fieldDescriptor2.getName()));
        
        deleteRecord.addFieldsToDelete(Scope.VERSIONABLE,Arrays.asList(new String[]{fieldDescriptor2.getName()}));
        deleteRecord.addFieldsToDelete(Scope.VERSIONABLE_MUTABLE, Arrays.asList(new String[]{fieldDescriptor3.getName()}));
        repository.update(deleteRecord);
        
        readRecord = repository.read(record.getId());
        assertEquals(Long.valueOf(3), readRecord.getVersion());
        assertEquals(1, readRecord.getFields(Scope.NON_VERSIONABLE).size());
        assertEquals(1, readRecord.getFields(Scope.VERSIONABLE).size());
        assertEquals(1, readRecord.getFields(Scope.VERSIONABLE_MUTABLE).size());
        assertEquals(789, readRecord.getField(Scope.NON_VERSIONABLE,fieldDescriptor2.getName()));
        assertEquals(false, readRecord.getField(Scope.VERSIONABLE,fieldDescriptor3.getName()));
        assertEquals("value2", readRecord.getField(Scope.VERSIONABLE_MUTABLE,fieldDescriptor1.getName()));
    }
    
    @Test
    public void testUpdateAfterDelete() throws Exception {
        Record record = createDefaultRecord(idGenerator.newRecordId());
        Record deleteRecord = repository.newRecord(record.getId());
        deleteRecord.setRecordType(record.getRecordTypeId(), record.getRecordTypeVersion());
        deleteRecord.addFieldsToDelete(Scope.VERSIONABLE,Arrays.asList(new String[]{fieldDescriptor2.getName()}));
        repository.update(deleteRecord);
        
        Record updateRecord = repository.newRecord(record.getId());
        updateRecord.setRecordType(record.getRecordTypeId(), record.getRecordTypeVersion());
        updateRecord.setField(Scope.VERSIONABLE,fieldDescriptor2.getName(), 789);
        repository.update(updateRecord);
        
        Record readRecord = repository.read(record.getId());
        assertEquals(Long.valueOf(3), readRecord.getVersion());
        assertEquals(789, readRecord.getField(Scope.VERSIONABLE,fieldDescriptor2.getName()));
        
        readRecord = repository.read(record.getId(), Long.valueOf(2));
        try {
            readRecord.getField(Scope.VERSIONABLE,fieldDescriptor2.getName());
            fail();
        } catch (FieldNotFoundException expected) {
        }
        
        readRecord = repository.read(record.getId(), Long.valueOf(1));
        assertEquals(123, readRecord.getField(Scope.VERSIONABLE,fieldDescriptor2.getName()));
    }
    
    @Test
    public void testDeleteNonVersionableFieldAndUpdateVersionableField() throws Exception {
        Record record = createDefaultRecord(idGenerator.newRecordId());
        Record updateRecord = repository.newRecord(record.getId());
        updateRecord.setRecordType(record.getRecordTypeId(), record.getRecordTypeVersion());
        updateRecord.setField(Scope.VERSIONABLE,fieldDescriptor2.getName(), 999);
        updateRecord.addFieldsToDelete(Scope.NON_VERSIONABLE,Arrays.asList(new String[]{fieldDescriptor1.getName()}));
        repository.update(updateRecord);
        
        Record readRecord = repository.read(record.getId());
        assertEquals(999, readRecord.getField(Scope.VERSIONABLE,fieldDescriptor2.getName()));
        try {
            readRecord.getField(Scope.NON_VERSIONABLE,fieldDescriptor1.getName());
            fail();
        } catch (FieldNotFoundException expected) {
        }
        
        readRecord = repository.read(record.getId(), Long.valueOf(1));
        try {
            readRecord.getField(Scope.NON_VERSIONABLE,fieldDescriptor1.getName());
            fail();
        } catch (FieldNotFoundException expected) {
        }
        
    }
    
    @Test
    public void testUpdateAndDeleteSameField() throws Exception {
        Record record = createDefaultRecord(idGenerator.newRecordId());
        Record updateRecord = repository.newRecord(record.getId());
        updateRecord.setRecordType(record.getRecordTypeId(), record.getRecordTypeVersion());
        updateRecord.setField(Scope.VERSIONABLE,fieldDescriptor2.getName(), 789);
        updateRecord.addFieldsToDelete(Scope.VERSIONABLE,Arrays.asList(new String[]{fieldDescriptor2.getName()}));
        repository.update(updateRecord);
        
        try {
            repository.read(record.getId()).getField(Scope.VERSIONABLE,fieldDescriptor2.getName());
            fail();
        } catch (FieldNotFoundException expected) {
        }
    }
    
    @Test
    public void testDeleteRecord() throws Exception {
        Record record = createDefaultRecord(idGenerator.newRecordId());
        repository.delete(record.getId());
        try {
            repository.read(record.getId());
            fail();
        } catch (RecordNotFoundException expected) {
        }
    }
    
    @Test
    public void testUpdateMutableField() throws Exception {
        Record record = createDefaultRecord(idGenerator.newRecordId());
        Record updateRecord = record.clone();
        updateRecord.setField(Scope.NON_VERSIONABLE,fieldDescriptor1.getName(), "value2");
        updateRecord.setField(Scope.VERSIONABLE,fieldDescriptor2.getName(), 789);
        updateRecord.setField(Scope.VERSIONABLE_MUTABLE,fieldDescriptor3.getName(), false);
        repository.update(updateRecord);
        
        updateRecord.setVersion(Long.valueOf(1));
        repository.updateMutableFields(updateRecord);
        
        Record readRecord = repository.read(record.getId(), Long.valueOf(1));
        assertEquals("value2", readRecord.getField(Scope.NON_VERSIONABLE, fieldDescriptor1.getName())); 
        assertEquals(123, readRecord.getField(Scope.VERSIONABLE, fieldDescriptor2.getName())); 
        assertEquals(false, readRecord.getField(Scope.VERSIONABLE_MUTABLE, fieldDescriptor3.getName())); 
    }
    
    @Test
    public void testUpdateMutableFieldWithNewRecordType() throws Exception {
        Record record = createDefaultRecord(idGenerator.newRecordId());
        Record updateRecord = record.clone();
        updateRecord.setField(Scope.NON_VERSIONABLE,fieldDescriptor1.getName(), "value2");
        updateRecord.setField(Scope.VERSIONABLE,fieldDescriptor2.getName(), 789);
        updateRecord.setField(Scope.VERSIONABLE_MUTABLE,fieldDescriptor3.getName(), false);
        repository.update(updateRecord);
        
        Record updateMutableRecord = repository.newRecord(record.getId());
        updateMutableRecord.setVersion(Long.valueOf(1));
        updateMutableRecord.setRecordType(recordType2.getId(), recordType2.getVersion());
        updateMutableRecord.setField(Scope.NON_VERSIONABLE,fieldDescriptor2.getName(), 789);
        updateMutableRecord.setField(Scope.VERSIONABLE,fieldDescriptor3.getName(), false);
        updateMutableRecord.setField(Scope.VERSIONABLE_MUTABLE,fieldDescriptor1.getName(), "value3");
        assertEquals(Long.valueOf(1), repository.updateMutableFields(updateMutableRecord).getVersion());
        
        Record readRecord = repository.read(record.getId(), Long.valueOf(1));
        assertEquals(Long.valueOf(1), readRecord.getVersion());
        assertEquals("value2", readRecord.getField(Scope.NON_VERSIONABLE, fieldDescriptor1.getName())); 
        assertEquals(123, readRecord.getField(Scope.VERSIONABLE, fieldDescriptor2.getName())); 
        assertEquals(true, readRecord.getField(Scope.VERSIONABLE_MUTABLE, fieldDescriptor3.getName())); 
        assertEquals("value3", readRecord.getField(Scope.VERSIONABLE_MUTABLE, fieldDescriptor1.getName()));
        try {
            readRecord.getField(Scope.NON_VERSIONABLE, fieldDescriptor2.getName());
            fail();
        } catch (FieldNotFoundException expected) {
        }
        try {
            readRecord.getField(Scope.VERSIONABLE, fieldDescriptor3.getName());
            fail();
        } catch (FieldNotFoundException expected) {
        }
        assertEquals(recordType1.getId(), readRecord.getRecordTypeId());
        assertEquals(recordType1.getId(), readRecord.getRecordTypeId(Scope.NON_VERSIONABLE));
        assertEquals(recordType1.getId(), readRecord.getRecordTypeId(Scope.VERSIONABLE));
        assertEquals(recordType2.getId(), readRecord.getRecordTypeId(Scope.VERSIONABLE_MUTABLE));

        readRecord = repository.read(record.getId());
        assertEquals(Long.valueOf(2), readRecord.getVersion());
        assertEquals("value2", readRecord.getField(Scope.NON_VERSIONABLE, fieldDescriptor1.getName())); 
        assertEquals(789, readRecord.getField(Scope.VERSIONABLE, fieldDescriptor2.getName())); 
        assertEquals(false, readRecord.getField(Scope.VERSIONABLE_MUTABLE, fieldDescriptor3.getName())); 
        assertEquals("value3", readRecord.getField(Scope.VERSIONABLE_MUTABLE, fieldDescriptor1.getName()));
        assertEquals(recordType1.getId(), readRecord.getRecordTypeId());
        assertEquals(recordType1.getId(), readRecord.getRecordTypeId(Scope.NON_VERSIONABLE));
        assertEquals(recordType1.getId(), readRecord.getRecordTypeId(Scope.VERSIONABLE));
        assertEquals(recordType1.getId(), readRecord.getRecordTypeId(Scope.VERSIONABLE_MUTABLE));
    }
    
    @Test
    public void testDeleteMutableField() throws Exception {
        Record record = createDefaultRecord(idGenerator.newRecordId());
        Record updateRecord = record.clone();
        updateRecord.setField(Scope.NON_VERSIONABLE,fieldDescriptor1.getName(), "value2");
        updateRecord.setField(Scope.VERSIONABLE,fieldDescriptor2.getName(), 789);
        updateRecord.setField(Scope.VERSIONABLE_MUTABLE,fieldDescriptor3.getName(), false);
        repository.update(updateRecord);
        
        Record deleteRecord = repository.newRecord(record.getId());
        deleteRecord.setVersion(Long.valueOf(1));
        deleteRecord.setRecordType(record.getRecordTypeId(), record.getRecordTypeVersion());
        deleteRecord.addFieldsToDelete(Scope.NON_VERSIONABLE, Arrays.asList(new String[]{fieldDescriptor1.getName()}));
        deleteRecord.addFieldsToDelete(Scope.VERSIONABLE, Arrays.asList(new String[]{fieldDescriptor2.getName()}));
        deleteRecord.addFieldsToDelete(Scope.VERSIONABLE_MUTABLE, Arrays.asList(new String[]{fieldDescriptor3.getName()}));
        
        repository.updateMutableFields(deleteRecord);
        
        Record readRecord = repository.read(record.getId(), Long.valueOf(1));
        assertEquals("value2", readRecord.getField(Scope.NON_VERSIONABLE, fieldDescriptor1.getName())); 
        assertEquals(123, readRecord.getField(Scope.VERSIONABLE, fieldDescriptor2.getName())); 
        try {
            readRecord.getField(Scope.VERSIONABLE_MUTABLE, fieldDescriptor3.getName());
            fail();
        } catch (FieldNotFoundException expected) {
        }

        readRecord = repository.read(record.getId());
        assertEquals(false, readRecord.getField(Scope.VERSIONABLE_MUTABLE, fieldDescriptor3.getName()));
    }
    
    @Test
    public void testDeleteMutableFieldCopiesValueToNext() throws Exception {
        Record record = createDefaultRecord(idGenerator.newRecordId());
        Record updateRecord = record.clone();
        updateRecord.setField(Scope.NON_VERSIONABLE,fieldDescriptor1.getName(), "value2");
        updateRecord.setField(Scope.VERSIONABLE,fieldDescriptor2.getName(), 789);
        updateRecord = repository.update(updateRecord); // Leave mutable field same on first update

        updateRecord.setField(Scope.VERSIONABLE_MUTABLE,fieldDescriptor3.getName(), false);
        updateRecord = repository.update(updateRecord);
        
        Record deleteRecord = repository.newRecord(record.getId());
        deleteRecord.setVersion(Long.valueOf(1));
        deleteRecord.setRecordType(record.getRecordTypeId(), record.getRecordTypeVersion());
        deleteRecord.addFieldsToDelete(Scope.VERSIONABLE_MUTABLE, Arrays.asList(new String[]{fieldDescriptor3.getName()}));
        
        repository.updateMutableFields(deleteRecord);
        
        Record readRecord = repository.read(record.getId(), Long.valueOf(1));
        try {
            readRecord.getField(Scope.VERSIONABLE_MUTABLE, fieldDescriptor3.getName());
            fail();
        } catch (FieldNotFoundException expected) {
        }

        readRecord = repository.read(record.getId(), Long.valueOf(2));
        assertEquals(true, readRecord.getField(Scope.VERSIONABLE_MUTABLE, fieldDescriptor3.getName()));
        
        readRecord = repository.read(record.getId());
        assertEquals(false, readRecord.getField(Scope.VERSIONABLE_MUTABLE, fieldDescriptor3.getName()));
    }
    
    @Test
    public void testMixin() throws Exception {
        FieldDescriptor fieldDescriptor4 = typeManager.createFieldDescriptor(typeManager.newFieldDescriptor(typeManager.getValueType("STRING", false, false), "field4"));
        FieldGroup fieldGroup4 = typeManager.newFieldGroup("FG4");
        fieldGroup4.setFieldGroupEntry(typeManager.newFieldGroupEntry(fieldDescriptor4.getId(), fieldDescriptor4.getVersion(), false, "alias4"));
        fieldGroup4 = typeManager.createFieldGroup(fieldGroup4);
        RecordType recordType4 = typeManager.newRecordType("RT4");
        recordType4.setFieldGroupId(Scope.VERSIONABLE,fieldGroup4.getId());
        recordType4.setFieldGroupVersion(Scope.VERSIONABLE,fieldGroup4.getVersion());
        recordType4.addMixin(recordType1.getId(), recordType1.getVersion());
        recordType4 = typeManager.createRecordType(recordType4);
        
        Record record = repository.newRecord(idGenerator.newRecordId());
        record.setRecordType(recordType4.getId(), recordType4.getVersion());
        record.setField(Scope.NON_VERSIONABLE,fieldDescriptor1.getName(), "foo");
        record.setField(Scope.VERSIONABLE,fieldDescriptor2.getName(), 555);
        record.setField(Scope.VERSIONABLE, fieldDescriptor4.getName(), "bar");
        record.setField(Scope.VERSIONABLE_MUTABLE,fieldDescriptor3.getName(), true);
        record = repository.create(record);
        
        Record readRecord = repository.read(record.getId());
        assertEquals(Long.valueOf(1), readRecord.getVersion());
        assertEquals("foo", readRecord.getField(Scope.NON_VERSIONABLE, fieldDescriptor1.getName()));
        assertEquals(555, readRecord.getField(Scope.VERSIONABLE, fieldDescriptor2.getName()));
        assertEquals("bar", readRecord.getField(Scope.VERSIONABLE, fieldDescriptor4.getName()));
        assertEquals(true, readRecord.getField(Scope.VERSIONABLE_MUTABLE, fieldDescriptor3.getName()));
    }
    
    @Test
    public void testUpdateNameFieldDescriptor() throws Exception {
        // TODO : store fieldDescriptorVersion together with fieldValue
        
//        FieldDescriptor updatedFieldDescriptor = fieldDescriptor2.clone();
//        updatedFieldDescriptor.setName("newName");
//        updatedFieldDescriptor = typeManager.updateFieldDescriptor(updatedFieldDescriptor);
//        
//        FieldGroup fieldGroup5 = typeManager.newFieldGroup("FG5");
//        fieldGroup5.setFieldGroupEntry(typeManager.newFieldGroupEntry(updatedFieldDescriptor.getId(), updatedFieldDescriptor.getVersion(), false, "alias5"));
//        fieldGroup5 = typeManager.createFieldGroup(fieldGroup5);
//        RecordType recordType5 = typeManager.newRecordType("RT5");
//        recordType5.setFieldGroupId(Scope.VERSIONABLE,fieldGroup5.getId());
//        recordType5.setFieldGroupVersion(Scope.VERSIONABLE,fieldGroup5.getVersion());
//        recordType5 = typeManager.createRecordType(recordType5);
//        
//        Record record = createDefaultRecord(idGenerator.newRecordId());
//        Record record2 = repository.newRecord(record.getId());
//        record2.setRecordType(recordType5.getId(), recordType5.getVersion());
//        record2.setField(Scope.VERSIONABLE, "newName", 54321);
//        repository.update(record2);
//        
//        Record readRecord = repository.read(record.getId());
//        assertEquals(1, readRecord.getFields(Scope.VERSIONABLE).size());
//        assertEquals(54321, readRecord.getField(Scope.VERSIONABLE, "newName"));
//        
//        readRecord = repository.read(record.getId(), Long.valueOf(1));
//        assertEquals(1, readRecord.getFields(Scope.VERSIONABLE).size());
//        assertEquals(123, readRecord.getField(Scope.VERSIONABLE, fieldDescriptor2.getName()));
    }
}
