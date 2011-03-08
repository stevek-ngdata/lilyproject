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
package org.lilyproject.repository.impl.test;

import static org.easymock.EasyMock.createControl;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isA;
import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.easymock.IMocksControl;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilyproject.repository.api.*;
import org.lilyproject.repository.avro.*;
import org.lilyproject.repository.impl.*;
import org.lilyproject.repository.impl.primitivevaluetype.StringValueType;


public class AvroConverterTest {

    private static Repository repository;
    private static TypeManager typeManager;
    private static AvroConverter converter;
    private static IMocksControl control;
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        control = createControl();
        repository = control.createMock(Repository.class);
        typeManager = control.createMock(TypeManager.class);
    }
    
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
        repository.getTypeManager();
        expectLastCall().andReturn(typeManager).anyTimes();
    }

    @After
    public void tearDown() throws Exception {
        control.reset();
    }
    
    @Test
    public void testQName() {
        control.replay();
        converter = new AvroConverter();
        converter.setRepository(repository);
        // Full name
        QName qname = new QName("namespace", "name");
        AvroQName avroQName = new AvroQName();
        avroQName.namespace = "namespace";
        avroQName.name = "name";
        assertEquals(avroQName, converter.convert(qname));
        assertEquals(qname, converter.convert(avroQName));
        
        // No namespace
        qname = new QName(null, "name");
        avroQName = new AvroQName();
        avroQName.name = "name";
        control.verify();
        assertEquals(avroQName, converter.convert(qname));
        assertEquals(qname, converter.convert(avroQName));
    }
    
    @Test
    public void testValueType() throws TypeException {
        PrimitiveValueType primitiveValueType = new StringValueType();
        ValueType valueType = new ValueTypeImpl(primitiveValueType , false, true);
        typeManager.getValueType("STRING", false, true);
        expectLastCall().andReturn(valueType);
        
        control.replay();
        converter = new AvroConverter();
        converter.setRepository(repository);
        AvroValueType avroValueType = new AvroValueType();
        avroValueType.primitiveValueType = "STRING";
        avroValueType.multivalue = false;
        avroValueType.hierarchical = true;
        
        assertEquals(valueType, converter.convert(avroValueType));
        assertEquals(avroValueType, converter.convert(valueType));
        control.verify();
    }
    
    @Test
    public void testFieldTypeEntry() {
        SchemaId id = new SchemaIdImpl(UUID.randomUUID());
        FieldTypeEntry fieldTypeEntry = new FieldTypeEntryImpl(id, true);
        typeManager.newFieldTypeEntry(id, true);
        expectLastCall().andReturn(fieldTypeEntry);
        
        control.replay();
        converter = new AvroConverter();
        converter.setRepository(repository);
        AvroFieldTypeEntry avroFieldTypeEntry = new AvroFieldTypeEntry();
        AvroSchemaId avroSchemaId = new AvroSchemaId();
        avroSchemaId.idBytes = ByteBuffer.wrap(id.getBytes());
        avroFieldTypeEntry.id = avroSchemaId;
        avroFieldTypeEntry.mandatory = true;
        assertEquals(fieldTypeEntry, converter.convert(avroFieldTypeEntry));
        assertEquals(avroFieldTypeEntry, converter.convert(fieldTypeEntry));
        control.verify();
    }
    
    @Test
    public void testFieldType() throws TypeException {
        ValueType valueType = new ValueTypeImpl(new StringValueType(), true, true);
        typeManager.getValueType("STRING", true, true);
        expectLastCall().andReturn(valueType);
        QName name = new QName("aNamespace", "aName");
        SchemaId fieldTypeId = new SchemaIdImpl(UUID.randomUUID());
        FieldType fieldType = new FieldTypeImpl(fieldTypeId, valueType , name, Scope.NON_VERSIONED);
        typeManager.newFieldType(fieldTypeId, valueType, name, Scope.NON_VERSIONED);
        expectLastCall().andReturn(fieldType);

        control.replay();
        converter = new AvroConverter();
        converter.setRepository(repository);
        AvroFieldType avroFieldType = new AvroFieldType();
        AvroSchemaId avroSchemaId = new AvroSchemaId();
        avroSchemaId.idBytes = ByteBuffer.wrap(fieldTypeId.getBytes());
        avroFieldType.id = avroSchemaId;
        AvroQName avroQName = new AvroQName();
        avroQName.namespace = "aNamespace";
        avroQName.name = "aName";
        avroFieldType.name = avroQName ;
        avroFieldType.scope = Scope.NON_VERSIONED;
        AvroValueType avroValueType = new AvroValueType();
        avroValueType.primitiveValueType = "STRING";
        avroValueType.hierarchical = true;
        avroValueType.multivalue = true;
        avroFieldType.valueType = avroValueType;
        
        assertEquals(fieldType, converter.convert(avroFieldType));
        assertEquals(avroFieldType, converter.convert(fieldType));
        control.verify();
    }
    
    @Test
    public void testFieldTypeWithoutId() throws TypeException {
        ValueType valueType = new ValueTypeImpl(new StringValueType(), true, true);
        typeManager.getValueType("STRING", true, true);
        expectLastCall().andReturn(valueType);
        QName name = new QName("aNamespace", "aName");
        FieldType fieldType = new FieldTypeImpl(null, valueType , name, Scope.NON_VERSIONED);
        typeManager.newFieldType(valueType, name, Scope.NON_VERSIONED);
        expectLastCall().andReturn(fieldType);

        control.replay();
        converter = new AvroConverter();
        converter.setRepository(repository);
        AvroFieldType avroFieldType = new AvroFieldType();
        AvroQName avroQName = new AvroQName();
        avroQName.namespace = "aNamespace";
        avroQName.name = "aName";
        avroFieldType.name = avroQName ;
        avroFieldType.scope = Scope.NON_VERSIONED;
        AvroValueType avroValueType = new AvroValueType();
        avroValueType.primitiveValueType = "STRING";
        avroValueType.hierarchical = true;
        avroValueType.multivalue = true;
        avroFieldType.valueType = avroValueType;
        
        assertEquals(fieldType, converter.convert(avroFieldType));
        assertEquals(avroFieldType, converter.convert(fieldType));
        control.verify();
    }
    
    @Test
    public void testEmptyRecordType() throws TypeException {
        QName name = new QName("aNamespace", "aName");
        SchemaId id = new SchemaIdImpl(UUID.randomUUID());
        RecordType recordType = new RecordTypeImpl(id, name);
        typeManager.newRecordType(id, name);
        expectLastCall().andReturn(recordType);

        control.replay();
        converter = new AvroConverter();
        converter.setRepository(repository);
        AvroRecordType avroRecordType = new AvroRecordType();
        AvroSchemaId avroSchemaId = new AvroSchemaId();
        avroSchemaId.idBytes = ByteBuffer.wrap(id.getBytes());
        avroRecordType.id = avroSchemaId;
        AvroQName avroQName = new AvroQName();
        avroQName.namespace = "aNamespace";
        avroQName.name = "aName";
        avroRecordType.name = avroQName ;
        // fieldTypeEntries and mixins are by default empty instead of null
        avroRecordType.fieldTypeEntries = new GenericData.Array<AvroFieldTypeEntry>(0, Schema.createArray(AvroFieldTypeEntry.SCHEMA$));
        avroRecordType.mixins = new GenericData.Array<AvroMixin>(0, Schema.createArray(AvroMixin.SCHEMA$));
        assertEquals(recordType, converter.convert(avroRecordType));
        assertEquals(avroRecordType, converter.convert(recordType));
        control.verify();
    }
    
    @Test
    public void testRecordTypeVersion() throws TypeException {
        QName name = new QName("aNamespace", "aName");
        SchemaId id = new SchemaIdImpl(UUID.randomUUID());
        RecordType recordType = new RecordTypeImpl(id, name);
        typeManager.newRecordType(id, name);
        expectLastCall().andReturn(recordType);

        control.replay();
        converter = new AvroConverter();
        converter.setRepository(repository);
        recordType.setVersion(1L);
        AvroRecordType avroRecordType = new AvroRecordType();
        AvroSchemaId avroSchemaId = new AvroSchemaId();
        avroSchemaId.idBytes = ByteBuffer.wrap(id.getBytes());
        avroRecordType.id = avroSchemaId;
        AvroQName avroQName = new AvroQName();
        avroQName.namespace = "aNamespace";
        avroQName.name = "aName";
        avroRecordType.name = avroQName ;
        avroRecordType.version = 1L;
        // fieldTypeEntries and mixins are by default empty instead of null
        avroRecordType.fieldTypeEntries = new GenericData.Array<AvroFieldTypeEntry>(0, Schema.createArray(AvroFieldTypeEntry.SCHEMA$));
        avroRecordType.mixins = new GenericData.Array<AvroMixin>(0, Schema.createArray(AvroMixin.SCHEMA$));
        assertEquals(recordType, converter.convert(avroRecordType));
        assertEquals(avroRecordType, converter.convert(recordType));
        control.verify();
    }
    
    @Test
    public void testRecordTypeFieldTypeEntries() throws TypeException {
        QName name = new QName("aNamespace", "aName");
        SchemaId recordTypeId = new SchemaIdImpl(UUID.randomUUID());
        RecordType recordType = new RecordTypeImpl(recordTypeId, name);
        typeManager.newRecordType(recordTypeId, name);
        expectLastCall().andReturn(recordType);
        
        SchemaId fieldTypeId1 = new SchemaIdImpl(UUID.randomUUID());        
        SchemaId fieldTypeId2 = new SchemaIdImpl(UUID.randomUUID());        
        FieldTypeEntryImpl fieldTypeEntry1 = new FieldTypeEntryImpl(fieldTypeId1, true);
        FieldTypeEntryImpl fieldTypeEntry2 = new FieldTypeEntryImpl(fieldTypeId2, false);
        typeManager.newFieldTypeEntry(fieldTypeId1, true);
        expectLastCall().andReturn(fieldTypeEntry1);
        typeManager.newFieldTypeEntry(fieldTypeId2, false);
        expectLastCall().andReturn(fieldTypeEntry2);
        
        control.replay();
        converter = new AvroConverter();
        converter.setRepository(repository);
        recordType.addFieldTypeEntry(fieldTypeEntry1);
        recordType.addFieldTypeEntry(fieldTypeEntry2);
        AvroRecordType avroRecordType = new AvroRecordType();
        AvroSchemaId avroRecordTypeId = new AvroSchemaId();
        avroRecordTypeId.idBytes = ByteBuffer.wrap(recordTypeId.getBytes());
        avroRecordType.id = avroRecordTypeId;
        AvroQName avroQName = new AvroQName();
        avroQName.namespace = "aNamespace";
        avroQName.name = "aName";
        avroRecordType.name = avroQName ;
        // fieldTypeEntries and mixins are by default empty instead of null
        avroRecordType.fieldTypeEntries = new GenericData.Array<AvroFieldTypeEntry>(0, Schema.createArray(AvroFieldTypeEntry.SCHEMA$));
        AvroFieldTypeEntry avroFieldTypeEntry = new AvroFieldTypeEntry();
        AvroSchemaId avroFieldTypeId1 = new AvroSchemaId();
        avroFieldTypeId1.idBytes = ByteBuffer.wrap(fieldTypeId1.getBytes());
        avroFieldTypeEntry.id = avroFieldTypeId1;
        avroFieldTypeEntry.mandatory = true;
        avroRecordType.fieldTypeEntries.add(avroFieldTypeEntry);
        Set<AvroFieldTypeEntry> expectedFieldTypeEntries = new HashSet<AvroFieldTypeEntry>();
        expectedFieldTypeEntries.add(avroFieldTypeEntry);
        avroFieldTypeEntry = new AvroFieldTypeEntry();
        AvroSchemaId avroFieldTypeId2 = new AvroSchemaId();
        avroFieldTypeId2.idBytes = ByteBuffer.wrap(fieldTypeId2.getBytes());
        avroFieldTypeEntry.id = avroFieldTypeId2;
        avroFieldTypeEntry.mandatory = false;
        avroRecordType.fieldTypeEntries.add(avroFieldTypeEntry);
        expectedFieldTypeEntries.add(avroFieldTypeEntry);
        avroRecordType.mixins = new GenericData.Array<AvroMixin>(0, Schema.createArray(AvroMixin.SCHEMA$));
        assertEquals(recordType, converter.convert(avroRecordType));
        AvroRecordType actualAvroRecordType = converter.convert(recordType);
        List<AvroFieldTypeEntry> fieldTypeEntries = actualAvroRecordType.fieldTypeEntries;
        assertEquals(2, fieldTypeEntries.size());
        Set<AvroFieldTypeEntry> actualFieldTypeEntries = new HashSet<AvroFieldTypeEntry>();
        for (AvroFieldTypeEntry entry : fieldTypeEntries) {
            actualFieldTypeEntries.add(entry);
        }
        assertEquals(expectedFieldTypeEntries, actualFieldTypeEntries);
        control.verify();
    }
    
    @Test
    public void testRecordTypeMixins() throws TypeException {
        QName name = new QName("aNamespace", "aName");
        SchemaId recordTypeId = new SchemaIdImpl(UUID.randomUUID());
        RecordType recordType = new RecordTypeImpl(recordTypeId, name);
        typeManager.newRecordType(recordTypeId, name);
        expectLastCall().andReturn(recordType);
        
        control.replay();
        converter = new AvroConverter();
        converter.setRepository(repository);
        SchemaId mixinId1 = new SchemaIdImpl(UUID.randomUUID());
        recordType.addMixin(mixinId1, 1L);
        SchemaId mixinId2 = new SchemaIdImpl(UUID.randomUUID());
        recordType.addMixin(mixinId2, 2L);
        AvroRecordType avroRecordType = new AvroRecordType();
        AvroSchemaId avroRecordTypeId = new AvroSchemaId();
        avroRecordTypeId.idBytes = ByteBuffer.wrap(recordTypeId.getBytes());
        avroRecordType.id = avroRecordTypeId;
        AvroQName avroQName = new AvroQName();
        avroQName.namespace = "aNamespace";
        avroQName.name = "aName";
        avroRecordType.name = avroQName ;
        // fieldTypeEntries and mixins are by default empty instead of null
        avroRecordType.fieldTypeEntries = new GenericData.Array<AvroFieldTypeEntry>(0, Schema.createArray(AvroFieldTypeEntry.SCHEMA$));
        avroRecordType.mixins = new GenericData.Array<AvroMixin>(0, Schema.createArray(AvroMixin.SCHEMA$));
        AvroMixin avroMixin1 = new AvroMixin();
        AvroSchemaId avroMixinId1 = new AvroSchemaId();
        avroMixinId1.idBytes = ByteBuffer.wrap(mixinId1.getBytes());
        avroMixin1.recordTypeId = avroMixinId1;
        avroMixin1.recordTypeVersion = 1L;
        avroRecordType.mixins.add(avroMixin1);
        Set<AvroMixin> expectedMixins = new HashSet<AvroMixin>();
        expectedMixins.add(avroMixin1);
        AvroMixin avroMixin2 = new AvroMixin();
        AvroSchemaId avroMixinId2 = new AvroSchemaId();
        avroMixinId2.idBytes = ByteBuffer.wrap(mixinId2.getBytes());
        avroMixin2.recordTypeId = avroMixinId2;
        avroMixin2.recordTypeVersion = 2L;
        avroRecordType.mixins.add(avroMixin2);
        expectedMixins.add(avroMixin2);
        assertEquals(recordType, converter.convert(avroRecordType));
        AvroRecordType actualAvroRecordType = converter.convert(recordType);
        List<AvroMixin> mixins = actualAvroRecordType.mixins;
        assertEquals(2, mixins.size());
        Set<AvroMixin> actualMixins = new HashSet<AvroMixin>();
        for (AvroMixin entry : mixins) {
            actualMixins.add(entry);
        }
        assertEquals(expectedMixins, actualMixins);
        control.verify();
    }
    
    @Test
    public void testEmptyRecord() throws Exception {
        repository.newRecord();
        expectLastCall().andReturn(new RecordImpl()).anyTimes();
        control.replay();
                converter = new AvroConverter();        converter.setRepository(repository);
        Record record = new RecordImpl();
        record.setRecordType(new QName("ns","recordTypeName"), null);
        AvroRecord avroRecord = new AvroRecord();
        AvroQName avroQName = new AvroQName();
        avroQName.namespace = "ns";
        avroQName.name = "recordTypeName";
        avroRecord.recordTypeName = avroQName;

        assertEquals(record, converter.convert(avroRecord));
        // A bit of converting back and forth since avro can't compare maps
        assertEquals(converter.convert(avroRecord), converter.convert(converter.convert(record)));
        control.verify();
    }
    
    @Test
    public void testRecord() throws Exception {
        FieldType fieldType = control.createMock(FieldType.class);
        ValueType valueType = new ValueTypeImpl(new StringValueType(), false, false);
        IdGenerator idGenerator = new IdGeneratorImpl();
        
        repository.newRecord();
        expectLastCall().andReturn(new RecordImpl()).anyTimes();
        repository.getIdGenerator();
        expectLastCall().andReturn(idGenerator).anyTimes();
        typeManager.getFieldTypeByName(isA(QName.class));
        expectLastCall().andReturn(fieldType).anyTimes();
        fieldType.getValueType();
        expectLastCall().andReturn(valueType).anyTimes();
        typeManager.getValueType("STRING", false, false);
        expectLastCall().andReturn(valueType).anyTimes();
        control.replay();

        converter = new AvroConverter();
        converter.setRepository(repository);
        Record record = new RecordImpl();
        RecordId recordId = repository.getIdGenerator().newRecordId();
        record.setId(recordId);
        // Scope.NON_VERSIONED recordType and master record type are the same
        record.setRecordType(Scope.NON_VERSIONED, new QName("ns", "nvrt"), 1L);
        record.setRecordType(Scope.VERSIONED, new QName("ns", "vrt"), 2L);
        record.setRecordType(Scope.VERSIONED_MUTABLE, new QName("ns", "vmrt"), 3L);
        QName fieldName = new QName("ns", "aName");
        record.setField(fieldName, "aValue");
        QName fieldName2 = new QName(null, "aName2");
        record.setField(fieldName2, "aValue2");
        record.addFieldsToDelete(Arrays.asList(new QName[]{new QName("devnull", "fieldToDelete")}));
        
        assertEquals(record, converter.convert(converter.convert(record)));
        control.verify();
    }
}
