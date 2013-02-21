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
package org.lilyproject.avro;

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
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.FieldTypeEntry;
import org.lilyproject.repository.api.FieldTypes;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordFactory;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.RepositoryManager;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.api.ValueType;
import org.lilyproject.repository.impl.FieldTypeEntryImpl;
import org.lilyproject.repository.impl.FieldTypeImpl;
import org.lilyproject.repository.impl.RecordImpl;
import org.lilyproject.repository.impl.RecordTypeImpl;
import org.lilyproject.repository.impl.id.IdGeneratorImpl;
import org.lilyproject.repository.impl.id.SchemaIdImpl;
import org.lilyproject.repository.impl.valuetype.StringValueType;


public class AvroConverterTest {

    private static RepositoryManager repositoryManager;
    private static TypeManager typeManager;
    private static RecordFactory recordFactory;
    private static AvroConverter converter;
    private static IMocksControl control;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        control = createControl();
        repositoryManager = control.createMock(RepositoryManager.class);
        typeManager = control.createMock(TypeManager.class);
        recordFactory = control.createMock(RecordFactory.class);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
        repositoryManager.getTypeManager();
        expectLastCall().andReturn(typeManager).anyTimes();
        
        repositoryManager.getRecordFactory();
        expectLastCall().andReturn(recordFactory).anyTimes();
    }

    @After
    public void tearDown() throws Exception {
        control.reset();
    }

    @Test
    public void testQName() {
        control.replay();
        converter = new AvroConverter(repositoryManager);
        // Full name
        QName qname = new QName("namespace", "name");
        AvroQName avroQName = new AvroQName();
        avroQName.namespace = "namespace";
        avroQName.name = "name";
        assertEquals(avroQName, converter.convert(qname));
        assertEquals(qname, converter.convert(avroQName));
    }

    @Test
    public void testValueType() throws Exception {
        ValueType valueType = new StringValueType();
        typeManager.getValueType("STRING");
        expectLastCall().andReturn(valueType);

        control.replay();
        converter = new AvroConverter(repositoryManager);
        AvroValueType avroValueType = new AvroValueType();
        avroValueType.valueType = "STRING";

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
        converter = new AvroConverter(repositoryManager);
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
    public void testFieldType() throws Exception {
        ValueType valueType = new StringValueType();
        typeManager.getValueType("STRING");
        expectLastCall().andReturn(valueType);
        QName name = new QName("aNamespace", "aName");
        SchemaId fieldTypeId = new SchemaIdImpl(UUID.randomUUID());
        FieldType fieldType = new FieldTypeImpl(fieldTypeId, valueType, name, Scope.NON_VERSIONED);
        typeManager.newFieldType(fieldTypeId, valueType, name, Scope.NON_VERSIONED);
        expectLastCall().andReturn(fieldType);

        control.replay();
        converter = new AvroConverter(repositoryManager);
        AvroFieldType avroFieldType = new AvroFieldType();
        AvroSchemaId avroSchemaId = new AvroSchemaId();
        avroSchemaId.idBytes = ByteBuffer.wrap(fieldTypeId.getBytes());
        avroFieldType.id = avroSchemaId;
        AvroQName avroQName = new AvroQName();
        avroQName.namespace = "aNamespace";
        avroQName.name = "aName";
        avroFieldType.name = avroQName;
        avroFieldType.scope = AvroScope.NON_VERSIONED;
        AvroValueType avroValueType = new AvroValueType();
        avroValueType.valueType = "STRING";
        avroFieldType.valueType = avroValueType;

        assertEquals(fieldType, converter.convert(avroFieldType));
        assertEquals(avroFieldType, converter.convert(fieldType));
        control.verify();
    }

    @Test
    public void testFieldTypeWithoutId() throws Exception {
        ValueType valueType = new StringValueType();
        typeManager.getValueType("LIST<STRING>");
        expectLastCall().andReturn(valueType);
        QName name = new QName("aNamespace", "aName");
        FieldType fieldType = new FieldTypeImpl(null, valueType, name, Scope.NON_VERSIONED);
        typeManager.newFieldType(valueType, name, Scope.NON_VERSIONED);
        expectLastCall().andReturn(fieldType);

        control.replay();
        converter = new AvroConverter(repositoryManager);
        AvroFieldType avroFieldType = new AvroFieldType();
        AvroQName avroQName = new AvroQName();
        avroQName.namespace = "aNamespace";
        avroQName.name = "aName";
        avroFieldType.name = avroQName;
        avroFieldType.scope = AvroScope.NON_VERSIONED;
        AvroValueType avroValueType = new AvroValueType();
        avroValueType.valueType = "LIST<STRING>";
        avroFieldType.valueType = avroValueType;

        converter.convert(avroFieldType);
//        assertEquals(fieldType, converter.convert(avroFieldType));
//        assertEquals(avroFieldType, converter.convert(fieldType));
        control.verify();
    }

    @Test
    public void testEmptyRecordType() throws Exception {
        QName name = new QName("aNamespace", "aName");
        SchemaId id = new SchemaIdImpl(UUID.randomUUID());
        RecordType recordType = new RecordTypeImpl(id, name);
        typeManager.newRecordType(id, name);
        expectLastCall().andReturn(recordType);

        control.replay();
        converter = new AvroConverter(repositoryManager);
        AvroRecordType avroRecordType = new AvroRecordType();
        AvroSchemaId avroSchemaId = new AvroSchemaId();
        avroSchemaId.idBytes = ByteBuffer.wrap(id.getBytes());
        avroRecordType.id = avroSchemaId;
        AvroQName avroQName = new AvroQName();
        avroQName.namespace = "aNamespace";
        avroQName.name = "aName";
        avroRecordType.name = avroQName;
        // fieldTypeEntries and supertypes are by default empty instead of null
        avroRecordType.fieldTypeEntries =
                new GenericData.Array<AvroFieldTypeEntry>(0, Schema.createArray(AvroFieldTypeEntry.SCHEMA$));
        avroRecordType.supertypes = new GenericData.Array<AvroSupertype>(0, Schema.createArray(AvroSupertype.SCHEMA$));
        assertEquals(recordType, converter.convert(avroRecordType));
        assertEquals(avroRecordType, converter.convert(recordType));
        control.verify();
    }

    @Test
    public void testRecordTypeVersion() throws Exception {
        QName name = new QName("aNamespace", "aName");
        SchemaId id = new SchemaIdImpl(UUID.randomUUID());
        RecordType recordType = new RecordTypeImpl(id, name);
        typeManager.newRecordType(id, name);
        expectLastCall().andReturn(recordType);

        control.replay();
        converter = new AvroConverter(repositoryManager);
        recordType.setVersion(1L);
        AvroRecordType avroRecordType = new AvroRecordType();
        AvroSchemaId avroSchemaId = new AvroSchemaId();
        avroSchemaId.idBytes = ByteBuffer.wrap(id.getBytes());
        avroRecordType.id = avroSchemaId;
        AvroQName avroQName = new AvroQName();
        avroQName.namespace = "aNamespace";
        avroQName.name = "aName";
        avroRecordType.name = avroQName;
        avroRecordType.version = 1L;
        // fieldTypeEntries and supertypes are by default empty instead of null
        avroRecordType.fieldTypeEntries =
                new GenericData.Array<AvroFieldTypeEntry>(0, Schema.createArray(AvroFieldTypeEntry.SCHEMA$));
        avroRecordType.supertypes = new GenericData.Array<AvroSupertype>(0, Schema.createArray(AvroSupertype.SCHEMA$));
        assertEquals(recordType, converter.convert(avroRecordType));
        assertEquals(avroRecordType, converter.convert(recordType));
        control.verify();
    }

    @Test
    public void testRecordTypeFieldTypeEntries() throws Exception {
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
        converter = new AvroConverter(repositoryManager);
        recordType.addFieldTypeEntry(fieldTypeEntry1);
        recordType.addFieldTypeEntry(fieldTypeEntry2);
        AvroRecordType avroRecordType = new AvroRecordType();
        AvroSchemaId avroRecordTypeId = new AvroSchemaId();
        avroRecordTypeId.idBytes = ByteBuffer.wrap(recordTypeId.getBytes());
        avroRecordType.id = avroRecordTypeId;
        AvroQName avroQName = new AvroQName();
        avroQName.namespace = "aNamespace";
        avroQName.name = "aName";
        avroRecordType.name = avroQName;
        // fieldTypeEntries and supertypes are by default empty instead of null
        avroRecordType.fieldTypeEntries =
                new GenericData.Array<AvroFieldTypeEntry>(0, Schema.createArray(AvroFieldTypeEntry.SCHEMA$));
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
        avroRecordType.supertypes = new GenericData.Array<AvroSupertype>(0, Schema.createArray(AvroSupertype.SCHEMA$));
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
    public void testRecordTypeSupertypes() throws Exception {
        QName name = new QName("aNamespace", "aName");
        SchemaId recordTypeId = new SchemaIdImpl(UUID.randomUUID());
        RecordType recordType = new RecordTypeImpl(recordTypeId, name);
        typeManager.newRecordType(recordTypeId, name);
        expectLastCall().andReturn(recordType);

        control.replay();
        converter = new AvroConverter(repositoryManager);
        SchemaId supertypeId1 = new SchemaIdImpl(UUID.randomUUID());
        recordType.addSupertype(supertypeId1, 1L);
        SchemaId supertypeId2 = new SchemaIdImpl(UUID.randomUUID());
        recordType.addSupertype(supertypeId2, 2L);
        AvroRecordType avroRecordType = new AvroRecordType();
        AvroSchemaId avroRecordTypeId = new AvroSchemaId();
        avroRecordTypeId.idBytes = ByteBuffer.wrap(recordTypeId.getBytes());
        avroRecordType.id = avroRecordTypeId;
        AvroQName avroQName = new AvroQName();
        avroQName.namespace = "aNamespace";
        avroQName.name = "aName";
        avroRecordType.name = avroQName;
        // fieldTypeEntries and supertypes are by default empty instead of null
        avroRecordType.fieldTypeEntries =
                new GenericData.Array<AvroFieldTypeEntry>(0, Schema.createArray(AvroFieldTypeEntry.SCHEMA$));
        avroRecordType.supertypes = new GenericData.Array<AvroSupertype>(0, Schema.createArray(AvroSupertype.SCHEMA$));
        AvroSupertype avroSupertype1 = new AvroSupertype();
        AvroSchemaId avroSupertypeId1 = new AvroSchemaId();
        avroSupertypeId1.idBytes = ByteBuffer.wrap(supertypeId1.getBytes());
        avroSupertype1.recordTypeId = avroSupertypeId1;
        avroSupertype1.recordTypeVersion = 1L;
        avroRecordType.supertypes.add(avroSupertype1);
        Set<AvroSupertype> expectedSupertypes = new HashSet<AvroSupertype>();
        expectedSupertypes.add(avroSupertype1);
        AvroSupertype avroSupertype2 = new AvroSupertype();
        AvroSchemaId avroSupertypeId2 = new AvroSchemaId();
        avroSupertypeId2.idBytes = ByteBuffer.wrap(supertypeId2.getBytes());
        avroSupertype2.recordTypeId = avroSupertypeId2;
        avroSupertype2.recordTypeVersion = 2L;
        avroRecordType.supertypes.add(avroSupertype2);
        expectedSupertypes.add(avroSupertype2);
        assertEquals(recordType, converter.convert(avroRecordType));
        AvroRecordType actualAvroRecordType = converter.convert(recordType);
        List<AvroSupertype> supertypes = actualAvroRecordType.supertypes;
        assertEquals(2, supertypes.size());
        Set<AvroSupertype> actualSupertypes = new HashSet<AvroSupertype>();
        for (AvroSupertype entry : supertypes) {
            actualSupertypes.add(entry);
        }
        assertEquals(expectedSupertypes, actualSupertypes);
        control.verify();
    }

    @Test
    public void testEmptyRecord() throws Exception {
        converter = new AvroConverter(repositoryManager);
        FieldTypes fieldTypesSnapshot = control.createMock(FieldTypes.class);
        recordFactory.newRecord();
        expectLastCall().andReturn(new RecordImpl()).anyTimes();
        typeManager.getFieldTypesSnapshot();
        expectLastCall().andReturn(fieldTypesSnapshot).anyTimes();
        control.replay();
        Record record = new RecordImpl();
        record.setRecordType(new QName("ns", "recordTypeName"), null);

        assertEquals(record, converter.convertRecord(converter.convert(record)));
        assertEquals(converter.convert(record), converter.convert(converter.convertRecord(converter.convert(record))));
        control.verify();
    }

    @Test
    public void testRecord() throws Exception {
        converter = new AvroConverter(repositoryManager);
        FieldType fieldType = control.createMock(FieldType.class);
        FieldTypes fieldTypesSnapshot = control.createMock(FieldTypes.class);
        ValueType valueType = new StringValueType();
        IdGenerator idGenerator = new IdGeneratorImpl();

        recordFactory.newRecord();
        expectLastCall().andReturn(new RecordImpl()).anyTimes();
        repositoryManager.getIdGenerator();
        expectLastCall().andReturn(idGenerator).anyTimes();
        typeManager.getFieldTypesSnapshot();
        expectLastCall().andReturn(fieldTypesSnapshot).anyTimes();
        fieldTypesSnapshot.getFieldType(isA(QName.class));
        expectLastCall().andReturn(fieldType).anyTimes();
        fieldType.getValueType();
        expectLastCall().andReturn(valueType).anyTimes();
        typeManager.getValueType("STRING");
        expectLastCall().andReturn(valueType).anyTimes();
        control.replay();

        Record record = new RecordImpl();
        RecordId recordId = repositoryManager.getIdGenerator().newRecordId();
        record.setId(recordId);
        // Scope.NON_VERSIONED recordType and master record type are the same
        record.setRecordType(Scope.NON_VERSIONED, new QName("ns", "nvrt"), 1L);
        record.setRecordType(Scope.VERSIONED, new QName("ns", "vrt"), 2L);
        record.setRecordType(Scope.VERSIONED_MUTABLE, new QName("ns", "vmrt"), 3L);
        QName fieldName = new QName("ns", "aName");
        record.setField(fieldName, "aValue");
        QName fieldName2 = new QName("ns", "aName2");
        record.setField(fieldName2, "aValue2");
        record.addFieldsToDelete(Arrays.asList(new QName("devnull", "fieldToDelete")));

        assertEquals(record, converter.convertRecord(converter.convert(record)));
        control.verify();
    }
}
