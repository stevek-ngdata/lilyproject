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

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.lilyproject.repository.api.Scope.VERSIONED;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;
import org.lilyproject.repository.api.*;
import org.lilyproject.repository.impl.SchemaIdImpl;

public abstract class AbstractTypeManagerRecordTypeTest {

    private static String namespace1 = "ns1";
    protected static TypeManager typeManager;
    private static FieldType fieldType1;
    private static FieldType fieldType2;
    private static FieldType fieldType3;

    protected static void setupFieldTypes() throws Exception {
        fieldType1 = typeManager.createFieldType(typeManager.newFieldType(typeManager.getValueType("STRING"),
                new QName(namespace1, "field1"), Scope.NON_VERSIONED));
        fieldType2 = typeManager.createFieldType(typeManager.newFieldType(typeManager.getValueType("INTEGER"),
                new QName(namespace1, "field2"), Scope.VERSIONED));
        fieldType3 = typeManager.createFieldType(typeManager.newFieldType(typeManager.getValueType("BOOLEAN"),
                new QName(namespace1, "field3"), Scope.VERSIONED_MUTABLE));
    }

    @Test
    public void testCreateEmpty() throws Exception {
        QName name = new QName("testNS", "testCreateEmpty");
        RecordType recordType = typeManager.newRecordType(name);
        recordType = typeManager.createRecordType(recordType);
        assertEquals(Long.valueOf(1), recordType.getVersion());
        RecordType recordType2 = typeManager.getRecordTypeByName(name, null);
        assertEquals(recordType, recordType2);
    }

    @Test
    public void testCreate() throws Exception {
        QName name = new QName("testNS", "testCreate");
        RecordType recordType = typeManager.newRecordType(name);
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType1.getId(), false));
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType2.getId(), false));
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType3.getId(), false));
        RecordType createdRecordType = typeManager.createRecordType(recordType);
        assertEquals(Long.valueOf(1), createdRecordType.getVersion());
        
        recordType.setVersion(Long.valueOf(1));
        recordType.setId(createdRecordType.getId());
        assertEquals(recordType, typeManager.getRecordTypeById(createdRecordType.getId(), null));
    }
    
    @Test
    public void testCreateSameNameFails() throws Exception {
        QName name = new QName(namespace1, "testCreateSameNameFails");
        RecordType recordType = typeManager.newRecordType(name);
        recordType = typeManager.createRecordType(recordType);
        
        recordType = typeManager.newRecordType(name);
        try {
            System.out.println("Expecting RecordTypeExistsException");
            typeManager.createRecordType(recordType);
            fail();
        } catch (RecordTypeExistsException expected) {
        }
    }

    @Test
    public void testUpdate() throws Exception {
        QName name = new QName(namespace1, "testUpdate");
        RecordType recordType = typeManager.newRecordType(name);
        recordType = typeManager.createRecordType(recordType);
        assertEquals(Long.valueOf(1), recordType.getVersion());
        RecordType recordTypeV1 = typeManager.getRecordTypeByName(name, null);
        assertEquals(Long.valueOf(1), typeManager.updateRecordType(recordTypeV1).getVersion());
        
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType1.getId(), true));
        RecordType recordTypeV2 = typeManager.updateRecordType(recordType);
        assertEquals(Long.valueOf(2), recordTypeV2.getVersion());
        assertEquals(Long.valueOf(2), typeManager.updateRecordType(recordTypeV2).getVersion());
        
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType2.getId(), true));
        RecordType recordTypeV3 = typeManager.updateRecordType(recordType);
        assertEquals(Long.valueOf(3), recordTypeV3.getVersion());
        assertEquals(Long.valueOf(3), typeManager.updateRecordType(recordType).getVersion());
    
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType3.getId(), true));
        RecordType recordTypeV4 = typeManager.updateRecordType(recordType);
        assertEquals(Long.valueOf(4), recordTypeV4.getVersion());
        assertEquals(Long.valueOf(4), typeManager.updateRecordType(recordType).getVersion());
        
        recordType.setVersion(Long.valueOf(4));
        assertEquals(recordType, typeManager.getRecordTypeByName(name, null));
        
        // Read old versions
        assertEquals(recordTypeV1, typeManager.getRecordTypeByName(name,Long.valueOf(1)));
        assertEquals(recordTypeV2, typeManager.getRecordTypeByName(name,Long.valueOf(2)));
        assertEquals(recordTypeV3, typeManager.getRecordTypeByName(name,Long.valueOf(3)));
        assertEquals(recordTypeV4, typeManager.getRecordTypeByName(name,Long.valueOf(4)));
    }

    @Test
    public void testReadNonExistingRecordTypeFails() throws Exception {
        QName name = new QName("testNS", "testReadNonExistingRecordTypeFails");
        try {
            System.out.println("Expecting RecordTypeNotFoundException");
            typeManager.getRecordTypeByName(name, null);
            fail();
        } catch (RecordTypeNotFoundException expected) {
        }
        
        typeManager.createRecordType(typeManager.newRecordType(name));
        try {
            System.out.println("Expecting RecordTypeNotFoundException");
            typeManager.getRecordTypeByName(name, Long.valueOf(2));
            fail();
        } catch (RecordTypeNotFoundException expected) {
        }
    }

    @Test
    public void testUpdateNonExistingRecordTypeFails() throws Exception {
        QName name = new QName("testNS", "testUpdateNonExistingRecordTypeFails");
        RecordType recordType = typeManager.newRecordType(name);
        try {
            System.out.println("Expecting RecordTypeNotFoundException");
            typeManager.updateRecordType(recordType);
            fail();
        } catch (RecordTypeNotFoundException expected) {
        }
        recordType.setId(new SchemaIdImpl(UUID.randomUUID()));
        try {
            System.out.println("Expecting RecordTypeNotFoundException");
            typeManager.updateRecordType(recordType);
            fail();
        } catch (RecordTypeNotFoundException expected) {
        }
    }

    @Test
    public void testFieldTypeExistsOnCreate() throws Exception {
        QName name = new QName("testNS", "testUpdateNonExistingRecordTypeFails");
        RecordType recordType = typeManager.newRecordType(name);
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(new SchemaIdImpl(UUID.randomUUID()), false));
        try {
            System.out.println("Expecting FieldTypeNotFoundException");
            typeManager.createRecordType(recordType);
            fail();
        } catch (FieldTypeNotFoundException expected) {
        }
    }

    @Test
    public void testFieldTypeExistsOnUpdate() throws Exception {
        QName name = new QName("testNS", "testFieldGroupExistsOnUpdate");
        RecordType recordType = typeManager.newRecordType(name);
        recordType = typeManager.createRecordType(recordType);
        
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(new SchemaIdImpl(UUID.randomUUID()), false));
        try {
            System.out.println("Expecting FieldTypeNotFoundException");
            typeManager.updateRecordType(recordType);
            fail();
        } catch (FieldTypeNotFoundException expected) {
        }
    }

    @Test
    public void testRemove() throws Exception {
        QName name = new QName("testNS", "testRemove");
        RecordType recordType = typeManager.newRecordType(name);
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType1.getId(), false));
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType2.getId(), false));
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType3.getId(), false));
        recordType = typeManager.createRecordType(recordType);
        
        recordType.removeFieldTypeEntry(fieldType1.getId());
        recordType.removeFieldTypeEntry(fieldType2.getId());
        recordType.removeFieldTypeEntry(fieldType3.getId());
        typeManager.updateRecordType(recordType);
        
        RecordType readRecordType = typeManager.getRecordTypeByName(name, null);
        assertTrue(readRecordType.getFieldTypeEntries().isEmpty());
    }

    @Test
    public void testRemoveLeavesOlderVersionsUntouched() throws Exception {
        QName name = new QName("testNS", "testRemoveLeavesOlderVersionsUntouched");
        RecordType recordType = typeManager.newRecordType(name);
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType1.getId(), false));
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType2.getId(), false));
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType3.getId(), false));
        recordType = typeManager.createRecordType(recordType);
        
        recordType.removeFieldTypeEntry(fieldType1.getId());
        recordType.removeFieldTypeEntry(fieldType2.getId());
        recordType.removeFieldTypeEntry(fieldType3.getId());
        typeManager.updateRecordType(recordType);
        
        RecordType readRecordType = typeManager.getRecordTypeByName(name, Long.valueOf(1));
        assertEquals(3, readRecordType.getFieldTypeEntries().size());
    }

    @Test
    public void testMixin() throws Exception {
        QName mixinName = new QName("mixinNS", "testMixin");
        RecordType mixinType = typeManager.newRecordType(mixinName);
        mixinType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType1.getId(), false));
        mixinType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType2.getId(), false));
        mixinType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType3.getId(), false));
        mixinType = typeManager.createRecordType(mixinType);

        QName recordName = new QName("recordNS", "testMixin");
        RecordType recordType = typeManager.newRecordType(recordName);
        recordType.addMixin(mixinType.getId(), mixinType.getVersion());
        recordType = typeManager.createRecordType(recordType);
        assertEquals(Long.valueOf(1), recordType.getVersion());
        assertEquals(recordType, typeManager.getRecordTypeById(recordType.getId(), null));
    }
    
    @Test
    public void testMixinLatestVersion() throws Exception {
        QName mixinName = new QName("mixinNS", "testMixinLatestVersion");
        RecordType mixinType = typeManager.newRecordType(mixinName);
        mixinType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType1.getId(), false));
        mixinType = typeManager.createRecordType(mixinType);

        mixinType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType2.getId(), false));
        mixinType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType3.getId(), false));
        mixinType = typeManager.updateRecordType(mixinType);

        QName recordName = new QName("recordNS", "testMixinLatestVersion");
        RecordType recordType = typeManager.newRecordType(recordName);
        recordType.addMixin(mixinType.getId());
        recordType = typeManager.createRecordType(recordType);
        assertEquals(Long.valueOf(1), recordType.getVersion());
        
        recordType.addMixin(mixinType.getId(), 2L); // Assert latest version of the Mixin RecordType got filled in
        assertEquals(recordType, typeManager.getRecordTypeById(recordType.getId(), null));
    }

    @Test
    public void testMixinUpdate() throws Exception {
        QName mixinName = new QName("mixinNS", "testMixinUpdate");
        RecordType mixinType1 = typeManager.newRecordType(mixinName);
        mixinType1.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType1.getId(), false));
        mixinType1.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType2.getId(), false));
        mixinType1.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType3.getId(), false));
        mixinType1 = typeManager.createRecordType(mixinType1);
        QName mixinName2 = new QName("mixinNS", "testMixinUpdate2");
        RecordType mixinType2 = typeManager.newRecordType(mixinName2);
        mixinType2.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType1.getId(), false));
        mixinType2.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType2.getId(), false));
        mixinType2.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType3.getId(), false));
        mixinType2 = typeManager.createRecordType(mixinType2);
        
        QName recordName = new QName("recordNS", "testMixinUpdate");
        RecordType recordType = typeManager.newRecordType(recordName);
        recordType.addMixin(mixinType1.getId(), mixinType1.getVersion());
        recordType = typeManager.createRecordType(recordType);
        
        recordType.addMixin(mixinType2.getId(), mixinType2.getVersion());
        recordType = typeManager.updateRecordType(recordType);
        assertEquals(Long.valueOf(2), recordType.getVersion());
        assertEquals(recordType, typeManager.getRecordTypeById(recordType.getId(), null));
    }

    @Test
    public void testMixinRemove() throws Exception {
        QName mixinName = new QName("mixinNS", "testMixinRemove");
        RecordType mixinType1 = typeManager.newRecordType(mixinName);
        mixinType1.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType1.getId(), false));
        mixinType1.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType2.getId(), false));
        mixinType1.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType3.getId(), false));
        mixinType1 = typeManager.createRecordType(mixinType1);
        QName mixinName2 = new QName("mixinNS", "testMixinRemove2");
        RecordType mixinType2 = typeManager.newRecordType(mixinName2);
        mixinType2.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType1.getId(), false));
        mixinType2.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType2.getId(), false));
        mixinType2.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType3.getId(), false));
        mixinType2 = typeManager.createRecordType(mixinType2);
        
        QName recordTypeName = new QName("recordNS", "testMixinRemove");
        RecordType recordType = typeManager.newRecordType(recordTypeName);
        recordType.addMixin(mixinType1.getId(), mixinType1.getVersion());
        recordType = typeManager.createRecordType(recordType);
        
        recordType.addMixin(mixinType2.getId(), mixinType2.getVersion());
        recordType.removeMixin(mixinType1.getId());
        recordType = typeManager.updateRecordType(recordType);
        assertEquals(Long.valueOf(2), recordType.getVersion());
        RecordType readRecordType = typeManager.getRecordTypeById(recordType.getId(), null);
        Map<SchemaId, Long> mixins = readRecordType.getMixins();
        assertEquals(1, mixins.size());
        assertEquals(Long.valueOf(1), mixins.get(mixinType2.getId()));
    }
    
    @Test
    public void testGetRecordTypes() throws Exception {
        RecordType recordType = typeManager.createRecordType(typeManager.newRecordType(new QName("NS", "getRecordTypes")));
        Collection<RecordType> recordTypes = typeManager.getRecordTypes();
        assertTrue(recordTypes.contains(recordType));
    }
    
    @Test
    public void testGetFieldTypes() throws Exception {
        FieldType fieldType = typeManager.createFieldType(typeManager.newFieldType(typeManager.getValueType("STRING"), new QName("NS", "getFieldTypes"), Scope.NON_VERSIONED));
        Collection<FieldType> fieldTypes = typeManager.getFieldTypes();
        assertTrue(fieldTypes.contains(fieldType));
    }
    
    @Test
    public void testUpdateName() throws Exception {
        QName name = new QName(namespace1, "testUpdateName");
        RecordType recordType = typeManager.newRecordType(name);
        recordType = typeManager.createRecordType(recordType);
        assertEquals(name, recordType.getName());
        
        QName name2 = new QName(namespace1, "testUpdateName2");
        recordType.setName(name2);
        recordType = typeManager.updateRecordType(recordType);
        recordType = typeManager.getRecordTypeById(recordType.getId(), null);
        assertEquals(name2, recordType.getName());
    }

    @Test
    public void testUpdateNameToExistingNameFails() throws Exception {
        QName name = new QName(namespace1, "testUpdateNameToExistingNameFails");
        QName name2 = new QName(namespace1, "testUpdateNameToExistingNameFails2");

        RecordType recordType = typeManager.newRecordType(name);
        recordType = typeManager.createRecordType(recordType);
        assertEquals(name, recordType.getName());

        RecordType recordType2 = typeManager.newRecordType(name2);
        recordType2 = typeManager.createRecordType(recordType2);

        recordType.setName(name2);
        try {
            System.out.println("Expecting TypeException"); 
            recordType = typeManager.updateRecordType(recordType);
            fail();
        } catch (TypeException expected){
        }
    }
    
    @Test
    public void testCreateOrUpdate() throws Exception {
        String NS = "testCreateOrUpdateRecordType";

        FieldType field1 = typeManager.createFieldType("STRING", new QName(NS, "field1"), Scope.NON_VERSIONED);
        FieldType field2 = typeManager.createFieldType("STRING", new QName(NS, "field2"), Scope.NON_VERSIONED);
        FieldType field3 = typeManager.createFieldType("STRING", new QName(NS, "field3"), Scope.NON_VERSIONED);

        RecordType recordType = typeManager.newRecordType(new QName(NS, "type1"));
        recordType.addFieldTypeEntry(field1.getId(), false);
        recordType.addFieldTypeEntry(field2.getId(), false);

        recordType = typeManager.createOrUpdateRecordType(recordType);
        assertNotNull(recordType.getId());

        // Without changing anything, do an update
        RecordType updatedRecordType = typeManager.createOrUpdateRecordType(recordType);
        assertEquals(recordType, updatedRecordType);

        // Remove the id from the record type and do a change
        recordType.setId(null);
        recordType.addFieldTypeEntry(field3.getId(), false);
        typeManager.createOrUpdateRecordType(recordType);
        recordType = typeManager.getRecordTypeByName(new QName(NS, "type1"), null);
        assertEquals(3, recordType.getFieldTypeEntries().size());
    }

    @Test
    public void testRecordTypeBuilderBasics() throws Exception {
        RecordTypeBuilder builder = typeManager.recordTypeBuilder();
        try {
            builder.create();
            fail("Exception expected since name of recordType is not specified");
        } catch (Exception expected) {
        }
        QName rtName = new QName("builderNS", "builderName");
        builder.name(rtName);
        builder.field(fieldType1.getId(), false);
        builder.field(fieldType2.getId(), true);
        RecordType recordType = builder.create();
        
        RecordType readRecordType = typeManager.getRecordTypeByName(rtName, null);
        assertEquals(recordType, readRecordType);
        assertFalse(readRecordType.getFieldTypeEntry(fieldType1.getId()).isMandatory());
        assertTrue(readRecordType.getFieldTypeEntry(fieldType2.getId()).isMandatory());
        
        builder.reset();
        builder.id(recordType.getId());
        recordType = builder.update();
        readRecordType = typeManager.getRecordTypeByName(rtName, null);
        assertEquals(recordType, readRecordType);
        assertEquals(Long.valueOf(2), readRecordType.getVersion());
        assertNull(readRecordType.getFieldTypeEntry(fieldType1.getId()));
    }

    @Test
    public void testRecordTypeBuilderFieldsAndMixins() throws Exception {
        String NS = "testRecordTypeBuilderFieldsAndMixins";

        //
        // Create some field types
        //
        FieldType field1 = typeManager.createFieldType("STRING", new QName(NS, "field1"), VERSIONED);
        FieldType field2 = typeManager.createFieldType("STRING", new QName(NS, "field2"), VERSIONED);
        FieldType field3 = typeManager.createFieldType("STRING", new QName(NS, "field3"), VERSIONED);
        FieldType field4 = typeManager.createFieldType("STRING", new QName(NS, "field4"), VERSIONED);
        FieldType field5 = typeManager.createFieldType("STRING", new QName(NS, "field5"), VERSIONED);

        //
        // Create some mixins
        //
        FieldType field21 = typeManager.createFieldType("STRING", new QName(NS, "field21"), VERSIONED);
        FieldType field22 = typeManager.createFieldType("STRING", new QName(NS, "field22"), VERSIONED);
        FieldType field23 = typeManager.createFieldType("STRING", new QName(NS, "field23"), VERSIONED);
        FieldType field24 = typeManager.createFieldType("STRING", new QName(NS, "field24"), VERSIONED);
        FieldType field25 = typeManager.createFieldType("STRING", new QName(NS, "field25"), VERSIONED);
        FieldType field26 = typeManager.createFieldType("STRING", new QName(NS, "field26"), VERSIONED);
        FieldType field27 = typeManager.createFieldType("STRING", new QName(NS, "field27"), VERSIONED);
        FieldType field28 = typeManager.createFieldType("STRING", new QName(NS, "field28"), VERSIONED);
        FieldType field29 = typeManager.createFieldType("STRING", new QName(NS, "field29"), VERSIONED);

        RecordType mixinType1 = typeManager.recordTypeBuilder().name(NS, "mixin1").fieldEntry().use(field21).add().create();
        RecordType mixinType2 = typeManager.recordTypeBuilder().name(NS, "mixin2").fieldEntry().use(field22).add().create();
        RecordType mixinType3 = typeManager.recordTypeBuilder().name(NS, "mixin3").fieldEntry().use(field23).add().create();
        RecordType mixinType4 = typeManager.recordTypeBuilder().name(NS, "mixin4").fieldEntry().use(field24).add().create();
        RecordType mixinType5 = typeManager.recordTypeBuilder().name(NS, "mixin5").fieldEntry().use(field25).add().create();
        RecordType mixinType6 = typeManager.recordTypeBuilder().name(NS, "mixin6").fieldEntry().use(field26).add().create();
        RecordType mixinType7 = typeManager.recordTypeBuilder().name(NS, "mixin7").fieldEntry().use(field27).add().create();
        // give mixin7 two more versions
        mixinType7.addFieldTypeEntry(field28.getId(), false);
        mixinType7 = typeManager.updateRecordType(mixinType7);
        mixinType7.addFieldTypeEntry(field29.getId(), false);
        mixinType7 = typeManager.updateRecordType(mixinType7);

        RecordType recordType = typeManager
                .recordTypeBuilder()
                .defaultNamespace(NS)
                .name("recordType1")

                /* Adding previously defined fields */
                /* By ID */
                .fieldEntry().id(field1.getId()).add()
                /* By object + test mandatory flag */
                .fieldEntry().use(field2).mandatory().add()
                /* By non-qualified name */
                .fieldEntry().name("field3").add()
                /* By qualified name */
                .fieldEntry().name(new QName(NS, "field4")).add()
                /* By indirect qualified name*/
                .fieldEntry().name(NS, "field5").add()

                /* Adding newly created fields */
                /* Using default default scope */
                .fieldEntry().defineField().name("field10").type("LIST<STRING>").create().add()
                /* Using default type (STRING) */
                .fieldEntry().defineField().name("field11").create().add()
                /* Using QName */
                .fieldEntry().defineField().name(new QName(NS, "field12")).create().add()
                /* Using explicit scope */
                .fieldEntry().defineField().name("field13").type("LONG").scope(VERSIONED).create().add()
                /* Using different default scope */
                .defaultScope(Scope.VERSIONED)
                .fieldEntry().defineField().name("field14").create().add()
                /* Using indirect qualified name*/
                .fieldEntry().defineField().name(NS, "field15").create().add()

                /* Adding mixins */
                .mixin().id(mixinType1.getId()).add()
                .mixin().name("mixin2").add()
                .mixin().name(new QName(NS, "mixin3")).add()
                .mixin().name(NS, "mixin4").add()
                .mixin().use(mixinType5).add()
                .mixin().name(NS, "mixin7").version(2L).add()

                .create();

        //
        // Global checks
        //
        assertEquals(new QName(NS, "recordType1"), recordType.getName());

        //
        // Verify fields
        //
        assertEquals(11, recordType.getFieldTypeEntries().size());

        assertFalse(recordType.getFieldTypeEntry(field1.getId()).isMandatory());
        assertTrue(recordType.getFieldTypeEntry(field2.getId()).isMandatory());
        assertFalse(recordType.getFieldTypeEntry(field3.getId()).isMandatory());

        // Verify the inline created fields
        FieldType field10 = typeManager.getFieldTypeByName(new QName(NS, "field10"));
        assertEquals("LIST<STRING>", field10.getValueType().getName());
        assertEquals(Scope.NON_VERSIONED, field10.getScope());
        assertNotNull(recordType.getFieldTypeEntry(field10.getId()));

        FieldType field11 = typeManager.getFieldTypeByName(new QName(NS, "field11"));
        assertEquals("STRING", field11.getValueType().getName());
        assertEquals(Scope.NON_VERSIONED, field11.getScope());
        assertNotNull(recordType.getFieldTypeEntry(field11.getId()));

        FieldType field13 = typeManager.getFieldTypeByName(new QName(NS, "field13"));
        assertEquals(Scope.VERSIONED, field13.getScope());

        FieldType field14 = typeManager.getFieldTypeByName(new QName(NS, "field14"));
        assertEquals(Scope.VERSIONED, field14.getScope());

        //
        // Verify mixins
        //
        Map<SchemaId, Long> mixins = recordType.getMixins();
        assertEquals(6, mixins.size());
        assertTrue(mixins.containsKey(mixinType1.getId()));
        assertTrue(mixins.containsKey(mixinType2.getId()));
        assertTrue(mixins.containsKey(mixinType3.getId()));
        assertTrue(mixins.containsKey(mixinType4.getId()));
        assertTrue(mixins.containsKey(mixinType5.getId()));
        assertFalse(mixins.containsKey(mixinType6.getId()));
        assertTrue(mixins.containsKey(mixinType7.getId()));

        assertEquals(new Long(1), mixins.get(mixinType1.getId()));
        assertEquals(new Long(2), mixins.get(mixinType7.getId()));
    }

    @Test
    public void testRecordTypeBuilderCreateOrUpdate() throws Exception {
        String NS = "testRecordTypeBuilderCreateOrUpdate";

        RecordType recordType = null;
        for (int i = 0; i < 3; i++) {
            recordType = typeManager
                    .recordTypeBuilder()
                    .defaultNamespace(NS)
                    .name("recordType1")
                    .fieldEntry().defineField().name("field1").createOrUpdate().add()
                    .fieldEntry().defineField().name("field2").createOrUpdate().add()
                    .createOrUpdate();
        }

        assertEquals(new Long(1L), recordType.getVersion());

        recordType = typeManager
                .recordTypeBuilder()
                .defaultNamespace(NS)
                .name("recordType1")
                .fieldEntry().defineField().name("field1").createOrUpdate().add()
                .fieldEntry().defineField().name("field2").createOrUpdate().add()
                .fieldEntry().defineField().name("field3").createOrUpdate().add()
                .createOrUpdate();

        assertEquals(new Long(2L), recordType.getVersion());
    }
}
