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

import java.util.UUID;

import org.junit.Test;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.FieldTypeExistsException;
import org.lilyproject.repository.api.FieldTypeNotFoundException;
import org.lilyproject.repository.api.FieldTypeUpdateException;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.api.ValueType;
import org.lilyproject.repository.impl.id.SchemaIdImpl;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public abstract class AbstractTypeManagerFieldTypeTest {

    private static String namespace = "NS";
    protected static TypeManager typeManager;
    protected static boolean avro = false;

    @Test
    public void testCreate() throws Exception {
        QName name = new QName(namespace, "testCreate");
        ValueType valueType = typeManager.getValueType("STRING");
        FieldType fieldType = typeManager.newFieldType(valueType, name, Scope.NON_VERSIONED);
        fieldType = typeManager.createFieldType(fieldType);
        assertEquals(fieldType, typeManager.getFieldTypeById(fieldType.getId()));
        assertEquals(fieldType, typeManager.getFieldTypeByName(fieldType.getName()));
        assertEquals(typeManager.getFieldTypeById(fieldType.getId()), typeManager.getFieldTypeByName(name));
    }

    @Test
    public void testCreateIgnoresGivenId() throws Exception {
        SchemaId id = new SchemaIdImpl(UUID.randomUUID());
        ValueType valueType = typeManager.getValueType("STRING");
        FieldType fieldType = typeManager.newFieldType(id, valueType, new QName(namespace, "aName"), Scope.VERSIONED_MUTABLE);
        fieldType = typeManager.createFieldType(fieldType);
        assertFalse(fieldType.getId().equals(id));
    }

    @Test
    public void testCreateSameNameFails() throws Exception {
        QName name = new QName(namespace, "testCreateSameNameFails");
        ValueType valueType = typeManager.getValueType("STRING");
        FieldType fieldType = typeManager.newFieldType(valueType, name, Scope.NON_VERSIONED);
        fieldType = typeManager.createFieldType(fieldType);

        ValueType valueType2 = typeManager.getValueType("INTEGER");
        FieldType fieldType2 = typeManager.newFieldType(valueType2, name, Scope.NON_VERSIONED);
        try {
            if (avro) {
                System.out.println("Expecting FieldTypeExistsException");
            }
            fieldType = typeManager.createFieldType(fieldType2);
            fail();
        } catch (FieldTypeExistsException expected) {
        }
    }

    @Test
    public void testUpdate() throws Exception {
        QName name = new QName(namespace, "testUpdate");
        ValueType valueType = typeManager.getValueType("STRING");
        FieldType fieldTypeCreate = typeManager.newFieldType(valueType, name, Scope.VERSIONED);
        fieldTypeCreate = typeManager.createFieldType(fieldTypeCreate);

        // Update name
        FieldType fieldTypeNewName = typeManager.newFieldType(fieldTypeCreate.getId(), valueType, new QName(namespace, "newName"), Scope.VERSIONED);
        fieldTypeNewName = typeManager.updateFieldType(fieldTypeNewName);
        assertEquals(fieldTypeCreate.getId(), fieldTypeNewName.getId());
        assertEquals(fieldTypeNewName, typeManager.getFieldTypeById(fieldTypeCreate.getId()));
        assertEquals(typeManager.getFieldTypeById(fieldTypeCreate.getId()), typeManager.getFieldTypeByName(new QName(namespace, "newName")));

        // Create new fieldType with first name
        ValueType valueType2 = typeManager.getValueType("INTEGER");
        FieldType fieldType2 = typeManager.newFieldType(valueType2, name, Scope.NON_VERSIONED);
        fieldType2 = typeManager.createFieldType(fieldTypeCreate);
        assertEquals(fieldType2, typeManager.getFieldTypeByName(name));
    }

    @Test
    public void testUpdateValueTypeFails() throws Exception {
        QName name = new QName(namespace, "testUpdateValueTypeFails");
        ValueType valueType = typeManager.getValueType("STRING");
        FieldType fieldType = typeManager.newFieldType(valueType, name, Scope.VERSIONED);
        fieldType = typeManager.createFieldType(fieldType);

        fieldType.setValueType(typeManager.getValueType("INTEGER"));
        try {
            if (avro) {
                System.out.println("Expecting FieldTypeUpdateException");
            }
            typeManager.updateFieldType(fieldType);
            fail("Changing the valueType of a fieldType is not allowed.");
        } catch (FieldTypeUpdateException e) {
        }
    }

    @Test
    public void testUpdateScopeFails() throws Exception {
        QName name = new QName(namespace, "testUpdateScopeFails");
        ValueType valueType = typeManager.getValueType("STRING");
        FieldType fieldType = typeManager.newFieldType(valueType, name, Scope.VERSIONED);
        fieldType = typeManager.createFieldType(fieldType);

        fieldType.setScope(Scope.NON_VERSIONED);
        try {
            if (avro) {
                System.out.println("Expecting FieldTypeUpdateException");
            }
            typeManager.updateFieldType(fieldType);
            fail("Changing the scope of a fieldType is not allowed.");
        } catch (FieldTypeUpdateException e) {
        }
    }

    @Test
    public void testUpdateToAnExistingNameFails() throws Exception {
        QName name1 = new QName(namespace, "testUpdateToAnExistingNameFails1");
        ValueType valueType = typeManager.getValueType("STRING");
        FieldType fieldType = typeManager.newFieldType(valueType, name1, Scope.VERSIONED);
        fieldType = typeManager.createFieldType(fieldType);

        QName name2 = new QName(namespace, "testUpdateToAnExistingNameFails2");
        ValueType valueType2 = typeManager.getValueType("STRING");
        FieldType fieldType2 = typeManager.newFieldType(valueType2, name2, Scope.VERSIONED);
        fieldType2 = typeManager.createFieldType(fieldType2);

        fieldType.setName(name2);
        try {
            if (avro) {
                System.out.println("Expecting FieldTypeUpdateException");
            }
            typeManager.updateFieldType(fieldType);
            fail("Updating to a fieldType with an existing name is not allowed.");
        } catch (FieldTypeUpdateException e) {
        }
    }

    @Test
    public void testCreateOrUpdate() throws Exception {
        String NS = "testCreateOrUpdateFieldType";

        //
        // Use createOrUpdate to create a new field type
        //
        FieldType fieldType = typeManager.newFieldType("STRING", new QName(NS, "field1"), Scope.NON_VERSIONED);
        fieldType = typeManager.createOrUpdateFieldType(fieldType);
        assertNotNull(fieldType.getId());
        assertEquals(new QName(NS, "field1"), fieldType.getName());

        //
        // Do a createOrUpdate with nothing changed
        //
        fieldType = typeManager.createOrUpdateFieldType(fieldType);
        assertNotNull(fieldType.getId());
        assertEquals(new QName(NS, "field1"), fieldType.getName());

        //
        // Use createOrUpdate to update the field type
        //
        fieldType.setName(new QName(NS, "field2"));
        fieldType = typeManager.createOrUpdateFieldType(fieldType);
        assertNotNull(fieldType.getId());
        assertEquals(new QName(NS, "field2"), fieldType.getName());

        //
        // Call createOrUpdate without name: should just return the complete type
        //
        fieldType.setName(null);
        fieldType = typeManager.createOrUpdateFieldType(fieldType);
        assertNotNull(fieldType.getId());
        assertEquals(new QName(NS, "field2"), fieldType.getName());

        //
        // Call createOrUpdate with a conflicting field type state
        //
        FieldType conflictFieldType = fieldType.clone();
        conflictFieldType.setScope(Scope.VERSIONED);
        try {
            typeManager.createOrUpdateFieldType(conflictFieldType);
            fail("expected exception");
        } catch (FieldTypeUpdateException e) {
            // expected
        }

        conflictFieldType = fieldType.clone();
        conflictFieldType.setValueType(typeManager.getValueType("LIST<STRING>"));
        try {
            typeManager.createOrUpdateFieldType(conflictFieldType);
            fail("expected exception");
        } catch (FieldTypeUpdateException e) {
            // expected
        }

        //
        // Call createOrUpdate without name and with conflicting state
        //
        conflictFieldType = fieldType.clone();
        conflictFieldType.setName(null);
        conflictFieldType.setScope(Scope.VERSIONED_MUTABLE);
        try {
            typeManager.createOrUpdateFieldType(conflictFieldType);
            fail("expected exception");
        } catch (FieldTypeUpdateException e) {
            // expected
        }

        //
        // Call createOrUpdate with name and without id
        //
        SchemaId expectedId = fieldType.getId();
        fieldType.setId(null);
        fieldType = typeManager.createOrUpdateFieldType(fieldType);
        assertEquals(expectedId, fieldType.getId());
        assertEquals(new QName(NS, "field2"), fieldType.getName());

        //
        // Call createOrUpdate without scope
        //
        fieldType.setScope(null);
        fieldType = typeManager.createOrUpdateFieldType(fieldType);
        assertEquals(Scope.NON_VERSIONED, fieldType.getScope());

        //
        // Call createOrUpdate without value type
        //
        fieldType.setValueType(null);
        fieldType = typeManager.createOrUpdateFieldType(fieldType);
        assertEquals(typeManager.getValueType("STRING"), fieldType.getValueType());
    }

    @Test
    public void testFieldTypeBuilder() throws Exception {
        String NS = "testFieldTypeBuilder";

        FieldType fieldType = typeManager
                .fieldTypeBuilder()
                .name(NS, "field1")
                .create();

        // Verify setting of defaults
        assertEquals(Scope.NON_VERSIONED, fieldType.getScope());
        assertEquals(typeManager.getValueType("STRING"), fieldType.getValueType());

        // Supplying ID only should be enough for createOrUpdate to return the stored state
        fieldType = typeManager
                .fieldTypeBuilder()
                .id(fieldType.getId())
                .createOrUpdate();

        assertEquals(new QName(NS, "field1"), fieldType.getName());
        assertEquals(Scope.NON_VERSIONED, fieldType.getScope());
        assertEquals(typeManager.getValueType("STRING"), fieldType.getValueType());

        fieldType = typeManager
                .fieldTypeBuilder()
                .name(NS, "field2")
                .type("LIST<STRING>")
                .scope(Scope.VERSIONED)
                .createOrUpdate();

        assertEquals(Scope.VERSIONED, fieldType.getScope());
        assertEquals(typeManager.getValueType("LIST<STRING>"), fieldType.getValueType());

        try {
            typeManager
                    .fieldTypeBuilder()
                    .id(new SchemaIdImpl(UUID.randomUUID()))
                    .name(NS, "field3")
                    .update();
            fail("expected exception");
        } catch (FieldTypeNotFoundException e) {
            // expected
        }
    }
}
