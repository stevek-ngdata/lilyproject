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
package org.lilyproject.hbaseindex;

import org.codehaus.jackson.node.ObjectNode;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class IndexDefinitionSerializationTest {
    @Test
    public void testStringField() throws Exception {
        IndexDefinition indexDef = new IndexDefinition("index");
        StringIndexFieldDefinition field = indexDef.addStringField("stringfield");
        ObjectNode json = indexDef.toJson();

        IndexDefinition newIndexDef = new IndexDefinition("index", json);
        StringIndexFieldDefinition newField = (StringIndexFieldDefinition) newIndexDef.getField("stringfield");

        assertEquals(field.getName(), newField.getName());
    }

    @Test
    public void testIntegerField() throws Exception {
        IndexDefinition indexDef = new IndexDefinition("index");
        IntegerIndexFieldDefinition field = indexDef.addIntegerField("intfield");
        ObjectNode json = indexDef.toJson();

        IndexDefinition newIndexDef = new IndexDefinition("index", json);
        IntegerIndexFieldDefinition newField = (IntegerIndexFieldDefinition) newIndexDef.getField("intfield");

        assertEquals(field.getName(), newField.getName());
    }

    @Test
    public void testFloatField() throws Exception {
        IndexDefinition indexDef = new IndexDefinition("index");
        FloatIndexFieldDefinition field = indexDef.addFloatField("floatfield");
        ObjectNode json = indexDef.toJson();

        IndexDefinition newIndexDef = new IndexDefinition("index", json);
        FloatIndexFieldDefinition newField = (FloatIndexFieldDefinition) newIndexDef.getField("floatfield");

        assertEquals(field.getName(), newField.getName());
    }

    @Test
    public void testByteIndexField() throws Exception {
        IndexDefinition indexDef = new IndexDefinition("index");
        ByteIndexFieldDefinition field = indexDef.addByteField("bytefield", 123);
        ObjectNode json = indexDef.toJson();

        final IndexDefinition newIndexDef = new IndexDefinition("index", json);
        ByteIndexFieldDefinition newField = (ByteIndexFieldDefinition) newIndexDef.getField("bytefield");

        assertEquals(field.getName(), newField.getName());
        assertEquals(field.getLength(), newField.getLength());
    }

}
