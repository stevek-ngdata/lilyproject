package org.lilycms.hbaseindex.test;

import org.codehaus.jackson.node.ObjectNode;
import org.junit.Test;
import org.lilycms.hbaseindex.*;

import static org.junit.Assert.*;

public class IndexDefinitionSerializationTest {
    @Test
    public void testStringField() throws Exception {
        IndexDefinition indexDef = new IndexDefinition("index");
        StringIndexFieldDefinition field = indexDef.addStringField("stringfield");
        ObjectNode json = indexDef.toJson();

        IndexDefinition newIndexDef = new IndexDefinition("index", json);
        StringIndexFieldDefinition newField = (StringIndexFieldDefinition)newIndexDef.getField("stringfield");

        assertEquals(field.getName(), newField.getName());
        assertEquals(field.getByteLength(), newField.getByteLength());
        assertEquals(field.isCaseSensitive(), newField.isCaseSensitive());
        assertEquals(field.getByteEncodeMode(), newField.getByteEncodeMode());
        assertEquals(field.getLocale(), newField.getLocale());
    }

    @Test
    public void testIntegerField() throws Exception {
        IndexDefinition indexDef = new IndexDefinition("index");
        IntegerIndexFieldDefinition field = indexDef.addIntegerField("intfield");
        ObjectNode json = indexDef.toJson();

        IndexDefinition newIndexDef = new IndexDefinition("index", json);
        IntegerIndexFieldDefinition newField = (IntegerIndexFieldDefinition)newIndexDef.getField("intfield");

        assertEquals(field.getName(), newField.getName());
    }

    @Test
    public void testFloatField() throws Exception {
        IndexDefinition indexDef = new IndexDefinition("index");
        FloatIndexFieldDefinition field = indexDef.addFloatField("floatfield");
        ObjectNode json = indexDef.toJson();

        IndexDefinition newIndexDef = new IndexDefinition("index", json);
        FloatIndexFieldDefinition newField = (FloatIndexFieldDefinition)newIndexDef.getField("floatfield");

        assertEquals(field.getName(), newField.getName());
    }

    @Test
    public void testDateTimeField() throws Exception {
        IndexDefinition indexDef = new IndexDefinition("index");
        DateTimeIndexFieldDefinition field = indexDef.addDateTimeField("datetimefield");
        field.setPrecision(DateTimeIndexFieldDefinition.Precision.DATETIME);
        ObjectNode json = indexDef.toJson();

        IndexDefinition newIndexDef = new IndexDefinition("index", json);
        DateTimeIndexFieldDefinition newField = (DateTimeIndexFieldDefinition)newIndexDef.getField("datetimefield");

        assertEquals(field.getName(), newField.getName());
        assertEquals(field.getPrecision(), newField.getPrecision());
    }
}
