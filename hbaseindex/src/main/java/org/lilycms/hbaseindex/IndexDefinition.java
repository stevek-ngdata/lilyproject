package org.lilycms.hbaseindex;

import org.lilycms.util.ArgumentValidator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class IndexDefinition {
    private String name;
    private List<IndexFieldDefinition> fields = new ArrayList<IndexFieldDefinition>();

    public IndexDefinition(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public StringIndexFieldDefinition addStringField(String name) {
        ArgumentValidator.notNull(name, "name");
        StringIndexFieldDefinition definition = new StringIndexFieldDefinition(name);
        fields.add(definition);
        return definition;
    }

    public IntegerIndexFieldDefinition addIntegerField(String name) {
        ArgumentValidator.notNull(name, "name");
        IntegerIndexFieldDefinition definition = new IntegerIndexFieldDefinition(name);
        fields.add(definition);
        return definition;
    }

    public FloatIndexFieldDefinition addFloatField(String name) {
        ArgumentValidator.notNull(name, "name");
        FloatIndexFieldDefinition definition = new FloatIndexFieldDefinition(name);
        fields.add(definition);
        return definition;
    }

    public DateTimeIndexFieldDefinition addDateTimeField(String name) {
        ArgumentValidator.notNull(name, "name");
        DateTimeIndexFieldDefinition definition = new DateTimeIndexFieldDefinition(name);
        fields.add(definition);
        return definition;
    }

    public List<IndexFieldDefinition> getFields() {
        return Collections.unmodifiableList(fields);
    }
}
