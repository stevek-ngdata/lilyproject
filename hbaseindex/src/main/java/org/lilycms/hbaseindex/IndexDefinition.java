package org.lilycms.hbaseindex;

import org.lilycms.util.ArgumentValidator;

import java.util.*;

public class IndexDefinition {
    private String name;
    private List<IndexFieldDefinition> fields = new ArrayList<IndexFieldDefinition>();
    private Map<String, IndexFieldDefinition> fieldsByName = new HashMap<String, IndexFieldDefinition>();

    public IndexDefinition(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public IndexFieldDefinition getField(String name) {
        return fieldsByName.get(name);
    }

    public StringIndexFieldDefinition addStringField(String name) {
        ArgumentValidator.notNull(name, "name");
        StringIndexFieldDefinition definition = new StringIndexFieldDefinition(name);
        add(definition);
        return definition;
    }

    public IntegerIndexFieldDefinition addIntegerField(String name) {
        ArgumentValidator.notNull(name, "name");
        IntegerIndexFieldDefinition definition = new IntegerIndexFieldDefinition(name);
        add(definition);
        return definition;
    }

    public FloatIndexFieldDefinition addFloatField(String name) {
        ArgumentValidator.notNull(name, "name");
        FloatIndexFieldDefinition definition = new FloatIndexFieldDefinition(name);
        add(definition);
        return definition;
    }

    public DateTimeIndexFieldDefinition addDateTimeField(String name) {
        ArgumentValidator.notNull(name, "name");
        DateTimeIndexFieldDefinition definition = new DateTimeIndexFieldDefinition(name);
        add(definition);
        return definition;
    }

    private void add(IndexFieldDefinition fieldDef) {
        fields.add(fieldDef);
        fieldsByName.put(fieldDef.getName(), fieldDef);
    }

    public List<IndexFieldDefinition> getFields() {
        return Collections.unmodifiableList(fields);
    }
}
