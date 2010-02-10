package org.lilycms.hbaseindex;

import java.util.ArrayList;
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

    public IndexFieldDefinition addIntegerField(String name) {
        IndexFieldDefinition definition = new IndexFieldDefinition(name, IndexValueType.INTEGER);
        fields.add(definition);
        return definition;
    }

    public StringIndexFieldDefinition addStringField(String name) {
        StringIndexFieldDefinition definition = new StringIndexFieldDefinition(name);
        fields.add(definition);
        return definition;
    }

    public List<IndexFieldDefinition> getFields() {
        // TODO unmodifiable?
        return fields;
    }
}
