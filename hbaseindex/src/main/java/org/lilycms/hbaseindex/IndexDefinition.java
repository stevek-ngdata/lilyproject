package org.lilycms.hbaseindex;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.lilycms.util.ArgumentValidator;

import java.lang.reflect.Constructor;
import java.util.*;

public class IndexDefinition {
    private String name;
    private List<IndexFieldDefinition> fields = new ArrayList<IndexFieldDefinition>();
    private Map<String, IndexFieldDefinition> fieldsByName = new HashMap<String, IndexFieldDefinition>();

    public IndexDefinition(String name) {
        ArgumentValidator.notNull(name, "name");
        this.name = name;
    }

    public IndexDefinition(String name, ObjectNode jsonObject) {
        this.name = name;

        try {
            ObjectNode fields = (ObjectNode)jsonObject.get("fields");
            Iterator<Map.Entry<String, JsonNode>> fieldsIt = fields.getFields();
            while (fieldsIt.hasNext()) {
                Map.Entry<String, JsonNode> entry = fieldsIt.next();
                String className = entry.getValue().get("class").getTextValue();
                Class<IndexFieldDefinition> clazz = (Class<IndexFieldDefinition>)getClass().getClassLoader().loadClass(className); 
                Constructor<IndexFieldDefinition> constructor = clazz.getConstructor(String.class, ObjectNode.class);
                IndexFieldDefinition field = constructor.newInstance(entry.getKey(), entry.getValue());
                add(field);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error instantiating IndexDefinition.", e);
        }
    }

    public String getName() {
        return name;
    }

    public IndexFieldDefinition getField(String name) {
        return fieldsByName.get(name);
    }

    public StringIndexFieldDefinition addStringField(String name) {
        validateName(name);
        StringIndexFieldDefinition definition = new StringIndexFieldDefinition(name);
        add(definition);
        return definition;
    }

    public IntegerIndexFieldDefinition addIntegerField(String name) {
        validateName(name);
        IntegerIndexFieldDefinition definition = new IntegerIndexFieldDefinition(name);
        add(definition);
        return definition;
    }

    public FloatIndexFieldDefinition addFloatField(String name) {
        validateName(name);
        FloatIndexFieldDefinition definition = new FloatIndexFieldDefinition(name);
        add(definition);
        return definition;
    }

    public DateTimeIndexFieldDefinition addDateTimeField(String name) {
        validateName(name);
        DateTimeIndexFieldDefinition definition = new DateTimeIndexFieldDefinition(name);
        add(definition);
        return definition;
    }

    private void add(IndexFieldDefinition fieldDef) {
        fields.add(fieldDef);
        fieldsByName.put(fieldDef.getName(), fieldDef);
    }

    private void validateName(String name) {
        ArgumentValidator.notNull(name, "name");
        if (fieldsByName.containsKey(name)) {
            throw new IllegalArgumentException("Field name already exists in this IndexDefinition: " + name);
        }
    }

    public List<IndexFieldDefinition> getFields() {
        return Collections.unmodifiableList(fields);
    }

    public ObjectNode toJson() {
        JsonNodeFactory factory = JsonNodeFactory.instance;
        ObjectNode object = factory.objectNode();
        ObjectNode fieldsJson = object.putObject("fields");

        for (IndexFieldDefinition field : fields) {
            fieldsJson.put(field.getName(), field.toJson());
        }

        return object;
    }

}
