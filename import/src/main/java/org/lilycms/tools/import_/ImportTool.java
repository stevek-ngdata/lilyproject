package org.lilycms.tools.import_;

import org.codehaus.jackson.*;
import org.codehaus.jackson.map.ObjectMapper;
import org.lilycms.client.Client;
import org.lilycms.repository.api.*;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.*;

public class ImportTool {
    private Map<String, String> namespaces = new HashMap<String, String>();
    private Repository repository;
    private TypeManager typeManager;

    public static void main(String[] args) throws Exception {
        String fileName = args[0];

        Client client = new Client("localhost:21812");

        InputStream is = new FileInputStream(fileName);
        load(client.getRepository(), is);
    }

    public static void load(Repository repository, InputStream is) throws Exception {
        new ImportTool(repository).load(is);
    }

    private ImportTool(Repository repository) {
        this.repository = repository;
        this.typeManager = repository.getTypeManager();
    }

    public void load(InputStream is) throws Exception {
        // A combination of the Jackson streaming and tree APIs is used: we moving streaming through the
        // whole of the file, but use the tree API to load individual items (field types, records, ...).
        // This way things should still work fast and within little memory if anyone would use this to
        // load large amounts of records.

        JsonFactory f = new JsonFactory();
        f.setCodec(new ObjectMapper());
        JsonParser jp = f.createJsonParser(is);

        JsonToken current;
        current = jp.nextToken();

        if (current != JsonToken.START_OBJECT) {
            System.out.println("Error: expected object node as root of the input. Giving up.");
            return;
        }

        while (jp.nextToken() != JsonToken.END_OBJECT) {
            String fieldName = jp.getCurrentName();
            current = jp.nextToken(); // move from field name to field value
            if (fieldName.equals("namespaces")) {
                if (current == JsonToken.START_ARRAY) {
                    while (jp.nextToken() != JsonToken.END_ARRAY) {
                        loadNamespace(jp.readValueAsTree());
                    }
                } else {
                    System.out.println("Error: namespaces property should be an array. Skipping.");
                    jp.skipChildren();
                }
            } else if (fieldName.equals("fieldTypes")) {
                if (current == JsonToken.START_ARRAY) {
                    while (jp.nextToken() != JsonToken.END_ARRAY) {
                        loadFieldType(jp.readValueAsTree());
                    }
                } else {
                    System.out.println("Error: fieldTypes property should be an array. Skipping.");
                    jp.skipChildren();
                }
            } else if (fieldName.equals("recordTypes")) {
                if (current == JsonToken.START_ARRAY) {
                    while (jp.nextToken() != JsonToken.END_ARRAY) {
                        loadRecordType(jp.readValueAsTree());
                    }
                } else {
                    System.out.println("Error: recordTypes property should be an array. Skipping.");
                    jp.skipChildren();
                }
            } else if (fieldName.equals("records")) {
                if (current == JsonToken.START_ARRAY) {
                    while (jp.nextToken() != JsonToken.END_ARRAY) {
                        loadRecord(jp.readValueAsTree());
                    }
                } else {
                    System.out.println("Error: records property should be an array. Skipping.");
                    jp.skipChildren();
                }
            }
        }
    }

    private void loadNamespace(JsonNode node) {
        namespaces.put(getString(node, "prefix"), getString(node, "uri"));
    }

    private void loadFieldType(JsonNode node) throws FieldTypeExistsException, RepositoryException {
        QName name = getQName(node, "name");

        JsonNode vtype = getNode(node, "valueType");
        String primitive = getString(vtype, "primitive");
        boolean multiValue = getBoolean(vtype, "multiValue", false);
        boolean hierarchical = getBoolean(vtype, "hierarchical", false);

        String scopeName = getString(node, "scope", "non_versioned");
        Scope scope = parseScope(scopeName);

        FieldType fieldType = null;
        try {
            fieldType = typeManager.getFieldTypeByName(name);
        } catch (FieldTypeNotFoundException e) {
            // ok
        }

        if (fieldType != null) {
            // check it is similar
            String primitive2 = fieldType.getValueType().getPrimitive().getName();
            checkEquals(primitive2, primitive, "Field type " + name, "primitive type");

            boolean multiValue2 = fieldType.getValueType().isMultiValue();
            checkEquals(multiValue2, multiValue, "Field type " + name, "multi-value");

            boolean hierarchical2 = fieldType.getValueType().isHierarchical();
            checkEquals(hierarchical2, hierarchical, "Field type " + name, "hierarchical");

            Scope scope2 = fieldType.getScope();
            checkEquals(scope2, scope, "Field type " + name, "scope");

            // everything equal, skip it
            System.out.println("Field type already exists and is equal: " + name);
            return;
        }

        ValueType valueType = typeManager.getValueType(primitive, multiValue, hierarchical);
        fieldType = typeManager.newFieldType(valueType, name, scope);
        typeManager.createFieldType(fieldType);
        System.out.println("Field type created: " + name);
    }

    private void loadRecordType(JsonNode node) throws RepositoryException, FieldTypeNotFoundException, RecordTypeExistsException, RecordTypeNotFoundException {
        String name = getString(node, "name");

        JsonNode fields = getNode(node, "fields");
        Set<FieldTypeEntry> fieldTypeEntries = new HashSet<FieldTypeEntry>();
        for (int j = 0; j < fields.size(); j++) {
            JsonNode field = fields.get(j);
            QName fieldName = getQName(field, "name");
            boolean mandatory = getBoolean(field, "mandatory", false);

            String fieldId;
            try {
                fieldId = typeManager.getFieldTypeByName(fieldName).getId();
            } catch (FieldTypeNotFoundException e) {
                throw new RuntimeException("Record type " + name + ": field type " + fieldName + " does not exist.");
            }
            fieldTypeEntries.add(typeManager.newFieldTypeEntry(fieldId, mandatory));
        }

        RecordType recordType = null;
        try {
            recordType = typeManager.getRecordType(name, null);
        } catch (RecordTypeNotFoundException e) {
            // ok
        }

        if (recordType != null) {
            // check it is similar
            Set<FieldTypeEntry> fieldTypeEntries2 = new HashSet<FieldTypeEntry>(recordType.getFieldTypeEntries());
            boolean updated = false;
            if (!fieldTypeEntries.equals(fieldTypeEntries2)) {
                updated = true;
                // update the record type
                for (FieldTypeEntry entry : fieldTypeEntries) {
                    if (recordType.getFieldTypeEntry(entry.getFieldTypeId()) == null) {
                        recordType.addFieldTypeEntry(entry);
                    }
                }
            }

            // TODO mixins


            if (updated) {
                recordType = typeManager.updateRecordType(recordType);
                System.out.println("Record type updated: " + name);
            } else {
                System.out.println("Record type already exists and is equal: " + name);
            }
        } else {
            recordType = typeManager.newRecordType(name);
            for (FieldTypeEntry entry : fieldTypeEntries) {
                recordType.addFieldTypeEntry(entry);
            }
            recordType = typeManager.createRecordType(recordType);
            System.out.println("Record type created: " + name);
        }
    }

    private void loadRecord(JsonNode node) throws FieldTypeNotFoundException, RepositoryException,
            RecordExistsException, RecordNotFoundException, RecordTypeNotFoundException, InvalidRecordException {
        Record record = repository.newRecord();

        String type = getString(node, "type");
        record.setRecordType(type);

        Iterator<String> it = node.getFieldNames();
        while (it.hasNext()) {
            String name = it.next();
            if (name.contains(":")) {
                QName qname = parseQName(name);
                String value = getString(node, name);
                record.setField(qname, value);
            }
        }

        record = repository.create(record);
        System.out.println("Created record " + record.getId());
    }

    private void checkEquals(Object existingValue, Object value, String thingName, String prop) {
        if (!existingValue.equals(value)) {
            throw new RuntimeException(String.format("%1$s exists but with %2$s %3$s instead of %4$s", thingName,
                    prop, existingValue, value));
        }

    }

    private JsonNode getNode(JsonNode node, String prop) {
        if (node.get(prop) == null) {
            throw new RuntimeException("Missing required property: " + prop);
        }
        return node.get(prop);
    }

    private String getString(JsonNode node, String prop) {
        if (node.get(prop) == null) {
            throw new RuntimeException("Missing required property: " + prop);
        }
        if (!node.get(prop).isTextual()) {
            throw new RuntimeException("Not a string property: " + prop);
        }
        return node.get(prop).getTextValue();
    }

    private String getString(JsonNode node, String prop, String defaultValue) {
        if (node.get(prop) == null) {
            return defaultValue;
        }
        if (!node.get(prop).isTextual()) {
            throw new RuntimeException("Not a string property: " + prop);
        }
        return node.get(prop).getTextValue();
    }

    private boolean getBoolean(JsonNode node, String prop, boolean defaultValue) {
        if (node.get(prop) == null) {
            return defaultValue;
        }
        if (!node.get(prop).isBoolean()) {
            throw new RuntimeException("Not a string property: " + prop);
        }
        return node.get(prop).getBooleanValue();
    }

    private QName getQName(JsonNode node, String prop) {
        if (node.get(prop) == null) {
            throw new RuntimeException("Missing required property: " + prop);
        }
        if (!node.get(prop).isTextual()) {
            throw new RuntimeException("Not a string property: " + prop);
        }
        String name = node.get(prop).getTextValue();
        return parseQName(name);
    }

    private QName parseQName(String name) {
        int pos = name.indexOf(':');
        if (pos == -1) {
            throw new RuntimeException("Invalid qualified name: " + name);
        }

        String prefix = name.substring(0, pos);
        String localName = name.substring(pos + 1);
        String uri = namespaces.get(prefix);
        if (uri == null) {
            throw new RuntimeException("Undefined prefix in qualified name: " + name);
        }

        return new QName(uri, localName);
    }

    private Scope parseScope(String scopeName) {
        scopeName = scopeName.toLowerCase();
        if (scopeName.equals("non_versioned")) {
            return Scope.NON_VERSIONED;
        } else if (scopeName.equals("versioned")) {
            return Scope.VERSIONED;
        } else if (scopeName.equals("versioned-mutable")) {
            return Scope.VERSIONED_MUTABLE;
        } else {
            throw new RuntimeException("Unrecognized scope name: " + scopeName);
        }
    }
}
