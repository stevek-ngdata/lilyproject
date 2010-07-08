package org.lilycms.tools.import_;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.MappingJsonFactory;
import org.lilycms.client.Client;
import org.lilycms.repository.api.*;
import static org.lilycms.repoutil.JsonUtil.*;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.*;

public class JsonImportTool {
    private ImportTool importTool;
    private Map<String, String> namespaces = new HashMap<String, String>();
    private Repository repository;
    private TypeManager typeManager;

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Specify two arguments: file to import, zookeeper connect string");
            System.exit(1);
        }

        String fileName = args[0];
        String zookeeperConnectString = args[1];

        Client client = new Client(zookeeperConnectString);

        InputStream is = new FileInputStream(fileName);
        load(client.getRepository(), is);
    }

    public static void load(Repository repository, InputStream is) throws Exception {
        load(repository, new DefaultImportListener(), is);
    }

    public static void load(Repository repository, ImportListener importListener, InputStream is) throws Exception {
        new JsonImportTool(repository, importListener).load(is);
    }

    public JsonImportTool(Repository repository, ImportListener importListener) {
        this.importTool = new ImportTool(repository, importListener);
        this.repository = repository;
        this.typeManager = repository.getTypeManager();
    }

    public ImportTool getImportTool() {
        return importTool;
    }

    public void load(InputStream is) throws Exception {
        // A combination of the Jackson streaming and tree APIs is used: we move streaming through the
        // whole of the file, but use the tree API to load individual items (field types, records, ...).
        // This way things should still work fast and within little memory if anyone would use this to
        // load large amounts of records.

        namespaces.clear();

        JsonFactory f = new MappingJsonFactory();
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
                        addNamespace(jp.readValueAsTree());
                    }
                } else {
                    System.out.println("Error: namespaces property should be an array. Skipping.");
                    jp.skipChildren();
                }
            } else if (fieldName.equals("fieldTypes")) {
                if (current == JsonToken.START_ARRAY) {
                    while (jp.nextToken() != JsonToken.END_ARRAY) {
                        importFieldType(jp.readValueAsTree());
                    }
                } else {
                    System.out.println("Error: fieldTypes property should be an array. Skipping.");
                    jp.skipChildren();
                }
            } else if (fieldName.equals("recordTypes")) {
                if (current == JsonToken.START_ARRAY) {
                    while (jp.nextToken() != JsonToken.END_ARRAY) {
                        importRecordType(jp.readValueAsTree());
                    }
                } else {
                    System.out.println("Error: recordTypes property should be an array. Skipping.");
                    jp.skipChildren();
                }
            } else if (fieldName.equals("records")) {
                if (current == JsonToken.START_ARRAY) {
                    while (jp.nextToken() != JsonToken.END_ARRAY) {
                        importRecord(jp.readValueAsTree());
                    }
                } else {
                    System.out.println("Error: records property should be an array. Skipping.");
                    jp.skipChildren();
                }
            }
        }
    }

    public void setNamespaces(Map<String, String> namespaces) {
        this.namespaces = namespaces;
    }

    public void addNamespace(JsonNode node) throws ImportException {
        namespaces.put(getString(node, "prefix"), getString(node, "uri"));
    }

    public FieldType importFieldType(JsonNode node) throws RepositoryException, ImportConflictException, ImportException {
        QName name = getQName(node, "name");

        JsonNode vtype = getNode(node, "valueType");
        String primitive = getString(vtype, "primitive");
        boolean multiValue = getBoolean(vtype, "multiValue", false);
        boolean hierarchical = getBoolean(vtype, "hierarchical", false);

        String scopeName = getString(node, "scope", "non_versioned");
        Scope scope = parseScope(scopeName);

        ValueType valueType = typeManager.getValueType(primitive, multiValue, hierarchical);
        FieldType fieldType = typeManager.newFieldType(valueType, name, scope);

        return importTool.importFieldType(fieldType);
    }

    public RecordType importRecordType(JsonNode node) throws RepositoryException, ImportException {
        String name = getString(node, "name");
        RecordType recordType = typeManager.newRecordType(name);

        JsonNode fields = getNode(node, "fields");
        for (int j = 0; j < fields.size(); j++) {
            JsonNode field = fields.get(j);
            QName fieldName = getQName(field, "name");
            boolean mandatory = getBoolean(field, "mandatory", false);

            String fieldId;
            try {
                fieldId = typeManager.getFieldTypeByName(fieldName).getId();
            } catch (FieldTypeNotFoundException e) {
                throw new ImportException("Record type " + name + ": field type " + fieldName + " does not exist.");
            }

            recordType.addFieldTypeEntry(fieldId, mandatory);
        }

        return importTool.importRecordType(recordType);
    }

    private void importRecord(JsonNode node) throws RepositoryException, ImportException {
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

    private QName getQName(JsonNode node, String prop) throws ImportException {
        if (node.get(prop) == null) {
            throw new ImportException("Missing required property: " + prop);
        }
        if (!node.get(prop).isTextual()) {
            throw new ImportException("Not a string property: " + prop);
        }
        String name = node.get(prop).getTextValue();
        return parseQName(name);
    }

    private QName parseQName(String name) throws ImportException {
        int pos = name.indexOf(':');
        if (pos == -1) {
            throw new ImportException("Invalid qualified name: " + name);
        }

        String prefix = name.substring(0, pos);
        String localName = name.substring(pos + 1);
        String uri = namespaces.get(prefix);
        if (uri == null) {
            throw new ImportException("Undefined prefix in qualified name: " + name);
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
