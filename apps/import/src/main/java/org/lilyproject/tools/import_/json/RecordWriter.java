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
package org.lilyproject.tools.import_.json;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.bytes.api.ByteArray;
import org.lilyproject.repository.api.Blob;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.HierarchyPath;
import org.lilyproject.repository.api.Metadata;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.RepositoryManager;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.ValueType;


public class RecordWriter implements EntityWriter<Record> {
    public static RecordWriter INSTANCE = new RecordWriter();

    @Override
    public ObjectNode toJson(Record record, WriteOptions options, RepositoryManager repositoryManager) throws RepositoryException,
            InterruptedException {
        Namespaces namespaces = new NamespacesImpl(options != null ? options.getUseNamespacePrefixes() :
                NamespacesImpl.DEFAULT_USE_PREFIXES);

        ObjectNode recordNode = toJson(record, options, namespaces, repositoryManager);

        if (namespaces.usePrefixes()) {
            recordNode.put("namespaces", NamespacesConverter.toJson(namespaces));
        }

        return recordNode;
    }

    @Override
    public ObjectNode toJson(Record record, WriteOptions options, Namespaces namespaces, RepositoryManager repositoryManager)
            throws RepositoryException, InterruptedException {
        JsonNodeFactory factory = JsonNodeFactory.instance;
        ObjectNode recordNode = factory.objectNode();

        if (record.getId() != null) {
            recordNode.put("id", record.getId().toString());
        }

        if (record.getVersion() != null) {
            recordNode.put("version", record.getVersion());
        }

        if (record.getRecordTypeName() != null) {
            recordNode.put("type", typeToJson(record.getRecordTypeName(), record.getRecordTypeVersion(), namespaces));
        }

        QName versionedTypeName = record.getRecordTypeName(Scope.VERSIONED);
        if (versionedTypeName != null) {
            long version = record.getRecordTypeVersion(Scope.VERSIONED);
            recordNode.put("versionedType", typeToJson(versionedTypeName, version, namespaces));
        }

        QName versionedMutableTypeName = record.getRecordTypeName(Scope.VERSIONED_MUTABLE);
        if (versionedMutableTypeName != null) {
            long version = record.getRecordTypeVersion(Scope.VERSIONED_MUTABLE);
            recordNode.put("versionedMutableType", typeToJson(versionedMutableTypeName, version, namespaces));
        }

        Map<QName, Object> fields = record.getFields();
        if (fields.size() > 0) {
            ObjectNode fieldsNode = recordNode.putObject("fields");

            ObjectNode schemaNode = null;
            if (options.getIncludeSchema()) {
                schemaNode = recordNode.putObject("schema");
            }

            for (Map.Entry<QName, Object> field : fields.entrySet()) {
                FieldType fieldType = repositoryManager.getTypeManager().getFieldTypeByName(field.getKey());
                String fieldName = QNameConverter.toJson(fieldType.getName(), namespaces);

                // fields entry
                fieldsNode.put(
                        fieldName,
                        valueToJson(field.getValue(), fieldType.getValueType(), options, namespaces, repositoryManager));

                // schema entry
                if (schemaNode != null) {
                    schemaNode.put(fieldName, FieldTypeWriter.toJson(fieldType, namespaces, false));
                }
            }
        }

        Map<String, String> attributes = record.getAttributes();
        if (attributes.size() > 0) {
            ObjectNode attributesNode = recordNode.putObject("attributes");
            for (String key : attributes.keySet()) {
                attributesNode.put(key, attributes.get(key));
            }
        }

        Map<QName, Metadata> metadatas = record.getMetadataMap();
        if (!metadatas.isEmpty()) {
            ObjectNode metadatasNode = recordNode.putObject("metadata");

            for (Map.Entry<QName, Metadata> entry : metadatas.entrySet()) {
                String fieldName = QNameConverter.toJson(entry.getKey(), namespaces);
                ObjectNode metadataNode = metadatasNode.putObject(fieldName);

                for (Map.Entry<String, Object> metadata : entry.getValue().getMap().entrySet()) {
                    Object value = metadata.getValue();
                    if (value instanceof String) {
                        metadataNode.put(metadata.getKey(), (String)value);
                    } else if (value instanceof Integer) {
                        metadataNode.put(metadata.getKey(), (Integer)value);
                    } else if (value instanceof Long) {
                        metadataNode.put(metadata.getKey(), (Long)value);
                    } else if (value instanceof Float) {
                        metadataNode.put(metadata.getKey(), (Float)value);
                    } else if (value instanceof Double) {
                        metadataNode.put(metadata.getKey(), (Double)value);
                    } else if (value instanceof Boolean) {
                        metadataNode.put(metadata.getKey(), (Boolean)value);
                    } else if (value instanceof ByteArray) {
                        ObjectNode binaryNode = metadataNode.putObject(metadata.getKey());
                        binaryNode.put("type", "binary");
                        binaryNode.put("value", ((ByteArray)value).getBytes());
                    } else {
                        throw new RuntimeException("Unsupported type of metadata value: " + value.getClass().getName()
                                + " for value '" + value + "' in metadata field '" + metadata.getKey()
                                + "' of record field " + entry.getKey());
                    }
                }
            }

            ObjectNode metadataToDeleteNode = null;
            for (Map.Entry<QName, Metadata> entry : metadatas.entrySet()) {
                Set<String> fieldsToDelete = entry.getValue().getFieldsToDelete();
                if (!fieldsToDelete.isEmpty()) {
                    if (metadataToDeleteNode == null) {
                        metadataToDeleteNode = recordNode.putObject("metadataToDelete");
                    }

                    String fieldName = QNameConverter.toJson(entry.getKey(), namespaces);
                    ArrayNode array = metadataToDeleteNode.putArray(fieldName);
                    for (String name : fieldsToDelete) {
                        array.add(name);
                    }
                }
            }
        }

        return recordNode;
    }

    private JsonNode listToJson(Object value, ValueType valueType, WriteOptions options, Namespaces namespaces,
            RepositoryManager repositoryManager) throws RepositoryException, InterruptedException {
        List list = (List)value;
        ArrayNode array = JsonNodeFactory.instance.arrayNode();
        for (Object item : list) {
            array.add(valueToJson(item, valueType, options, namespaces, repositoryManager));
        }
        return array;
    }

    private JsonNode pathToJson(Object value, ValueType valueType, WriteOptions options, Namespaces namespaces,
            RepositoryManager repositoryManager) throws RepositoryException, InterruptedException {
        HierarchyPath path = (HierarchyPath)value;
        ArrayNode array = JsonNodeFactory.instance.arrayNode();
        for (Object element : path.getElements()) {
            array.add(valueToJson(element, valueType, options, namespaces, repositoryManager));
        }
        return array;
    }

    public JsonNode valueToJson(Object value, ValueType valueType, WriteOptions options, Namespaces namespaces,
            RepositoryManager repositoryManager) throws RepositoryException, InterruptedException {
        String name = valueType.getBaseName();

        JsonNodeFactory factory = JsonNodeFactory.instance;

        JsonNode result;

        if (name.equals("LIST")) {
            result = listToJson(value, valueType.getNestedValueType(), options, namespaces, repositoryManager);
        } else if (name.equals("PATH")) {
            result = pathToJson(value, valueType.getNestedValueType(), options, namespaces, repositoryManager);
        } else if (name.equals("STRING")) {
            result = factory.textNode((String)value);
        } else if (name.equals("LONG")) {
            result = factory.numberNode((Long)value);
        } else if (name.equals("DOUBLE")) {
            result = factory.numberNode((Double)value);
        } else if (name.equals("BOOLEAN")) {
            result = factory.booleanNode((Boolean)value);
        } else if (name.equals("INTEGER")) {
            result = factory.numberNode((Integer)value);
        } else if (name.equals("URI") || name.equals("DATETIME") || name.equals("DATE") || name.equals("LINK")) {
            result = factory.textNode(value.toString());
        } else if (name.equals("DECIMAL")) {
            result = factory.numberNode((BigDecimal)value);
        } else if (name.equals("BLOB")) {
            Blob blob = (Blob)value;
            result = BlobConverter.toJson(blob);
        } else if (name.equals("RECORD")){
            result = toJson((Record)value, options, namespaces, repositoryManager);
        } else if (name.equals("BYTEARRAY")) {
            result = factory.binaryNode(((ByteArray) value).getBytes());
        } else {
            throw new RuntimeException("Unsupported value type: " + name);
        }

        return result;
    }

    private static JsonNode typeToJson(QName name, Long version, Namespaces namespaces) {
        JsonNodeFactory factory = JsonNodeFactory.instance;
        ObjectNode jsonType = factory.objectNode();

        jsonType.put("name", QNameConverter.toJson(name, namespaces));
        if (version != null) {
            jsonType.put("version", version);
        }

        return jsonType;
    }


}
