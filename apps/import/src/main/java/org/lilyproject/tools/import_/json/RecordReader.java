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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.lilyproject.bytes.api.ByteArray;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.HierarchyPath;
import org.lilyproject.repository.api.MetadataBuilder;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.RepositoryManager;
import org.lilyproject.repository.api.ValueType;
import org.lilyproject.util.json.JsonUtil;

import static org.lilyproject.util.json.JsonUtil.getArray;
import static org.lilyproject.util.json.JsonUtil.getObject;
import static org.lilyproject.util.json.JsonUtil.getString;

public class RecordReader implements EntityReader<Record> {
    public static RecordReader INSTANCE = new RecordReader();
    private final LinkTransformer defaultLinkTransformer = new DefaultLinkTransformer();

    @Override
    public Record fromJson(JsonNode node, RepositoryManager repositoryManager) throws JsonFormatException, RepositoryException,
            InterruptedException {
        return fromJson(node, null, repositoryManager);
    }

    @Override
    public Record fromJson(JsonNode nodeNode, Namespaces namespaces, RepositoryManager repositoryManager)
            throws JsonFormatException, RepositoryException, InterruptedException {
        return fromJson(nodeNode, namespaces, repositoryManager, defaultLinkTransformer);
    }


    @Override
    public Record fromJson(JsonNode nodeNode, Namespaces namespaces, RepositoryManager repositoryManager,
                            LinkTransformer linkTransformer)
            throws JsonFormatException, RepositoryException, InterruptedException {

        if (!nodeNode.isObject()) {
            throw new JsonFormatException("Expected a json object for record, got: " +
                    nodeNode.getClass().getName());
        }

        ObjectNode node = (ObjectNode)nodeNode;

        namespaces = NamespacesConverter.fromContextJson(node, namespaces);

        Record record = repositoryManager.getRecordFactory().newRecord();

        String id = getString(node, "id", null);
        if (id != null) {
            record.setId(repositoryManager.getIdGenerator().fromString(id));
        }

        JsonNode typeNode = node.get("type");
        if (typeNode != null) {
            if (typeNode.isObject()) {
                QName qname = QNameConverter.fromJson(JsonUtil.getString(typeNode, "name"), namespaces);
                Long version = JsonUtil.getLong(typeNode, "version", null);
                record.setRecordType(qname, version);
            } else if (typeNode.isTextual()) {
                record.setRecordType(QNameConverter.fromJson(typeNode.getTextValue(), namespaces));
            }
        }

        ObjectNode fields = getObject(node, "fields", null);
        if (fields != null) {
            Iterator<Map.Entry<String, JsonNode>> it = fields.getFields();
            while (it.hasNext()) {
                Map.Entry<String, JsonNode> entry = it.next();

                QName qname = QNameConverter.fromJson(entry.getKey(), namespaces);
                FieldType fieldType = repositoryManager.getTypeManager().getFieldTypeByName(qname);
                Object value = readValue(fields.get(entry.getKey()), fieldType.getValueType(), entry.getKey(),
                        namespaces, repositoryManager, linkTransformer);
                record.setField(qname, value);
            }
        }

        ArrayNode fieldsToDelete = getArray(node, "fieldsToDelete", null);
        if (fieldsToDelete != null) {
            for (int i = 0; i < fieldsToDelete.size(); i++) {
                JsonNode fieldToDelete = fieldsToDelete.get(i);
                if (!fieldToDelete.isTextual()) {
                    throw new JsonFormatException("fieldsToDelete should be an array of strings, encountered: " + fieldToDelete);
                } else {
                    QName qname = QNameConverter.fromJson(fieldToDelete.getTextValue(), namespaces);
                    record.getFieldsToDelete().add(qname);
                }
            }
        }

        ObjectNode attributes = getObject(node, "attributes", null);
        if (attributes != null) {
            Iterator<Map.Entry<String, JsonNode>> it = attributes.getFields();
            while(it.hasNext()) {
                Map.Entry<String, JsonNode> entry = it.next();
                record.getAttributes().put(entry.getKey(), entry.getValue().getTextValue());
            }
        }

        Map<QName, MetadataBuilder> metadataBuilders = null;
        ObjectNode metadata = getObject(node, "metadata", null);
        if (metadata != null) {
            metadataBuilders = new HashMap<QName, MetadataBuilder>();
            Iterator<Map.Entry<String, JsonNode>> it = metadata.getFields();
            while (it.hasNext()) {
                Map.Entry<String, JsonNode> entry = it.next();
                QName qname = QNameConverter.fromJson(entry.getKey(), namespaces);
                MetadataBuilder builder = readMetadata(entry.getValue(), qname);
                metadataBuilders.put(qname, builder);
            }
        }

        ObjectNode metadataToDelete = getObject(node, "metadataToDelete", null);
        if (metadataToDelete != null) {
            if (metadataBuilders == null) {
                metadataBuilders = new HashMap<QName, MetadataBuilder>();
            }

            Iterator<Map.Entry<String, JsonNode>> it = metadataToDelete.getFields();
            while (it.hasNext()) {
                Map.Entry<String, JsonNode> entry = it.next();
                QName qname = QNameConverter.fromJson(entry.getKey(), namespaces);
                MetadataBuilder builder = readMetadataToDelete(entry.getValue(), metadataBuilders.get(qname), qname);
                metadataBuilders.put(qname, builder);
            }
        }

        if (metadataBuilders != null) {
            for (Map.Entry<QName, MetadataBuilder> entry : metadataBuilders.entrySet()) {
                record.setMetadata(entry.getKey(), entry.getValue().build());
            }
        }

        return record;
    }

    private Object readList(JsonNode node, ValueType valueType, String prop, Namespaces namespaces, RepositoryManager repositoryManager, LinkTransformer linkTransformer)
            throws JsonFormatException, RepositoryException, InterruptedException {
        if (!node.isArray()) {
            throw new JsonFormatException("List value should be specified as array in " + prop);
        }

        List<Object> value = new ArrayList<Object>();
        for (int i = 0; i < node.size(); i++) {
            value.add(readValue(node.get(i), valueType, prop, namespaces, repositoryManager, linkTransformer));
        }

        return value;
    }

    private Object readPath(JsonNode node, ValueType valueType, String prop, Namespaces namespaces,
                                RepositoryManager repositoryManager, LinkTransformer linkTransformer)
            throws JsonFormatException, RepositoryException, InterruptedException {

        if (!node.isArray()) {
            throw new JsonFormatException("Path value should be specified as an array in " + prop);
        }

        Object[] elements = new Object[node.size()];
        for (int i = 0; i < node.size(); i++) {
            elements[i] = readValue(node.get(i), valueType, prop, namespaces, repositoryManager, linkTransformer);
        }

        return new HierarchyPath(elements);
    }

    public Object readValue(JsonNode node, ValueType valueType, String prop, Namespaces namespaces,
                            RepositoryManager repositoryManager, LinkTransformer linkTransformer)
            throws JsonFormatException, RepositoryException, InterruptedException {

        String name = valueType.getBaseName();

        if (name.equals("LIST")) {
            return readList(node, valueType.getNestedValueType(), prop, namespaces, repositoryManager, linkTransformer);
        } else if (name.equals("PATH")) {
            return readPath(node, valueType.getNestedValueType(), prop, namespaces, repositoryManager, linkTransformer);
        } else if (name.equals("STRING")) {
            if (!node.isTextual()) {
                throw new JsonFormatException("Expected text value for property '" + prop + "'");
            }

            return node.getTextValue();
        } else if (name.equals("INTEGER")) {
            if (!node.isIntegralNumber()) {
                throw new JsonFormatException("Expected int value for property '" + prop + "'");
            }

            return node.getIntValue();
        } else if (name.equals("LONG")) {
            if (!node.isIntegralNumber()) {
                throw new JsonFormatException("Expected long value for property '" + prop + "'");
            }

            return node.getLongValue();
        } else if (name.equals("DOUBLE")) {
            if (!node.isNumber()) {
                throw new JsonFormatException("Expected double value for property '" + prop + "'");
            }

            return node.getDoubleValue();
        } else if (name.equals("DECIMAL")) {
            if (!node.isNumber()) {
                throw new JsonFormatException("Expected decimal value for property '" + prop + "'");
            }

            return node.getDecimalValue();
        } else if (name.equals("URI")) {
            if (!node.isTextual()) {
                throw new JsonFormatException("Expected URI (string) value for property '" + prop + "'");
            }

            try {
                return new URI(node.getTextValue());
            } catch (URISyntaxException e) {
                throw new JsonFormatException("Invalid URI in property '" + prop + "': " + node.getTextValue());
            }
        } else if (name.equals("BOOLEAN")) {
            if (!node.isBoolean()) {
                throw new JsonFormatException("Expected boolean value for property '" + prop + "'");
            }

            return node.getBooleanValue();
        } else if (name.equals("LINK")) {
            if (!node.isTextual()) {
                throw new JsonFormatException("Expected text value for property '" + prop + "'");
            }

            return linkTransformer.transform(node.getTextValue(), repositoryManager);
        } else if (name.equals("DATE")) {
            if (!node.isTextual()) {
                throw new JsonFormatException("Expected text value for property '" + prop + "'");
            }

            return new LocalDate(node.getTextValue());
        } else if (name.equals("DATETIME")) {
            if (!node.isTextual()) {
                throw new JsonFormatException("Expected text value for property '" + prop + "'");
            }

            return new DateTime(node.getTextValue());
        } else if (name.equals("BLOB")) {
            if (!node.isObject()) {
                throw new JsonFormatException("Expected object value for property '" + prop + "'");
            }

            ObjectNode blobNode = (ObjectNode)node;
            return BlobConverter.fromJson(blobNode);
        } else if (name.equals("RECORD")) {
            return fromJson(node, namespaces, repositoryManager);

        } else if (name.equals("BYTEARRAY")) {
            if (!node.isTextual()) {
                throw new JsonFormatException("Expected base64 encoded value for property '" + prop + "'");
            }
            try {
                return new ByteArray(node.getBinaryValue());
            } catch (IOException e) {
                throw new JsonFormatException("Could not read base64 value for property '" + prop + "'", e);
            }
        } else {
            throw new JsonFormatException("Value type not supported: " + name);
        }
    }

    private MetadataBuilder readMetadata(JsonNode metadata, QName recordField) throws JsonFormatException {
        if (!metadata.isObject()) {
            throw new JsonFormatException("The value for the metadata should be an object, field: " + recordField);
        }

        ObjectNode object = (ObjectNode)metadata;
        MetadataBuilder builder = new MetadataBuilder();

        Iterator<Map.Entry<String, JsonNode>> it = object.getFields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> entry = it.next();
            String name = entry.getKey();
            JsonNode value = entry.getValue();

            if (value.isTextual()) {
                builder.value(name, value.getTextValue());
            } else if (value.isInt()) {
                builder.value(name, value.getIntValue());
            } else if (value.isLong()) {
                builder.value(name, value.getLongValue());
            } else if (value.isBoolean()) {
                builder.value(name, value.getBooleanValue());
            } else if (value.isFloatingPointNumber()) {
                // In the JSON format, for simplicity, we don't make distinction between float & double, so you
                // can't control which of the two is created.
                builder.value(name, value.getDoubleValue());
            } else if (value.isObject()) {
                String type = JsonUtil.getString(value, "type", null);
                if (type == null) {
                    throw new JsonFormatException("Missing required 'type' property on object in metadata field '"
                            + name + "' of record field " + recordField);
                }

                if (!type.equals("binary")) {
                    throw new JsonFormatException("Unsupported type value '" + type + "' for metadata field '"
                            + name + "' of record field " + recordField);
                }

                JsonNode binaryValue = value.get("value");
                if (!binaryValue.isTextual()) {
                    throw new JsonFormatException("Invalid binary value for metadata field '"
                            + name + "' of record field " + recordField);
                }

                try {
                    builder.value(name, new ByteArray(binaryValue.getBinaryValue()));
                } catch (IOException e) {
                    throw new JsonFormatException("Invalid binary value for metadata field '"
                            + name + "' of record field " + recordField);
                }
            } else {
                throw new JsonFormatException("Unsupported type of value for metadata field '" + name
                        + "' of record field " + recordField);
            }
        }

        return builder;
    }

    private MetadataBuilder readMetadataToDelete(JsonNode metadataToDelete, MetadataBuilder builder,
            QName recordField) throws JsonFormatException {
        if (!metadataToDelete.isArray()) {
            throw new JsonFormatException("The value for the metadataToDelete should be an array, field: " + recordField);
        }

        ArrayNode array = (ArrayNode)metadataToDelete;
        if (builder == null) {
            builder = new MetadataBuilder();
        }

        for (int i = 0; i < array.size(); i++) {
            JsonNode entry = array.get(i);
            if (!entry.isTextual()) {
                throw new JsonFormatException("Non-string found in the metadataToDelete array of field: " + recordField);
            }
            builder.delete(entry.getTextValue());
        }

        return builder;
    }

}
