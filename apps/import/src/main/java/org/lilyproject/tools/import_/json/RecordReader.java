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
import java.math.BigDecimal;
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
import org.joda.time.format.ISODateTimeFormat;
import org.lilyproject.bytes.api.ByteArray;
import org.lilyproject.repository.api.Blob;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.HierarchyPath;
import org.lilyproject.repository.api.LRepository;
import org.lilyproject.repository.api.Link;
import org.lilyproject.repository.api.MetadataBuilder;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.ValueType;
import org.lilyproject.util.json.JsonUtil;

import static org.lilyproject.util.json.JsonUtil.getArray;
import static org.lilyproject.util.json.JsonUtil.getObject;
import static org.lilyproject.util.json.JsonUtil.getString;

public class RecordReader implements EntityReader<Record> {
    public static final RecordReader INSTANCE = new RecordReader();
    private final LinkTransformer defaultLinkTransformer = new DefaultLinkTransformer();

    @Override
    public Record fromJson(JsonNode node, LRepository repository) throws JsonFormatException, RepositoryException,
            InterruptedException {
        return fromJson(node, null, repository);
    }

    @Override
    public Record fromJson(JsonNode nodeNode, Namespaces namespaces, LRepository repository)
            throws JsonFormatException, RepositoryException, InterruptedException {
        return fromJson(nodeNode, namespaces, repository, defaultLinkTransformer);
    }


    @Override
    public Record fromJson(JsonNode nodeNode, Namespaces namespaces, LRepository repository,
            LinkTransformer linkTransformer)
            throws JsonFormatException, RepositoryException, InterruptedException {

        if (!nodeNode.isObject()) {
            throw new JsonFormatException("Expected a json object for record, got: " +
                    nodeNode.getClass().getName());
        }

        ObjectNode node = (ObjectNode)nodeNode;

        namespaces = NamespacesConverter.fromContextJson(node, namespaces);

        return readRootRecord(new ValueHandle(node, "(root object)", null),
                new ReadContext(repository, namespaces, linkTransformer));
    }

    protected Record readRootRecord(ValueHandle handle, ReadContext context)
            throws InterruptedException, RepositoryException, JsonFormatException {
        LRepository repository = context.repository;
        Namespaces namespaces = context.namespaces;
        JsonNode node = handle.node;

        Record record = readCommonRecordAspects(handle, context, true);

        String id = getString(node, "id", null);
        if (id != null) {
            record.setId(repository.getIdGenerator().fromString(id));
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

    protected Record readNestedRecord(ValueHandle handle, ReadContext context)
            throws InterruptedException, RepositoryException, JsonFormatException {
        return readCommonRecordAspects(handle, context, false);
    }

    /**
     * Reads those aspects of a record that are shared between top-level and nested records.
     */
    protected Record readCommonRecordAspects(ValueHandle handle, ReadContext context, boolean topLevelRecord)
            throws JsonFormatException, InterruptedException, RepositoryException {
        LRepository repository = context.repository;
        Namespaces namespaces = context.namespaces;

        Record record = repository.getRecordFactory().newRecord();

        JsonNode typeNode = handle.node.get("type");
        if (typeNode != null) {
            if (typeNode.isObject()) {
                QName qname = QNameConverter.fromJson(JsonUtil.getString(typeNode, "name"), namespaces);
                Long version = JsonUtil.getLong(typeNode, "version", null);
                record.setRecordType(qname, version);
            } else if (typeNode.isTextual()) {
                record.setRecordType(QNameConverter.fromJson(typeNode.getTextValue(), namespaces));
            }
        }

        ObjectNode fields = getObject(handle.node, "fields", null);
        if (fields != null) {
            Iterator<Map.Entry<String, JsonNode>> it = fields.getFields();
            while (it.hasNext()) {
                Map.Entry<String, JsonNode> entry = it.next();

                QName qname = QNameConverter.fromJson(entry.getKey(), namespaces);
                FieldType fieldType = repository.getTypeManager().getFieldTypeByName(qname);
                ValueHandle subHandle = new ValueHandle(fields.get(entry.getKey()), "fields." + entry.getKey(),
                        fieldType.getValueType());
                Object value = readValue(subHandle, context);
                if (value != null) {
                    record.setField(qname, value);
                } else if (value == null && deleteNullFields() && topLevelRecord) {
                    record.delete(qname, true);
                }
            }
        }

        return record;
    }

    protected boolean deleteNullFields() {
        return false;
    }

    protected List<Object> readList(ValueHandle handle, ReadContext context)
            throws JsonFormatException, RepositoryException, InterruptedException {
        JsonNode node = handle.node;
        if (!node.isArray()) {
            throw new JsonFormatException("List value should be specified as array in " + handle.prop);
        }

        List<Object> value = new ArrayList<Object>();
        for (int i = 0; i < node.size(); i++) {
            ValueHandle subHandle = new ValueHandle(node.get(i), handle.prop + "[" + i + "]",
                    handle.valueType.getNestedValueType());
            Object subValue = readValue(subHandle, context);
            if (subValue != null) {
                value.add(subValue);
            }
        }

        return value;
    }

    protected List<Object> readPath(ValueHandle handle, ReadContext context)
            throws JsonFormatException, RepositoryException, InterruptedException {

        JsonNode node = handle.node;

        if (!node.isArray()) {
            throw new JsonFormatException("Path value should be specified as an array in " + handle.prop);
        }

        List<Object> elements = new ArrayList<Object>(node.size());
        for (int i = 0; i < node.size(); i++) {
            ValueHandle subHandle = new ValueHandle(node.get(i), handle.prop,
                    handle.valueType.getNestedValueType());
            Object subValue = readValue(subHandle, context);
            if (subValue != null) {
                elements.add(subValue);
            }
        }

        return new HierarchyPath(elements.toArray(new Object[elements.size()]));
    }

    protected String readString(ValueHandle handle, ReadContext context)
            throws JsonFormatException, RepositoryException, InterruptedException {
        if (!handle.node.isTextual()) {
            throw new JsonFormatException("Expected text value for property '" + handle.prop + "'");
        }
        return handle.node.getTextValue();
    }

    protected Integer readInteger(ValueHandle handle, ReadContext context)
            throws JsonFormatException, RepositoryException, InterruptedException {
        if (handle.node.isIntegralNumber()) {
            return handle.node.getIntValue();
        } else if (handle.node.isTextual()) {
            try {
                return Integer.parseInt(handle.node.getTextValue());
            } catch (NumberFormatException e) {
                throw new JsonFormatException(String.format("Unparsable int value in property '%s': %s", handle.prop,
                        handle.node.getTextValue()));
            }
        } else {
            throw new JsonFormatException("Expected int value for property '" + handle.prop + "'");
        }
    }

    protected Long readLong(ValueHandle handle, ReadContext context)
            throws JsonFormatException, RepositoryException, InterruptedException {
        if (handle.node.isIntegralNumber()) {
            return handle.node.getLongValue();
        } else if (handle.node.isTextual()) {
            try {
                return Long.parseLong(handle.node.getTextValue());
            } catch (NumberFormatException e) {
                throw new JsonFormatException(String.format("Unparsable long value in property '%s': %s", handle.prop,
                        handle.node.getTextValue()));
            }
        } else {
            throw new JsonFormatException("Expected long value for property '" + handle.prop + "'");
        }
    }

    protected Double readDouble(ValueHandle handle, ReadContext context)
            throws JsonFormatException, RepositoryException, InterruptedException {
        if (handle.node.isNumber()) {
            return handle.node.getDoubleValue();
        } else if (handle.node.isTextual()) {
            try {
                return Double.parseDouble(handle.node.getTextValue());
            } catch (NumberFormatException e) {
                throw new JsonFormatException(String.format("Unparsable double value in property '%s': %s", handle.prop,
                        handle.node.getTextValue()));
            }
        } else {
            throw new JsonFormatException("Expected double value for property '" + handle.prop + "'");
        }
    }

    protected BigDecimal readDecimal(ValueHandle handle, ReadContext context)
            throws JsonFormatException, RepositoryException, InterruptedException {
        if (handle.node.isNumber()) {
            return handle.node.getDecimalValue();
        } else if (handle.node.isTextual()) {
            try {
                return new BigDecimal(handle.node.getTextValue());
            } catch (NumberFormatException e) {
                throw new JsonFormatException(String.format("Unparsable decimal value in property '%s': %s", handle.prop,
                        handle.node.getTextValue()));
            }
        } else {
            throw new JsonFormatException("Expected decimal value for property '" + handle.prop + "'");
        }
    }

    protected URI readUri(ValueHandle handle, ReadContext context)
            throws JsonFormatException, RepositoryException, InterruptedException {
        if (!handle.node.isTextual()) {
            throw new JsonFormatException("Expected URI (string) value for property '" + handle.prop + "'");
        }

        try {
            return new URI(handle.node.getTextValue());
        } catch (URISyntaxException e) {
            throw new JsonFormatException("Invalid URI in property '" + handle.prop + "': "
                    + handle.node.getTextValue());
        }
    }

    protected Boolean readBoolean(ValueHandle handle, ReadContext context)
            throws JsonFormatException, RepositoryException, InterruptedException {
        if (handle.node.isBoolean()) {
            return handle.node.getBooleanValue();
        } else if (handle.node.isTextual()) {
            String text = handle.node.getTextValue();
            // I think being strict in what to accept is more user friendly, rather than considering everything
            // that is not recognized to be false
            if (text.equalsIgnoreCase("true") || text.equalsIgnoreCase("t")) {
                return Boolean.TRUE;
            } else if (text.equalsIgnoreCase("false") || text.equalsIgnoreCase("f")) {
                return Boolean.FALSE;
            } else {
                throw new JsonFormatException(String.format("Unparsable boolean value in property '%s': %s", handle.prop,
                        handle.node.getTextValue()));
            }
        } else {
            throw new JsonFormatException("Expected boolean value for property '" + handle.prop + "'");
        }
    }

    protected Link readLink(ValueHandle handle, ReadContext context)
            throws JsonFormatException, RepositoryException, InterruptedException {
        if (!handle.node.isTextual()) {
            throw new JsonFormatException("Expected text value for property '" + handle.prop + "'");
        }

        return context.linkTransformer.transform(handle.node.getTextValue(), context.repository);
    }

    protected LocalDate readDate(ValueHandle handle, ReadContext context)
            throws JsonFormatException, RepositoryException, InterruptedException {
        if (!handle.node.isTextual()) {
            throw new JsonFormatException("Expected text value for property '" + handle.prop + "'");
        }

        return new LocalDate(handle.node.getTextValue());
    }

    protected DateTime readDateTime(ValueHandle handle, ReadContext context)
            throws JsonFormatException, RepositoryException, InterruptedException {
        if (!handle.node.isTextual()) {
            throw new JsonFormatException("Expected text value for property '" + handle.prop + "'");
        }

        return new DateTime(handle.node.getTextValue());
    }

    protected Blob readBlob(ValueHandle handle, ReadContext context)
            throws JsonFormatException, RepositoryException, InterruptedException {
        if (!handle.node.isObject()) {
            throw new JsonFormatException("Expected object value for property '" + handle.prop + "'");
        }

        ObjectNode blobNode = (ObjectNode)handle.node;
        return BlobConverter.fromJson(blobNode);
    }

    protected ByteArray readByteArray(ValueHandle handle, ReadContext context)
            throws JsonFormatException, RepositoryException, InterruptedException {
        if (!handle.node.isTextual()) {
            throw new JsonFormatException("Expected base64 encoded value for property '" + handle.prop + "'");
        }
        try {
            return new ByteArray(handle.node.getBinaryValue());
        } catch (IOException e) {
            throw new JsonFormatException("Could not read base64 value for property '" + handle.prop + "'", e);
        }
    }

    /**
     * Reads/parses the JSON serialization of a value following a Lily {@link ValueType}. While typically this
     * will be the value of a Lily field in a record, such values might also occur in other places (e.g. a
     * scan filter or a mutation condition) and this method can also be called from there.
     */
    public Object readValue(ValueHandle handle, ReadContext context)
            throws JsonFormatException, RepositoryException, InterruptedException {

        String name = handle.valueType.getBaseName();

        if (name.equals("LIST")) {
            return readList(handle, context);
        } else if (name.equals("PATH")) {
            return readPath(handle, context);
        } else if (name.equals("STRING")) {
            return readString(handle, context);
        } else if (name.equals("INTEGER")) {
            return readInteger(handle, context);
        } else if (name.equals("LONG")) {
            return readLong(handle, context);
        } else if (name.equals("DOUBLE")) {
            return readDouble(handle, context);
        } else if (name.equals("DECIMAL")) {
            return readDecimal(handle, context);
        } else if (name.equals("URI")) {
            return readUri(handle, context);
        } else if (name.equals("BOOLEAN")) {
            return readBoolean(handle, context);
        } else if (name.equals("LINK")) {
            return readLink(handle, context);
        } else if (name.equals("DATE")) {
            return readDate(handle, context);
        } else if (name.equals("DATETIME")) {
            return readDateTime(handle, context);
        } else if (name.equals("BLOB")) {
            return readBlob(handle, context);
        } else if (name.equals("BYTEARRAY")) {
            return readByteArray(handle, context);
        } else if (name.equals("RECORD")) {
            return readNestedRecord(handle, context);
        } else {
            throw new JsonFormatException("Value type not supported: " + name);
        }
    }

    /**
     * Information on a value to parse: the JSON node containing the value, the property in which it occurs,
     * and its Lily ValueType.
     */
    public static class ValueHandle {
        /** Node representing the value to parse. */
        JsonNode node;
        /** JSON property name in which the value occurs, used in error messages. */
        String prop;
        /** Lily value type of the value to parse. */
        ValueType valueType;

        public ValueHandle(JsonNode node, String prop, ValueType valueType) {
            this.node = node;
            this.prop = prop;
            this.valueType = valueType;
        }
    }

    /**
     * Global context accessible while parsing values.
     */
    public static class ReadContext {
        LRepository repository;
        LinkTransformer linkTransformer;
        Namespaces namespaces;
        
        public ReadContext(LRepository repository, Namespaces namespaces, LinkTransformer linkTransformer) {
            this.repository = repository;
            this.namespaces = namespaces;
            this.linkTransformer = linkTransformer;
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

                if (type.equals("binary")) {
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
                } else if (type.equals("datetime")) {
                    JsonNode datetimeValue = value.get("value");
                    if (!datetimeValue.isTextual()) {
                        throw new JsonFormatException("Invalid datetime value for metadata field '"
                                + name + "' of record field " + recordField);
                    }

                    try {
                        builder.value(name, ISODateTimeFormat.dateTime().parseDateTime(datetimeValue.getTextValue()));
                    } catch (Exception e) {
                        throw new JsonFormatException("Invalid datetime value for metadata field '"
                                + name + "' of record field " + recordField);
                    }
                } else {
                    throw new JsonFormatException("Unsupported type value '" + type + "' for metadata field '"
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
