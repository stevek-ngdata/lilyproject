package org.lilyproject.tools.import_.json.filters;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.bytes.api.ByteArray;
import org.lilyproject.repository.api.*;
import org.lilyproject.repository.api.filter.FieldValueFilter;
import org.lilyproject.repository.api.filter.RecordFilter;
import org.lilyproject.tools.import_.json.*;
import org.lilyproject.util.json.JsonFormat;
import org.lilyproject.util.json.JsonUtil;

import java.math.BigDecimal;
import java.net.URI;
import java.util.List;

public class FieldValueFilterJson implements RecordFilterJsonConverter<FieldValueFilter> {
    @Override
    public boolean supports(String typeName) {
        return typeName.equals(FieldValueFilter.class.getName());
    }

    public FieldValueFilter fromJson(JsonNode node, Namespaces namespaces, Repository repository,
            RecordFilterJsonConverter<RecordFilter> converter)
            throws JsonFormatException, RepositoryException, InterruptedException {
        FieldValueFilter filter = new FieldValueFilter();

        String field = JsonUtil.getString(node, "field", null);
        ObjectNode fieldValue = JsonUtil.getObject(node, "fieldValue", null);

        if (field != null) {
            filter.setField(QNameConverter.fromJson(field, namespaces));
        }

        if (fieldValue != null) {
            String valueTypeName = JsonUtil.getString(fieldValue, "valueType");
            JsonNode valueNode = JsonUtil.getNode(fieldValue, "value");
            ValueType valueType = repository.getTypeManager().getValueType(valueTypeName);
            Object value = RecordReader.INSTANCE.readValue(valueNode, valueType, "value", new NamespacesImpl(), repository);
            filter.setFieldValue(value);
        }

        String compareOp = JsonUtil.getString(node, "compareOp", null);
        if (compareOp != null) {
            filter.setCompareOp(CompareOp.valueOf(compareOp));
        }
        

        filter.setFilterIfMissing(JsonUtil.getBoolean(node, "filterIfMissing", filter.getFilterIfMissing()));
        
        return filter;
    }
    
    public ObjectNode toJson(FieldValueFilter filter, Namespaces namespaces, Repository repository,
            RecordFilterJsonConverter<RecordFilter> converter)
            throws RepositoryException, InterruptedException {
        ObjectNode node = JsonFormat.OBJECT_MAPPER.createObjectNode();
        
        if (filter.getField() != null) {
            node.put("field", QNameConverter.toJson(filter.getField(), namespaces));
        }

        if (filter.getCompareOp() != null) {
            node.put("compareOp", filter.getCompareOp().toString());
        }
        
        if (filter.getFieldValue() != null) {
            node.put("fieldValue", fieldValueToJson(filter.getFieldValue(), repository));
            ObjectNode valueNode = node.putObject("value");
        }

        node.put("filterIfMissing", filter.getFilterIfMissing());        
        
        return node;
    }

    // This code could move to a separate class when more generally useful
    private static ObjectNode fieldValueToJson(Object value, Repository repository) throws RepositoryException, InterruptedException {
        String valueTypeName = determineValueType(value);
        ValueType valueType = repository.getTypeManager().getValueType(valueTypeName);
        WriteOptions options = new WriteOptions();
        Namespaces namespaces = new NamespacesImpl(false);

        JsonNode node = RecordWriter.INSTANCE.valueToJson(value, valueType, options, namespaces, repository);

        ObjectNode valueNode = JsonFormat.OBJECT_MAPPER.createObjectNode();
        valueNode.put("valueType", valueTypeName);
        valueNode.put("value", node);

        return valueNode;
    }

    /**
     * Generates a Lily value type string for a given value. For LIST-type values, it assumes
     * the first value in the list is representative for all the values in the list, it is not
     * validated that all list entries are of the same type (this would be costly, it is
     * assumed this is check when serializing or de-serializing).
     */
    private static String determineValueType(Object value) {
        String typeName;

        if (value instanceof List) {
            List list = (List)value;
            if (list.size() == 0) {
                // the type doesn't matter, but is obliged, so just use string
                return "LIST<STRING>";
            } else {
                typeName = "LIST<" + determineValueType(list.get(0)) + ">";
            }
        } else if (value instanceof HierarchyPath) {
            HierarchyPath path = (HierarchyPath)value;
            if (path.size() == 0) {
                // the type doesn't matter, but is obliged, so just use string
                return "PATH<STRING>";
            } else {
                typeName = "PATH<" + determineValueType(path.get(0)) + ">";
            }
        } else if (value instanceof String) {
            typeName = "STRING";
        } else if (value instanceof Integer) {
            typeName = "INTEGER";
        } else if (value instanceof Long) {
            typeName = "LONG";
        } else if (value instanceof Double) {
            typeName = "DOUBLE";
        } else if (value instanceof BigDecimal) {
            typeName = "DECIMAL";
        } else if (value instanceof Boolean) {
            typeName = "BOOLEAN";
        } else if (value instanceof org.joda.time.LocalDate) {
            typeName = "DATE";
        } else if (value instanceof org.joda.time.DateTime) {
            typeName = "DATETIME";
        } else if (value instanceof Blob) {
            typeName = "BLOB";
        } else if (value instanceof Link) {
            typeName = "LINK";
        } else if (value instanceof URI) {
            typeName = "URI";
        } else if (value instanceof Record) {
            typeName = "RECORD";
        } else if (value instanceof ByteArray) {
            typeName = "BYTEARRAY";
        } else {
            throw new RuntimeException("This type of object is not supported by the JSON field value serialization: " +
                    value.getClass().getName());
        }

        return typeName;
    }
}
