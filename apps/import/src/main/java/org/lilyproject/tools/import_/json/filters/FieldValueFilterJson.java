/*
 * Copyright 2012 NGDATA nv
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
package org.lilyproject.tools.import_.json.filters;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.repository.api.CompareOp;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.ValueType;
import org.lilyproject.repository.api.filter.FieldValueFilter;
import org.lilyproject.repository.api.filter.RecordFilter;
import org.lilyproject.tools.import_.json.DefaultLinkTransformer;
import org.lilyproject.tools.import_.json.JsonFormatException;
import org.lilyproject.tools.import_.json.LinkTransformer;
import org.lilyproject.tools.import_.json.Namespaces;
import org.lilyproject.tools.import_.json.NamespacesImpl;
import org.lilyproject.tools.import_.json.QNameConverter;
import org.lilyproject.tools.import_.json.RecordReader;
import org.lilyproject.tools.import_.json.RecordWriter;
import org.lilyproject.tools.import_.json.WriteOptions;
import org.lilyproject.util.json.JsonFormat;
import org.lilyproject.util.json.JsonUtil;

public class FieldValueFilterJson implements RecordFilterJsonConverter<FieldValueFilter> {
    private final LinkTransformer defaultLinkTransformer = new DefaultLinkTransformer();

    @Override
    public boolean supports(String typeName) {
        return typeName.equals(FieldValueFilter.class.getName());
    }

    @Override
    public FieldValueFilter fromJson(JsonNode node, Namespaces namespaces, Repository repository,
                                     RecordFilterJsonConverter<RecordFilter> converter)
            throws JsonFormatException, RepositoryException, InterruptedException {
        FieldValueFilter filter = new FieldValueFilter();

        String field = JsonUtil.getString(node, "field", null);
        JsonNode fieldValue = node.get("fieldValue");

        // field and fieldValue should be specified both, or not at all, for deserialization to work
        if ((field != null || fieldValue != null) && (field == null || fieldValue == null)) {
            throw new RuntimeException("FieldValueFilter deserialization: both field and fieldValue must be specified.");
        }

        if (field != null && fieldValue != null) {
            QName fieldQName = QNameConverter.fromJson(field, namespaces);
            filter.setField(fieldQName);
            ValueType valueType = repository.getTypeManager().getFieldTypeByName(fieldQName).getValueType();
            Object value = RecordReader.INSTANCE.readValue(fieldValue, valueType, "fieldValue", new NamespacesImpl(), repository, defaultLinkTransformer);
            filter.setFieldValue(value);
        }

        String compareOp = JsonUtil.getString(node, "compareOp", null);
        if (compareOp != null) {
            filter.setCompareOp(CompareOp.valueOf(compareOp));
        }


        filter.setFilterIfMissing(JsonUtil.getBoolean(node, "filterIfMissing", filter.getFilterIfMissing()));

        return filter;
    }

    @Override
    public ObjectNode toJson(FieldValueFilter filter, Namespaces namespaces, Repository repository,
                             RecordFilterJsonConverter<RecordFilter> converter)
            throws RepositoryException, InterruptedException {
        ObjectNode node = JsonFormat.OBJECT_MAPPER.createObjectNode();

        // field and fieldValue should be specified both, or not at all, for serialization to work
        if ((filter.getField() != null || filter.getFieldValue() != null) &&
                (filter.getField() == null || filter.getFieldValue() == null)) {
            throw new RuntimeException("Both field and fieldValue must be specified.");
        }

        if (filter.getField() != null && filter.getFieldValue() != null) {
            node.put("field", QNameConverter.toJson(filter.getField(), namespaces));

            ValueType valueType = repository.getTypeManager().getFieldTypeByName(filter.getField()).getValueType();
            JsonNode valueAsJson = RecordWriter.INSTANCE.valueToJson(filter.getFieldValue(), valueType,
                    new WriteOptions(), namespaces, repository);

            node.put("fieldValue", valueAsJson);
            ObjectNode valueNode = node.putObject("value");
        }

        if (filter.getCompareOp() != null) {
            node.put("compareOp", filter.getCompareOp().toString());
        }

        node.put("filterIfMissing", filter.getFilterIfMissing());

        return node;
    }
}
