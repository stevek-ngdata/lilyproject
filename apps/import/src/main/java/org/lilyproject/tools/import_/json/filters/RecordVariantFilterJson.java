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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.repository.api.LRepository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.filter.RecordFilter;
import org.lilyproject.repository.api.filter.RecordVariantFilter;
import org.lilyproject.tools.import_.json.JsonFormatException;
import org.lilyproject.tools.import_.json.Namespaces;
import org.lilyproject.util.json.JsonFormat;
import org.lilyproject.util.json.JsonUtil;

public class RecordVariantFilterJson implements RecordFilterJsonConverter<RecordVariantFilter> {
    @Override
    public boolean supports(String typeName) {
        return typeName.equals(RecordVariantFilter.class.getName());
    }

    @Override
    public ObjectNode toJson(RecordVariantFilter filter, Namespaces namespaces, LRepository repository,
                             RecordFilterJsonConverter<RecordFilter> converter)
            throws RepositoryException, InterruptedException {

        ObjectNode node = JsonFormat.OBJECT_MAPPER.createObjectNode();

        if (filter.getMasterRecordId() != null) {
            node.put("recordId", filter.getMasterRecordId().toString());
            final ObjectNode variantProperties = JsonFormat.OBJECT_MAPPER.createObjectNode();
            for (Map.Entry<String, String> variantProperty : filter.getVariantProperties().entrySet()) {
                variantProperties.put(variantProperty.getKey(), variantProperty.getValue());
            }
            node.put("variantProperties", variantProperties);
        }

        return node;
    }

    @Override
    public RecordVariantFilter fromJson(JsonNode node, Namespaces namespaces, LRepository repository,
                                        RecordFilterJsonConverter<RecordFilter> converter)
            throws JsonFormatException, RepositoryException, InterruptedException {

        String recordId = JsonUtil.getString(node, "recordId", null);
        if (recordId == null) {
            throw new IllegalStateException("expected non null recordId in json input");
        }

        final ObjectNode variantPropertiesNode = JsonUtil.getObject(node, "variantProperties", null);
        if (variantPropertiesNode == null) {
            throw new IllegalStateException("expected non null variantProperties in json input");
        }

        final HashMap<String, String> variantProperties = new HashMap<String, String>();

        final Iterator<Map.Entry<String, JsonNode>> fields = variantPropertiesNode.getFields();
        while (fields.hasNext()) {
            final Map.Entry<String, JsonNode> next = fields.next();
            variantProperties.put(next.getKey(), next.getValue().getTextValue());
        }
        return new RecordVariantFilter(repository.getIdGenerator().fromString(recordId), variantProperties);
    }
}
