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
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.filter.RecordFilter;
import org.lilyproject.repository.api.filter.RecordFilterList;
import org.lilyproject.tools.import_.json.JsonFormatException;
import org.lilyproject.tools.import_.json.Namespaces;
import org.lilyproject.util.json.JsonFormat;
import org.lilyproject.util.json.JsonUtil;

public class RecordFilterListJson implements RecordFilterJsonConverter<RecordFilterList> {

    @Override
    public boolean supports(String typeName) {
        return typeName.equals(RecordFilterList.class.getName());
    }

    @Override
    public ObjectNode toJson(RecordFilterList filter, Namespaces namespaces, Repository repository,
            RecordFilterJsonConverter<RecordFilter> converter)
            throws RepositoryException, InterruptedException {

        ObjectNode node = JsonFormat.OBJECT_MAPPER.createObjectNode();
        
        if (filter.getOperator() != null) {
            node.put("operator", filter.getOperator().toString());
        }
        
        if (filter.getFilters() != null) {
            ArrayNode filters = node.putArray("filters");
            for (RecordFilter subFilter : filter.getFilters()) {
                filters.add(converter.toJson(subFilter, namespaces, repository, converter));
            }
        }
        
        return node;
    }

    @Override
    public RecordFilterList fromJson(JsonNode node, Namespaces namespaces, Repository repository,
            RecordFilterJsonConverter<RecordFilter> converter)
            throws JsonFormatException, RepositoryException, InterruptedException {
        
        RecordFilterList filter = new RecordFilterList();
        
        String operator = JsonUtil.getString(node, "operator", null);
        if (operator != null) {
            filter.setOperator(RecordFilterList.Operator.valueOf(operator));
        }
        
        ArrayNode filters = JsonUtil.getArray(node, "filters", null);
        if (filters != null) {
            for (JsonNode subFilterNode : filters) {
                if (!subFilterNode.isObject()) {
                    throw new JsonFormatException("filters should contain a json object");
                }
                ObjectNode subFilterObjectNode = (ObjectNode)subFilterNode;
                filter.addFilter(converter.fromJson(subFilterObjectNode, namespaces, repository, converter));
            }
        }
        
        return filter;
    }
}
