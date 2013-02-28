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
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.RepositoryManager;
import org.lilyproject.repository.api.filter.RecordFilter;
import org.lilyproject.repository.api.filter.RecordTypeFilter;
import org.lilyproject.tools.import_.json.JsonFormatException;
import org.lilyproject.tools.import_.json.Namespaces;
import org.lilyproject.tools.import_.json.QNameConverter;
import org.lilyproject.util.json.JsonFormat;
import org.lilyproject.util.json.JsonUtil;

public class RecordTypeFilterJson implements RecordFilterJsonConverter<RecordTypeFilter> {
    @Override
    public boolean supports(String typeName) {
        return typeName.equals(RecordTypeFilter.class.getName());
    }

    @Override
    public ObjectNode toJson(RecordTypeFilter filter, Namespaces namespaces,
            RepositoryManager repositoryManager, RecordFilterJsonConverter<RecordFilter> converter)
            throws RepositoryException, InterruptedException {

        ObjectNode node = JsonFormat.OBJECT_MAPPER.createObjectNode();
        
        if (filter.getRecordType() != null) {
            node.put("recordType", QNameConverter.toJson(filter.getRecordType(), namespaces));
        }
        
        if (filter.getVersion() != null) {
            node.put("version", filter.getVersion());
        }

        if (filter.getOperator() != null) {
            node.put("operator", filter.getOperator().toString());
        }
        
        return node;
    }

    @Override
    public RecordTypeFilter fromJson(JsonNode node, Namespaces namespaces, RepositoryManager repositoryManager,
            RecordFilterJsonConverter<RecordFilter> converter)
            throws JsonFormatException, RepositoryException, InterruptedException {
        
        RecordTypeFilter filter = new RecordTypeFilter();

        String recordType = JsonUtil.getString(node, "recordType", null);
        if (recordType != null) {
            filter.setRecordType(QNameConverter.fromJson(recordType, namespaces));
        }
        
        Long version = JsonUtil.getLong(node, "version", null);
        if (version != null) {
            filter.setVersion(version);
        }

        String operator = JsonUtil.getString(node, "operator", null);
        if (operator != null) {
            filter.setOperator(RecordTypeFilter.Operator.valueOf(operator));
        }

        return filter;
    }
}
