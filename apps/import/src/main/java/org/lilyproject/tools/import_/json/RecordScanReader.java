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
package org.lilyproject.tools.import_.json;

import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.RecordScan;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.ReturnFields;
import org.lilyproject.tools.import_.json.filters.RecordFilterJsonConverters;
import org.lilyproject.util.json.JsonUtil;

public class RecordScanReader implements EntityReader<RecordScan> {
    public static final RecordScanReader INSTANCE = new RecordScanReader();

    @Override
    public RecordScan fromJson(JsonNode node, Repository repository)
            throws JsonFormatException, RepositoryException, InterruptedException {
        return fromJson(node, null, repository);
    }

    @Override
    public RecordScan fromJson(JsonNode nodeNode, Namespaces namespaces, Repository repository)
            throws JsonFormatException, RepositoryException, InterruptedException {

        if (!nodeNode.isObject()) {
            throw new JsonFormatException("Expected a json object for record scan, got: " +
                    nodeNode.getClass().getName());
        }

        ObjectNode node = (ObjectNode) nodeNode;

        namespaces = NamespacesConverter.fromContextJson(node, namespaces);

        RecordScan scan = new RecordScan();

        String startRecordId = JsonUtil.getString(node, "startRecordId", null);
        if (startRecordId != null) {
            scan.setStartRecordId(repository.getIdGenerator().fromString(startRecordId));
        }

        String stopRecordId = JsonUtil.getString(node, "stopRecordId", null);
        if (stopRecordId != null) {
            scan.setStopRecordId(repository.getIdGenerator().fromString(stopRecordId));
        }

        byte[] rawStartRecordId = JsonUtil.getBinary(node, "rawStartRecordId", null);
        if (rawStartRecordId != null) {
            scan.setRawStartRecordId(rawStartRecordId);
        }

        byte[] rawStopRecordId = JsonUtil.getBinary(node, "rawStopRecordId", null);
        if (rawStopRecordId != null) {
            scan.setRawStopRecordId(rawStopRecordId);
        }

        ObjectNode filter = JsonUtil.getObject(node, "recordFilter", null);
        if (filter != null) {
            scan.setRecordFilter(RecordFilterJsonConverters.INSTANCE.fromJson(filter, namespaces, repository,
                    RecordFilterJsonConverters.INSTANCE));
        }

        ObjectNode returnFieldsNode = JsonUtil.getObject(node, "returnFields", null);
        if (returnFieldsNode != null) {
            ReturnFields returnFields = new ReturnFields();
            returnFields.setType(ReturnFields.Type.valueOf(JsonUtil.getString(returnFieldsNode, "type")));

            ArrayNode fieldsArray = JsonUtil.getArray(returnFieldsNode, "fields", null);
            if (fieldsArray != null) {
                List<QName> fields = new ArrayList<QName>();
                for (JsonNode subFilterNode : fieldsArray) {
                    if (!subFilterNode.isTextual()) {
                        throw new JsonFormatException("ReturnFields.fields should be a string array, found: "
                                + subFilterNode.getClass().getName());
                    }
                    fields.add(QNameConverter.fromJson(subFilterNode.getTextValue(), namespaces));
                }
                returnFields.setFields(fields);
            }

            scan.setReturnFields(returnFields);
        }

        scan.setCaching(JsonUtil.getInt(node, "caching", scan.getCaching()));

        scan.setCacheBlocks(JsonUtil.getBoolean(node, "cacheBlocks", scan.getCacheBlocks()));

        scan.setReturnsIdRecords(JsonUtil.getBoolean(node, "returnsIdRecords", scan.isReturnsIdRecords()));

        return scan;
    }
}
