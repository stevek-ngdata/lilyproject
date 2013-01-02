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
package org.lilyproject.repository.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.util.ArgumentValidator;
import org.lilyproject.util.json.JsonFormat;
import org.lilyproject.util.json.JsonUtil;

public class BlobStoreAccessConfig {

    private String defaultAccess = null;
    private Map<String, Long> limits = new HashMap<String, Long>();

    public BlobStoreAccessConfig(byte[] encodedConfig) {
        JsonNode node = JsonFormat.deserializeSoft(encodedConfig, "BlobStoreAccessConfig");

        defaultAccess = JsonUtil.getString(node, "default");

        ArrayNode limitsNode = JsonUtil.getArray(node, "limits");
        for (int i = 0; i < limitsNode.size(); i++) {
            JsonNode limitNode = limitsNode.get(i);
            String store = JsonUtil.getString(limitNode, "store");
            long limit = JsonUtil.getLong(limitNode, "limit");
            limits.put(store, limit);
        }
    }

    public byte[] toBytes() {
        ObjectNode node = JsonNodeFactory.instance.objectNode();
        node.put("default", defaultAccess);

        ArrayNode limitsNode = node.putArray("limits");
        for (Entry<String, Long> limit : limits.entrySet()) {
            ObjectNode limitNode = limitsNode.addObject();
            limitNode.put("store", limit.getKey());
            limitNode.put("limit", limit.getValue());
        }

        return JsonFormat.serializeAsBytesSoft(node, "BlobStoreAccessConfig");
    }

    public BlobStoreAccessConfig(String defaultAccess) {
        ArgumentValidator.notNull(defaultAccess, "defaultAccess");
        this.defaultAccess = defaultAccess;
    }

    public void setDefault(String defaultAccess) {
        ArgumentValidator.notNull(defaultAccess, "defaultAccess");
        this.defaultAccess = defaultAccess;
    }

    public String getDefault() {
        return this.defaultAccess;
    }

    public void setLimit(String access, long limit) {
        limits.put(access, limit);
    }

    public Map<String, Long> getLimits() {
        return limits;
    }

}
