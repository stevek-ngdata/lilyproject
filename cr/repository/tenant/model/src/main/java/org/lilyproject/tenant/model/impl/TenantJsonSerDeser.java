/*
 * Copyright 2013 NGDATA nv
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
package org.lilyproject.tenant.model.impl;

import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.tenant.model.api.Tenant;
import org.lilyproject.util.json.JsonFormat;
import org.lilyproject.util.json.JsonUtil;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.lilyproject.tenant.model.api.Tenant.TenantLifecycleState;

public class TenantJsonSerDeser {
    public static TenantJsonSerDeser INSTANCE = new TenantJsonSerDeser();

    private TenantJsonSerDeser() {

    }

    public Tenant fromJsonBytes(String name, byte[] json) {
        ObjectNode node;
        try {
            node = (ObjectNode)JsonFormat.deserialize(new ByteArrayInputStream(json));
        } catch (IOException e) {
            throw new RuntimeException("Error parsing tenant definition JSON.", e);
        }
        return fromJson(name, node);
    }

    public Tenant fromJson(String name, ObjectNode node) {
        TenantLifecycleState lifecycleState = JsonUtil.getEnum(node, "lifecycleState", TenantLifecycleState.class);
        return new Tenant(name, lifecycleState);
    }

    public ObjectNode toJson(Tenant tenant) {
        ObjectNode node = JsonNodeFactory.instance.objectNode();
        node.put("lifecycleState", tenant.getLifecycleState().toString());
        return node;
    }

    byte[] toJsonBytes(Tenant tenant) {
        try {
            return JsonFormat.serializeAsBytes(toJson(tenant));
        } catch (IOException e) {
            throw new RuntimeException("Error serializing tenant definition to JSON.", e);
        }
    }
}
