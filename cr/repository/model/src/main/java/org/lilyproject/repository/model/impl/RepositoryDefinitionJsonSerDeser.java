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
package org.lilyproject.repository.model.impl;

import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.repository.model.api.RepositoryDefinition;
import org.lilyproject.util.json.JsonFormat;
import org.lilyproject.util.json.JsonUtil;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class RepositoryDefinitionJsonSerDeser {
    public static RepositoryDefinitionJsonSerDeser INSTANCE = new RepositoryDefinitionJsonSerDeser();

    private RepositoryDefinitionJsonSerDeser() {

    }

    public RepositoryDefinition fromJsonBytes(String name, byte[] json) {
        ObjectNode node;
        try {
            node = (ObjectNode)JsonFormat.deserialize(new ByteArrayInputStream(json));
        } catch (IOException e) {
            throw new RuntimeException("Error parsing tenant definition JSON.", e);
        }
        return fromJson(name, node);
    }

    public RepositoryDefinition fromJson(String name, ObjectNode node) {
        RepositoryDefinition.RepositoryLifecycleState lifecycleState = JsonUtil.getEnum(node, "lifecycleState", RepositoryDefinition.RepositoryLifecycleState.class);
        return new RepositoryDefinition(name, lifecycleState);
    }

    public ObjectNode toJson(RepositoryDefinition repositoryDefinition) {
        ObjectNode node = JsonNodeFactory.instance.objectNode();
        node.put("lifecycleState", repositoryDefinition.getLifecycleState().toString());
        return node;
    }

    byte[] toJsonBytes(RepositoryDefinition repositoryDefinition) {
        try {
            return JsonFormat.serializeAsBytes(toJson(repositoryDefinition));
        } catch (IOException e) {
            throw new RuntimeException("Error serializing tenant definition to JSON.", e);
        }
    }
}
