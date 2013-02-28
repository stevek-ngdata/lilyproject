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

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.RepositoryManager;
import org.lilyproject.repository.api.filter.RecordFilter;
import org.lilyproject.tools.import_.json.filters.RecordFilterJsonConverters;

public class RecordFilterReader implements EntityReader<RecordFilter> {
    public static final RecordScanReader INSTANCE = new RecordScanReader();

    @Override
    public RecordFilter fromJson(JsonNode node, RepositoryManager repositoryManager)
            throws JsonFormatException, RepositoryException, InterruptedException {
        return fromJson(node, null, repositoryManager);
    }

    @Override
    public RecordFilter fromJson(JsonNode nodeNode, Namespaces namespaces, RepositoryManager repositoryManager)
            throws JsonFormatException, RepositoryException, InterruptedException {

        if (!nodeNode.isObject()) {
            throw new JsonFormatException("Expected a json object for record filter, got: " +
                    nodeNode.getClass().getName());
        }

        ObjectNode node = (ObjectNode)nodeNode;

        namespaces = NamespacesConverter.fromContextJson(node, namespaces);

        return RecordFilterJsonConverters.INSTANCE.fromJson(node, namespaces, repositoryManager,
                RecordFilterJsonConverters.INSTANCE);
    }

    @Override
    public RecordFilter fromJson(JsonNode node, Namespaces namespaces, RepositoryManager repositoryManager,
            LinkTransformer linkTransformer) throws JsonFormatException, RepositoryException, InterruptedException {
        return fromJson(node, namespaces, repositoryManager);
    }

}
