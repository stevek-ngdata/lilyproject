/*
 * Copyright 2010 Outerthought bvba
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
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.RepositoryManager;

public interface EntityReader<T> {
    T fromJson(JsonNode node, Namespaces namespaces, RepositoryManager repositoryManager, LinkTransformer linkTransformer)
            throws JsonFormatException, RepositoryException, InterruptedException;

    T fromJson(JsonNode node, Namespaces namespaces, RepositoryManager repositoryManager)
            throws JsonFormatException, RepositoryException, InterruptedException;

    T fromJson(JsonNode node, RepositoryManager repositoryManager)
            throws JsonFormatException, RepositoryException, InterruptedException;
}
