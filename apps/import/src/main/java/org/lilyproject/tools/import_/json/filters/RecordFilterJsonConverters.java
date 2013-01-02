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

import java.util.ServiceLoader;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.filter.RecordFilter;
import org.lilyproject.tools.import_.json.JsonFormatException;
import org.lilyproject.tools.import_.json.Namespaces;
import org.lilyproject.util.json.JsonUtil;

public class RecordFilterJsonConverters implements RecordFilterJsonConverter<RecordFilter> {
    private ServiceLoader<RecordFilterJsonConverter> filterLoader = ServiceLoader.load(RecordFilterJsonConverter.class);

    public static final RecordFilterJsonConverters INSTANCE = new RecordFilterJsonConverters();

    @Override
    public boolean supports(String typeName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ObjectNode toJson(RecordFilter filter, Namespaces namespaces, Repository repository,
                             RecordFilterJsonConverter<RecordFilter> converter)
            throws RepositoryException, InterruptedException {

        String className = filter.getClass().getName();

        for (RecordFilterJsonConverter json : filterLoader) {
            if (json.supports(className)) {
                ObjectNode node = json.toJson(filter, namespaces, repository, converter);
                node.put("@class", className);
                return node;
            }
        }

        throw new RepositoryException("No json converter available for filter type " + className);
    }

    @Override
    public RecordFilter fromJson(JsonNode node, Namespaces namespaces, Repository repository,
                                 RecordFilterJsonConverter<RecordFilter> converter)
            throws JsonFormatException, RepositoryException, InterruptedException {

        String className = JsonUtil.getString(node, "@class");

        for (RecordFilterJsonConverter json : filterLoader) {
            if (json.supports(className)) {
                return json.fromJson(node, namespaces, repository, converter);
            }
        }

        throw new RepositoryException("No json converter available for filter type " + className);
    }
}
