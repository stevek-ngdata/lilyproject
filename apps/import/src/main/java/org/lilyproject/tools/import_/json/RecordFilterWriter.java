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

import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.filter.RecordFilter;
import org.lilyproject.tools.import_.json.filters.RecordFilterJsonConverters;

public class RecordFilterWriter implements EntityWriter<RecordFilter> {

    @Override
    public ObjectNode toJson(RecordFilter entity, WriteOptions options, Repository repository)
            throws RepositoryException, InterruptedException {
        Namespaces namespaces = new NamespacesImpl();

        ObjectNode node = toJson(entity, options, namespaces, repository);

        node.put("namespaces", NamespacesConverter.toJson(namespaces));

        return node;
    }

    @Override
    public ObjectNode toJson(RecordFilter filter, WriteOptions options, Namespaces namespaces, Repository repository)
            throws RepositoryException, InterruptedException {

        return RecordFilterJsonConverters.INSTANCE.toJson(filter, namespaces, repository,
                RecordFilterJsonConverters.INSTANCE);
    }

}
