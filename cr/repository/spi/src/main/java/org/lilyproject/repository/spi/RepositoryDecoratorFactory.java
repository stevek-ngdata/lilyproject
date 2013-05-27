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
package org.lilyproject.repository.spi;

import org.lilyproject.repository.api.Repository;

public interface RepositoryDecoratorFactory {
    /**
     * This will be called once for each repository-table couple, the first time that that repository
     * is requested.
     *
     * <p>In most cases, you will want to extend the actual decorator on {@link BaseRepositoryDecorator},
     * passing the supplied delegate repository to its constructor.</p>
     */
    Repository createInstance(Repository delegate);
}
