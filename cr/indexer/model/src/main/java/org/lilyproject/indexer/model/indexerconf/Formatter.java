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
package org.lilyproject.indexer.model.indexerconf;

import java.util.List;

import org.lilyproject.repository.api.RepositoryManager;

/**
 * Formats field values to string for transfer to Solr.
 */
public interface Formatter {
    /**
     * Formats the given values.
     *
     * <p>All the supplied values are of the same type, but not necessarily from the
     * same field:
     *
     * <ul>
     *     <li>In case of a simple LIST-type field, each value of the field will be
     *     a separate IndexValue in the supplied list.
     *     <li>In case of nested lists, such as a LIST&lt;LIST&lt;LIST&lt;STRING>>>,
     *     the first list-level will be converted to IndexValues in the supplied list,
     *     the value of each IndexValue is then a List&lt;List&lt;String>>.
     *     <li>In case of a LIST&lt;LINK>-type field on which a dereference (=>)
     *     is performed to a single-valued field, each IndexValue will be the value
     *     of that single-valued field from a different record.
     *     <li>In case of a LIST&lt;LINK>-type field (with M values) dereferencing
     *     towards a LIST field (with N values), there will be IndexValues for
     *     each value of the dereferenced field, that is MxN values.
     *     <li>...
     * </ul>
     *
     * <p>The above is just to explain why the first list level is treated differently
     * (i.e., supplied as IndexValue objects) then nested lists.
     *
     * <p>It is not required that the items in the returned list correspond to
     * those in the input list: they can be more or less items or they can be
     * in a different order.
     */
    List<String> format(List<IndexValue> indexValues, RepositoryManager repositoryManager)
            throws InterruptedException;
}
