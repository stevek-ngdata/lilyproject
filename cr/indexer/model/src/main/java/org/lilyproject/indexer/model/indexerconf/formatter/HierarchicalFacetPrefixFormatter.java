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
package org.lilyproject.indexer.model.indexerconf.formatter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.common.base.Splitter;
import org.lilyproject.indexer.model.indexerconf.Formatter;
import org.lilyproject.indexer.model.indexerconf.IndexValue;
import org.lilyproject.repository.api.RepositoryManager;
import org.lilyproject.repository.api.ValueType;

/**
 * Formatter which formats values suitable for hierarchical faceting in Solr.
 *
 * <p>More information on hierarchical faceting in Solr can be found at
 * https://wiki.apache.org/solr/HierarchicalFaceting</p>
 *
 * <p>To take the example for the Solr wiki page, the following hierarchies:</p>
 *
 * <pre>
 * Doc#1: NonFic > Law
 * Doc#2: NonFic > Sci
 * Doc#3: NonFic > Hist, NonFic > Sci > Phys
 * </pre>
 *
 * <p>are by this formatter translated to multiple tokens, prefixed with the
 * depth of each path:</p>
 *
 * <pre>
 * Doc#1: 0/NonFic, 1/NonFic/Law
 * Doc#2: 0/NonFic, 1/NonFic/Sci
 * Doc#3: 0/NonFic, 1/NonFic/Hist,
 *           0/NonFic, 1/NonFic/Sci, 2/NonFic/Sci/Phys
 * </pre>
 *
 * <p>So the resulting value of this formatter will (in most cases) be
 * a multi-value. The input can be single or multi-valued. The inputs
 * should be slash-separated strings.</p>
 *
 */
public class HierarchicalFacetPrefixFormatter implements Formatter {
    @Override
    public List<String> format(List<IndexValue> indexValues, RepositoryManager repositoryManager) throws InterruptedException {
        List<String> result = new ArrayList<String>();
        for (IndexValue indexValue : indexValues) {
            ValueType valueType = indexValue.fieldType.getValueType();
            if (valueType.getBaseName().equals("LIST")) {
                // The values of the first list-level are supplied as individual IndexValues
                valueType = valueType.getNestedValueType();
            }

            if (!valueType.getName().equals("STRING")) {
                throw new RuntimeException(this.getClass().getSimpleName() + " only supports string values, but got: "
                        + valueType.getName());
            }
            processPath((String)indexValue.value, result);
        }
        return result;
    }

    private void processPath(String path, Collection<String> result) {
        final Iterable<String> splitted = Splitter.on("/").omitEmptyStrings().trimResults().split(path);
        int level = 0;
        final StringBuilder entry = new StringBuilder();
        for (String split : splitted) {
            entry.append("/").append(split);
            result.add(level + entry.toString());
            level++;
        }
    }
}
