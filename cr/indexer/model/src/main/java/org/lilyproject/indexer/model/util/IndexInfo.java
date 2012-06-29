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
package org.lilyproject.indexer.model.util;

import org.lilyproject.indexer.model.api.IndexDefinition;
import org.lilyproject.indexer.model.indexerconf.IndexerConf;

public class IndexInfo {
    IndexDefinition indexDefinition;
    IndexerConf indexerConf;

    public IndexInfo(IndexDefinition indexDefinition, IndexerConf indexerConf) {
        this.indexDefinition = indexDefinition;
        this.indexerConf = indexerConf;
    }

    public IndexDefinition getIndexDefinition() {
        return indexDefinition;
    }

    public IndexerConf getIndexerConf() {
        return indexerConf;
    }
}
