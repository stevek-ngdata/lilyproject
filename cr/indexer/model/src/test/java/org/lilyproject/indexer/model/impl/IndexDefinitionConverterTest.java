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
package org.lilyproject.indexer.model.impl;

import com.google.common.collect.Lists;
import org.junit.Test;
import org.lilyproject.indexer.model.api.IndexDefinition;

import static org.junit.Assert.assertEquals;

public class IndexDefinitionConverterTest {

    private static final byte[] EMPTY_INDEXER_CONFIG = "<?xml version=\"1.0\"?><indexer/>".getBytes();

    /**
     * Perform a serialization round-trip to JSON and back.
     */
    private IndexDefinition doJsonRoundtrip(IndexDefinition indexDefinition) {
        byte[] jsonBytes = IndexDefinitionConverter.INSTANCE.toJsonBytes(indexDefinition);
        IndexDefinitionImpl deserialized = new IndexDefinitionImpl(indexDefinition.getName());
        IndexDefinitionConverter.INSTANCE.fromJsonBytes(jsonBytes, deserialized);
        return deserialized;
    }

    private IndexDefinition createEmptyIndexDefinition() {
        IndexDefinitionImpl indexDefinition = new IndexDefinitionImpl("test_index");
        indexDefinition.setConfiguration(EMPTY_INDEXER_CONFIG);
        return indexDefinition;
    }

    @Test
    public void testJsonRoundtrip_EmptyIndexer() {
        IndexDefinition emptyIndexDefinition = createEmptyIndexDefinition();
        IndexDefinition deserialized = doJsonRoundtrip(emptyIndexDefinition);

        assertEquals(emptyIndexDefinition, deserialized);
    }

    @Test
    public void testJsonRoundtrip_WithSubscriptionStart() {
        IndexDefinition indexDefinition = createEmptyIndexDefinition();
        indexDefinition.setSubscriptionTimestamp(42L);

        IndexDefinition deserialized = doJsonRoundtrip(indexDefinition);
        assertEquals(42L, deserialized.getSubscriptionTimestamp());
        assertEquals(indexDefinition, deserialized);
    }

    @Test
    public void testJsonRoundtrip_WithDefaultBatchTables() {
        IndexDefinition indexDefinition = createEmptyIndexDefinition();
        indexDefinition.setDefaultBatchTables(Lists.newArrayList("one", "two"));

        IndexDefinition deserialized = doJsonRoundtrip(indexDefinition);
        assertEquals(Lists.newArrayList("one", "two"), deserialized.getDefaultBatchTables());
    }

    @Test
    public void testJsonRoundtrip_WithBatchTables() {
        IndexDefinition indexDefinition = createEmptyIndexDefinition();
        indexDefinition.setBatchTables(Lists.newArrayList("one", "two"));

        IndexDefinition deserialized = doJsonRoundtrip(indexDefinition);
        assertEquals(Lists.newArrayList("one", "two"), deserialized.getBatchTables());
    }



}
