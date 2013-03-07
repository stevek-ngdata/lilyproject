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

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;

import com.google.common.collect.Maps;
import org.junit.Before;
import org.junit.Test;
import org.lilyproject.indexer.model.api.IndexDefinition;
import org.lilyproject.indexer.model.api.IndexUpdateState;
import org.lilyproject.util.zookeeper.ZooKeeperItf;
import org.lilyproject.util.zookeeper.ZooKeeperOperation;
import org.mockito.ArgumentCaptor;

public class IndexerModelImplTest {

    private ZooKeeperItf zk;
    private IndexerModelImpl indexerModel;

    @Before
    public void setUp() throws Exception {
        zk = mock(ZooKeeperItf.class);
        when(zk.retryOperation(any(ZooKeeperOperation.class))).thenReturn(true);
        indexerModel = new IndexerModelImpl(zk);
    }

    /** Creates a Mockito-spyed basic IndexDefinition. */
    private IndexDefinition createSpyIndexDefinition() {
        IndexDefinition indexDefinition = spy(new IndexDefinitionImpl("test_index"));
        indexDefinition.setConfiguration("<?xml version=\"1.0\"?><indexer/>".getBytes());
        Map<String,String> solrShardMap = Maps.newHashMap();
        solrShardMap.put("shard1", "http://shard.somewhere.com/solr");
        indexDefinition.setSolrShards(solrShardMap);
        return indexDefinition;
    }

    // Check that the current timestamp is set on an index definition when it is added
    @Test
    public void testAddIndex() throws Exception {
        IndexDefinition indexDefinition = createSpyIndexDefinition();

        indexerModel.addIndex(indexDefinition);

        ArgumentCaptor<Long> timestampCaptor = ArgumentCaptor.forClass(Long.class);
        verify(indexDefinition).setSubscriptionTimestamp(timestampCaptor.capture());
        assertTrue(Math.abs(timestampCaptor.getValue() - System.currentTimeMillis()) < 1000);
    }

    // Check that the current timestamp is not set on an index definition when it is added
    // if the update state is set to DO_NOT_SUBSCRIBE
    @Test
    public void testAddIndex_UpdateStateDoNotSubscribe() throws Exception {
        IndexDefinition indexDefinition = createSpyIndexDefinition();
        indexDefinition.setUpdateState(IndexUpdateState.DO_NOT_SUBSCRIBE);

        indexerModel.addIndex(indexDefinition);
        verify(indexDefinition, never()).setSubscriptionTimestamp(anyLong());
    }

    @Test
    public void testValidateIndexName_Valid() {
        // Nothing should happen
        IndexerModelImpl.validateIndexName("valid-index-name");
    }

    @Test(expected=IllegalArgumentException.class)
    public void testValidateIndexName_Invalid_EmptyString() {
        IndexerModelImpl.validateIndexName("");
    }

    @Test(expected=IllegalArgumentException.class)
    public void testValidateIndexName_Invalid_NonPrintableCharacters() {
        IndexerModelImpl.validateIndexName("not\u0001valid");
    }

}
