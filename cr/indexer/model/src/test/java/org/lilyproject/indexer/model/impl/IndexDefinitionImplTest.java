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

import org.junit.Before;
import org.junit.Test;
import org.lilyproject.indexer.model.api.IndexUpdateState;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IndexDefinitionImplTest {

    private IndexDefinitionImpl indexDefinition;

    @Before
    public void setUp() {
        indexDefinition = new IndexDefinitionImpl("test_index");
    }

    // Check that changing the update state from DO_NOT_SUBSCRIBE to
    // something else updates the subscription timestamp
    @Test
    public void testSetUpdateState_ChangeFromDoNotSubscribe() {
        indexDefinition.setUpdateState(IndexUpdateState.DO_NOT_SUBSCRIBE);

        // Sanity check
        assertEquals(0, indexDefinition.getSubscriptionTimestamp());

        long now = System.currentTimeMillis();
        indexDefinition.setUpdateState(IndexUpdateState.SUBSCRIBE_AND_LISTEN);

        assertTrue(Math.abs(indexDefinition.getSubscriptionTimestamp() - now) < 10);
    }

    // Check that changing the update state from DO_NOT_SUBSCRIBE to
    // DO_NOT_SUBSCRIBE doesn't affect the subscription timestamp
    @Test
    public void testSetUpdateState_NoChangeFromDoNotSubscribe() {
        indexDefinition.setUpdateState(IndexUpdateState.DO_NOT_SUBSCRIBE);
        indexDefinition.setSubscriptionTimestamp(42);

        indexDefinition.setUpdateState(IndexUpdateState.DO_NOT_SUBSCRIBE);
        assertEquals(42, indexDefinition.getSubscriptionTimestamp());
    }

    // Check that changing the update state from something other than
    // DO_NOT_SUBSCRIBE doesn't affect the subscription timestamp
    @Test
    public void testSetUpdateState_ChangeFromSubscribeAndListen() {
        indexDefinition.setUpdateState(IndexUpdateState.SUBSCRIBE_DO_NOT_LISTEN);
        indexDefinition.setSubscriptionTimestamp(42);

        indexDefinition.setUpdateState(IndexUpdateState.SUBSCRIBE_AND_LISTEN);
        assertEquals(42, indexDefinition.getSubscriptionTimestamp());
    }

}
