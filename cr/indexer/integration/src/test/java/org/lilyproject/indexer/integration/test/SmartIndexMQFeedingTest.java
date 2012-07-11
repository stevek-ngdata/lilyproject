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
package org.lilyproject.indexer.integration.test;

import com.google.common.collect.Lists;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.Scope;

import static org.junit.Assert.assertEquals;

/**
 * Tests the functionality related to intelligent feeding of the MQ: rather than dispatching
 * each record change event to each subscription (= each indexer), the messages are only
 * put on the Q for the subscriptions that need them.
 */
public class SmartIndexMQFeedingTest extends BaseIndexMQFeedingTest{

    @BeforeClass
    public static void setupBeforeClass () throws Exception{
        BaseIndexMQFeedingTest.baseSetup();
        setupSchema();
    }

    public static void setupSchema()  throws Exception{
        //
        // Define schema
        //
        repository = repoSetup.getRepository();
        typeManager = repository.getTypeManager();

        FieldType field1 = typeManager.fieldTypeBuilder()
                .name(new QName("mqfeedtest", "field1"))
                .type("STRING")
                .scope(Scope.NON_VERSIONED)
                .createOrUpdate();

        FieldType toggle = typeManager.fieldTypeBuilder()
                .name(new QName("mqfeedtest", "toggle"))
                .type("BOOLEAN")
                .scope(Scope.NON_VERSIONED)
                .createOrUpdate();

        typeManager.recordTypeBuilder()
                .defaultNamespace("mqfeedtest")
                .name("typeA")
                .field(field1.getId(), true)
                .field(toggle.getId(), false)
                .createOrUpdate();

        typeManager.recordTypeBuilder()
                .defaultNamespace("mqfeedtest")
                .name("typeB")
                .field(field1.getId(), true)
                .field(toggle.getId(), false)
                .createOrUpdate();

        typeManager.recordTypeBuilder()
                .defaultNamespace("mqfeedtest")
                .name("typeC")
                .field(field1.getId(), true)
                .field(toggle.getId(), false)
                .createOrUpdate();

    }

    @Test
    public void testRecordTypeBasedRouting() throws Exception {
        setupTwoIndexes(Lists.newArrayList("indexerconf_typeA.xml", "indexerconf_typeB.xml"));

        TrackingIndexUpdater indexUpdaterA = indexUpdaters.get(0);
        TrackingIndexUpdater indexUpdaterB = indexUpdaters.get(1);

        MySolrClient solrClientA = solrClients.get(0);
        MySolrClient solrClientB = solrClients.get(1);

        //
        // Verifiy initial state
        //
        assertEquals(0, indexUpdaterA.events());
        assertEquals(0, indexUpdaterB.events());

        //
        // Records of type A and B should only go to their respective indexes
        //
        repository.recordBuilder()
                .defaultNamespace("mqfeedtest")
                .recordType("typeA")
                .field("field1", "value1")
                .create();

        repoSetup.processMQ();

        assertEquals(1, indexUpdaterA.events());
        assertEquals(1, solrClientA.adds());

        assertEquals(0, indexUpdaterB.events());
        assertEquals(0, solrClientB.adds());

        //
        // A record of type C should go to both indexes
        //
        repository.recordBuilder()
                .defaultNamespace("mqfeedtest")
                .recordType("typeC")
                .field("field1", "value1")
                .create();

        repoSetup.processMQ();

        assertEquals(1, indexUpdaterA.events());
        assertEquals(1, solrClientA.adds());

        assertEquals(1, indexUpdaterB.events());
        assertEquals(1, solrClientB.adds());

        //
        // Create a record and change its type
        //
        Record record = repository.recordBuilder()
                .defaultNamespace("mqfeedtest")
                .recordType("typeA")
                .field("field1", "value1")
                .create();

        repoSetup.processMQ();

        // Now event should only go to index A
        assertEquals(1, indexUpdaterA.events());
        assertEquals(1, solrClientA.adds());

        assertEquals(0, indexUpdaterB.events());
        assertEquals(0, solrClientB.adds());

        record.setRecordType(new QName("mqfeedtest", "typeB"));
        record.setField(new QName("mqfeedtest", "field1"), "value2"); // can't change only RT, also need field change
        record = repository.update(record);

        repoSetup.processMQ();

        // When changing its type, event should go to both indexes
        assertEquals(1, indexUpdaterA.events());
        assertEquals(0, solrClientA.adds());
        assertEquals(1, solrClientA.deletes());

        assertEquals(1, indexUpdaterB.events());
        assertEquals(1, solrClientB.adds());
        assertEquals(1, solrClientB.deletes()); // when record type changes, applicable vtags might change
        // so indexer first deletes existing entries

        record.setField(new QName("mqfeedtest", "field1"), "value3");
        record = repository.update(record);

        repoSetup.processMQ();

        // And now event should only go to event B
        assertEquals(0, indexUpdaterA.events());
        assertEquals(0, solrClientA.adds());

        assertEquals(1, indexUpdaterB.events());
        assertEquals(1, solrClientB.adds());
    }

    @Test
    public void testFieldBasedRouting() throws Exception {
        setupTwoIndexes(Lists.newArrayList("indexerconf_fieldvalue_true.xml", "indexerconf_fieldvalue_false.xml"));

        TrackingIndexUpdater indexUpdaterTrue = indexUpdaters.get(0);
        TrackingIndexUpdater indexUpdaterFalse = indexUpdaters.get(1);

        MySolrClient solrClientTrue = solrClients.get(0);
        MySolrClient solrClientFalse = solrClients.get(1);

        //
        // Verify initial state
        //
        repoSetup.processMQ();

        assertEquals(0, indexUpdaterTrue.events());
        assertEquals(0, solrClientTrue.adds());
        assertEquals(0, solrClientTrue.deletes());

        assertEquals(0, indexUpdaterFalse.events());
        assertEquals(0, solrClientFalse.adds());
        assertEquals(0, solrClientFalse.deletes());

        //
        // Record with toggle=true should only go to first index
        //
        repository.recordBuilder()
                .defaultNamespace("mqfeedtest")
                .recordType("typeA")
                .field("field1", "value1")
                .field("toggle", Boolean.TRUE)
                .create();

        repoSetup.processMQ();

        assertEquals(1, indexUpdaterTrue.events());
        assertEquals(1, solrClientTrue.adds());
        assertEquals(0, solrClientTrue.deletes());

        assertEquals(0, indexUpdaterFalse.events());
        assertEquals(0, solrClientFalse.adds());
        assertEquals(0, solrClientFalse.deletes());

        //
        // Record with toggle=false should only go to first index
        //
        repository.recordBuilder()
                .defaultNamespace("mqfeedtest")
                .recordType("typeA")
                .field("field1", "value1")
                .field("toggle", Boolean.FALSE)
                .create();

        repoSetup.processMQ();

        assertEquals(0, indexUpdaterTrue.events());
        assertEquals(0, solrClientTrue.adds());

        assertEquals(1, indexUpdaterFalse.events());
        assertEquals(1, solrClientFalse.adds());

        //
        // Create a record and change the value of the toggle field
        //
        Record record = repository.recordBuilder()
                .defaultNamespace("mqfeedtest")
                .recordType("typeA")
                .field("field1", "value1")
                .field("toggle", Boolean.TRUE)
                .create();

        repoSetup.processMQ();

        // Now event should only go to index A
        assertEquals(1, indexUpdaterTrue.events());
        assertEquals(1, solrClientTrue.adds());

        assertEquals(0, indexUpdaterFalse.events());
        assertEquals(0, solrClientFalse.adds());

        // Change toggle to false
        record.setField(new QName("mqfeedtest", "toggle"), Boolean.FALSE);
        record = repository.update(record);

        repoSetup.processMQ();

        // When changing the toggle field, event should go to both indexes
        assertEquals(1, indexUpdaterTrue.events());
        assertEquals(0, solrClientTrue.adds());
        assertEquals(1, solrClientTrue.deletes());

        assertEquals(1, indexUpdaterFalse.events());
        assertEquals(1, solrClientFalse.adds());
        assertEquals(0, solrClientFalse.deletes());

        //
        // Test deleting toggle field
        //
        record.delete(new QName("mqfeedtest", "toggle"), true);
        record = repository.update(record);

        repoSetup.processMQ();

        // Index 2 should get an event
        assertEquals(0, indexUpdaterTrue.events());
        assertEquals(0, solrClientTrue.adds());

        assertEquals(1, indexUpdaterFalse.events());
        assertEquals(1, solrClientFalse.deletes());

        //
        // Update record, it should go to none of the two indexes now
        //
        record.setField(new QName("mqfeedtest", "field1"), "updated value");
        record = repository.update(record);

        repoSetup.processMQ();

        assertEquals(0, indexUpdaterTrue.events());
        assertEquals(0, solrClientTrue.adds());
        assertEquals(0, indexUpdaterFalse.events());
        assertEquals(0, solrClientFalse.deletes());
    }
}
