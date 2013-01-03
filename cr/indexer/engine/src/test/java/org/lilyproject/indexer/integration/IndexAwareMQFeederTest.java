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
package org.lilyproject.indexer.integration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.tika.io.IOUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilyproject.hadooptestfw.TestHelper;
import org.lilyproject.indexer.derefmap.DerefMap;
import org.lilyproject.indexer.engine.IndexLocker;
import org.lilyproject.indexer.engine.IndexUpdater;
import org.lilyproject.indexer.engine.IndexUpdaterMetrics;
import org.lilyproject.indexer.engine.Indexer;
import org.lilyproject.indexer.engine.IndexerMetrics;
import org.lilyproject.indexer.engine.SolrClient;
import org.lilyproject.indexer.engine.SolrClientException;
import org.lilyproject.indexer.engine.SolrShardManager;
import org.lilyproject.indexer.model.api.IndexDefinition;
import org.lilyproject.indexer.model.api.WriteableIndexerModel;
import org.lilyproject.indexer.model.impl.IndexerModelImpl;
import org.lilyproject.indexer.model.indexerconf.IndexerConf;
import org.lilyproject.indexer.model.indexerconf.IndexerConfBuilder;
import org.lilyproject.indexer.model.sharding.ShardSelectorException;
import org.lilyproject.indexer.model.util.IndexesInfo;
import org.lilyproject.indexer.model.util.IndexesInfoImpl;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.spi.RecordUpdateHook;
import org.lilyproject.repotestfw.RepositorySetup;
import org.lilyproject.rowlog.api.RowLog;
import org.lilyproject.rowlog.api.RowLogConfigurationManager;
import org.lilyproject.rowlog.api.RowLogMessage;
import org.lilyproject.rowlog.api.RowLogMessageListener;
import org.lilyproject.rowlog.api.RowLogMessageListenerMapping;
import org.lilyproject.rowlog.api.RowLogSubscription;
import org.lilyproject.util.repo.PrematureRepository;
import org.lilyproject.util.repo.PrematureRepositoryImpl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests the functionality related to intelligent feeding of the MQ: rather than dispatching
 * each record change event to each subscription (= each indexer), the messages are only
 * put on the Q for the subscriptions that need them.
 */
public class IndexAwareMQFeederTest {

    protected static WriteableIndexerModel indexerModel;
    protected static RowLogConfigurationManager rowLogConfMgr;
    protected static IndexesInfo indexesInfo;
    protected static Repository repository;
    protected static TypeManager typeManager;

    protected static List<CountingSolrShardManager> solrShardManagers = new ArrayList<CountingSolrShardManager>();
    protected static List<CountingSolrClient> solrClients = new ArrayList<CountingSolrClient>();
    protected static List<CountingIndexUpdater> indexUpdaters = new ArrayList<CountingIndexUpdater>();

    protected static final RepositorySetup repoSetup = new RepositorySetup() {
        @Override
        public RowLogMessageListener createMQFeeder(RowLog mq) {
            if (indexesInfo == null) {
                throw new RuntimeException("Expected IndexesInfo to be available at this point.");
            }
            return new IndexAwareMQFeeder(mq, getRepository(), indexesInfo);
        }
    };

    @BeforeClass
    public static void setupBeforeClass() throws Exception {
        TestHelper.setupLogging("org.lilyproject.indexer", "org.lilyproject.linkindex",
                "org.lilyproject.rowlog.impl.RowLogImpl");

        repoSetup.setupCore();

        indexerModel = new IndexerModelImpl(repoSetup.getZk());
        PrematureRepository prematureRepository = new PrematureRepositoryImpl();

        indexesInfo = new IndexesInfoImpl(indexerModel, prematureRepository);
        RecordUpdateHook hook = new IndexRecordFilterHook(indexesInfo);

        repoSetup.setRecordUpdateHooks(Collections.singletonList(hook));

        repoSetup.setupRepository(true);
        repoSetup.setupMessageQueue(false, true);

        prematureRepository.setRepository(repoSetup.getRepository());

        rowLogConfMgr = repoSetup.getRowLogConfManager();

        setupSchema();
    }

    protected static void setupIndexes(List<String> confNames) throws Exception {
        // Remove old indexes & subscriptions, if any
        for (IndexDefinition indexDef : indexerModel.getIndexes()) {
            indexerModel.deleteIndex(indexDef.getName());
        }

        // Remove rowlog subscriptions (normally responsibility of IndexerMaster, which we don't use here)
        for (RowLogSubscription subscription : rowLogConfMgr.getSubscriptions("MQ")) {
            rowLogConfMgr.removeSubscription("MQ", subscription.getId());
            RowLogMessageListenerMapping.INSTANCE.remove(subscription.getId());
        }

        solrShardManagers.clear();
        solrClients.clear();
        indexUpdaters.clear();

        waitForIndexesInfoUpdate(0);
        waitForRowLog(0);

        // Define the new indexes
        for (int i = 0; i < confNames.size(); i++) {
            String confName = confNames.get(i);
            IndexDefinition indexDef = indexerModel.newIndex("index" + i);
            indexDef.setConfiguration(IOUtils.toByteArray(IndexAwareMQFeederTest.class.getResourceAsStream(confName)));
            indexDef.setQueueSubscriptionId("IndexUpdater" + i);
            indexDef.setSolrShards(Collections.singletonMap("shard1", "http://somewhere" + i + "/"));
            indexerModel.addIndex(indexDef);

            solrShardManagers.add(new CountingSolrShardManager());
            solrClients.add(solrShardManagers.get(i).getSolrClient());

            rowLogConfMgr.addSubscription("MQ", "IndexUpdater" + i, RowLogSubscription.Type.VM, 1);

            indexUpdaters.add(createIndexUpdater("IndexUpdater" + i, confName, solrShardManagers.get(i)));
        }

        waitForIndexesInfoUpdate(confNames.size());
        waitForRowLog(confNames.size());
    }

    protected static void waitForIndexesInfoUpdate(int expectedCount) throws InterruptedException {
        // IndexesInfo will be updated asynchronously: wait for that to happen
        long now = System.currentTimeMillis();
        while (indexesInfo.getIndexInfos().size() != expectedCount) {
            if (System.currentTimeMillis() - now > 10000) {
                fail("IndexesInfo was not updated within the expected timeout.");
            }
            Thread.sleep(30);
        }
    }

    private static void waitForRowLog(int expectedCount) throws InterruptedException {
        RowLog mq = repoSetup.getMq();
        long now = System.currentTimeMillis();
        while (mq.getSubscriptions().size() != expectedCount) {
            if (System.currentTimeMillis() - now > 10000) {
                fail("RowLog was not updated within the expected timeout.");
            }
            Thread.sleep(30);
        }
    }

    private static CountingIndexUpdater createIndexUpdater(String subscriptionId, String confName,
                                                           SolrShardManager solrShardManager) throws Exception {
        IndexerConf INDEXER_CONF = IndexerConfBuilder.build(IndexAwareMQFeederTest.class.getResourceAsStream(confName),
                repository);

        IndexLocker indexLocker = new IndexLocker(repoSetup.getZk(), false);
        DerefMap derefMap = null;
        Indexer indexer = new Indexer("test", INDEXER_CONF, repository, solrShardManager, indexLocker,
                new IndexerMetrics("test"), derefMap);

        IndexUpdater indexUpdater = new IndexUpdater(indexer, repository, indexLocker, repoSetup.getMq(),
                new IndexUpdaterMetrics("test"), derefMap, subscriptionId);

        CountingIndexUpdater trackingIndexUpdater = new CountingIndexUpdater(indexUpdater);

        RowLogMessageListenerMapping.INSTANCE.put(subscriptionId, trackingIndexUpdater);

        return trackingIndexUpdater;
    }

    public static void setupSchema() throws Exception {
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
        setupIndexes(Lists.newArrayList("indexerconf_typeA.xml", "indexerconf_typeB.xml"));

        CountingIndexUpdater indexUpdaterA = indexUpdaters.get(0);
        CountingIndexUpdater indexUpdaterB = indexUpdaters.get(1);

        CountingSolrClient solrClientA = solrClients.get(0);
        CountingSolrClient solrClientB = solrClients.get(1);

        //
        // Verify initial state
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

        // Delete record
        repository.delete(record.getId());

        repoSetup.processMQ();

        assertEquals(0, indexUpdaterA.events());
        assertEquals(0, solrClientA.adds());
        assertEquals(1, indexUpdaterB.events());
        assertEquals(1, solrClientB.deletes());
    }

    @Test
    public void testFieldBasedRouting() throws Exception {
        setupIndexes(Lists.newArrayList("indexerconf_fieldvalue_true.xml", "indexerconf_fieldvalue_false.xml"));

        CountingIndexUpdater indexUpdaterTrue = indexUpdaters.get(0);
        CountingIndexUpdater indexUpdaterFalse = indexUpdaters.get(1);

        CountingSolrClient solrClientTrue = solrClients.get(0);
        CountingSolrClient solrClientFalse = solrClients.get(1);

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

    @Test
    public void testDisableMQFeedingAttribute() throws Exception {
        String NS = "org.lilyproject.indexer.integration.test.attr";

        repository = repoSetup.getRepository();
        typeManager = repository.getTypeManager();

        FieldType field1 = typeManager.fieldTypeBuilder()
                .name(new QName(NS, "noindex_string"))
                .type("STRING")
                .scope(Scope.NON_VERSIONED)
                .createOrUpdate();

        typeManager.recordTypeBuilder()
                .defaultNamespace(NS)
                .name("NoIndexAttribute")
                .field(field1.getId(), true)
                .createOrUpdate();

        QName stringFieldName = new QName(NS, "noindex_string");

        List<String> conf = new ArrayList<String>();
        conf.add("indexerconf_noindex_configuration.xml");
        setupIndexes(conf);

        CountingIndexUpdater indexUpdater = indexUpdaters.get(0);
        CountingSolrClient solrClient = solrClients.get(0);

        assertEquals(0, indexUpdater.events());


        Record record = repository.recordBuilder()
                .defaultNamespace(NS)
                .recordType("NoIndexAttribute")
                .field(stringFieldName, "noindex-field-test")
                .build();
        record.getAttributes().put("lily.mq", "false");
        record = repository.create(record);
        repoSetup.processMQ();

        // Assert not indexed by checking that the recordEvent didn't reach the indexer
        assertEquals(0, indexUpdaters.get(0).events());
        assertEquals(0, solrClient.adds());

        record.setField(stringFieldName, "index-field-test");
        record.setAttributes(null);
        record = repository.update(record);
        repoSetup.processMQ();

        // Assert indexed
        assertEquals(1, solrClient.adds());
        assertEquals(1, indexUpdater.events());

        assertEquals(0, indexUpdater.events());
        assertEquals(0, solrClient.adds());

        record.setField(stringFieldName, "updated-changes-noindex");
        record.getAttributes().put("lily.mq", "false");
        record = repository.update(record);
        repoSetup.processMQ();

        // Assert changes not indexed
        assertEquals(0, indexUpdater.events());
        assertEquals(0, solrClient.adds());
    }

    protected static class CountingIndexUpdater implements RowLogMessageListener {
        private final IndexUpdater delegate;
        private int eventCount = 0;

        public CountingIndexUpdater(IndexUpdater delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean processMessage(RowLogMessage message) throws InterruptedException {
            eventCount++;
            return delegate.processMessage(message);
        }

        public int events() {
            int result = eventCount;
            eventCount = 0;
            return result;
        }
    }

    protected static class CountingSolrShardManager implements SolrShardManager {
        private CountingSolrClient solrClient = new CountingSolrClient();

        @Override
        public SolrClient getSolrClient(RecordId recordId) throws ShardSelectorException {
            return solrClient;
        }

        public CountingSolrClient getSolrClient() {
            return solrClient;
        }

        @Override
        public void close() throws IOException {
            // no op
        }
    }

    protected static class CountingSolrClient implements SolrClient {
        private int addCount = 0;
        private int deleteCount = 0;

        @Override
        public String getDescription() {
            return null;
        }

        @Override
        public UpdateResponse add(SolrInputDocument doc) throws SolrClientException, InterruptedException {
            addCount++;
            return dummyResponse(); // Indexer expects a non-null response
        }

        private UpdateResponse dummyResponse() {
            UpdateResponse r = new UpdateResponse();
            r.setResponse(new NamedList<Object>());
            return r;
        }

        @Override
        public UpdateResponse add(Collection<SolrInputDocument> docs) throws SolrClientException, InterruptedException {
            return null;
        }

        @Override
        public UpdateResponse deleteById(String id) throws SolrClientException, InterruptedException {
            return null;
        }

        @Override
        public UpdateResponse deleteById(List<String> ids) throws SolrClientException, InterruptedException {
            return null;
        }

        @Override
        public UpdateResponse deleteByQuery(String query) throws SolrClientException, InterruptedException {
            deleteCount++;
            return dummyResponse(); // Indexer expects a non-null response
        }

        @Override
        public UpdateResponse commit(boolean waitFlush, boolean waitSearcher)
                throws SolrClientException, InterruptedException {
            return null;
        }

        @Override
        public UpdateResponse commit() throws SolrClientException, InterruptedException {
            return null;
        }

        @Override
        public QueryResponse query(SolrParams params) throws SolrClientException, InterruptedException {
            return null;
        }

        public int adds() {
            int result = addCount;
            addCount = 0;
            return result;
        }

        public int deletes() {
            int result = deleteCount;
            deleteCount = 0;
            return result;
        }
    }
}
