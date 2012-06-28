package org.lilyproject.indexer.integration.test;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.tika.io.IOUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilyproject.indexer.engine.*;
import org.lilyproject.indexer.integration.IndexAwareMQFeeder;
import org.lilyproject.indexer.integration.IndexSelectionRecordUpdateHook;
import org.lilyproject.indexer.model.api.IndexDefinition;
import org.lilyproject.indexer.model.api.WriteableIndexerModel;
import org.lilyproject.indexer.model.impl.IndexerModelImpl;
import org.lilyproject.indexer.model.indexerconf.IndexerConf;
import org.lilyproject.indexer.model.indexerconf.IndexerConfBuilder;
import org.lilyproject.indexer.model.sharding.ShardSelectorException;
import org.lilyproject.indexer.model.util.IndexesInfo;
import org.lilyproject.indexer.model.util.IndexesInfoImpl;
import org.lilyproject.repository.api.*;
import org.lilyproject.repository.spi.RecordUpdateHook;
import org.lilyproject.repotestfw.RepositorySetup;
import org.lilyproject.rowlog.api.*;
import org.lilyproject.util.repo.PrematureRepository;
import org.lilyproject.util.repo.PrematureRepositoryImpl;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests the functionality related to intelligent feeding of the MQ: rather than dispatching
 * each record change event to each subscription (= each indexer), the messages are only
 * put on the Q for the subscriptions that need them.
 */
public class SmartIndexMQFeedingTest {
    private static IndexesInfo indexesInfo;
    private static Repository repository;
    private static TypeManager typeManager;
    private static MySolrClient solrClientA;
    private static MySolrClient solrClientB;

    private static TrackingIndexUpdater indexUpdaterA;
    private static TrackingIndexUpdater indexUpdaterB;


    private final static RepositorySetup repoSetup = new RepositorySetup() {
        @Override
        public RowLogMessageListener createMQFeeder(RowLog mq) {
            if (indexesInfo == null) {
                throw new RuntimeException("Expected IndexesInfo to be available at this point.");
            }
            return new IndexAwareMQFeeder(mq, getRepository(), indexesInfo);
        }
    };

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        repoSetup.setupCore();

        WriteableIndexerModel indexerModel = new IndexerModelImpl(repoSetup.getZk());
        PrematureRepository prematureRepository = new PrematureRepositoryImpl();

        indexesInfo = new IndexesInfoImpl(indexerModel, prematureRepository);
        RecordUpdateHook hook = new IndexSelectionRecordUpdateHook(indexesInfo);

        repoSetup.setRecordUpdateHooks(Collections.singletonList(hook));

        repoSetup.setupRepository(true);
        repoSetup.setupMessageQueue(false, true);

        prematureRepository.setRepository(repoSetup.getRepository());

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

        RecordType rtA = typeManager.recordTypeBuilder()
                .defaultNamespace("mqfeedtest")
                .name("typeA")
                .field(field1.getId(), true)
                .createOrUpdate();

        RecordType rtB = typeManager.recordTypeBuilder()
                .defaultNamespace("mqfeedtest")
                .name("typeB")
                .field(field1.getId(), true)
                .createOrUpdate();

        RecordType rtC = typeManager.recordTypeBuilder()
                .defaultNamespace("mqfeedtest")
                .name("typeC")
                .field(field1.getId(), true)
                .createOrUpdate();

        //
        // Define indexes
        //
        IndexDefinition indexDef = indexerModel.newIndex("indexA");
        indexDef.setConfiguration(IOUtils.toByteArray(SmartIndexMQFeedingTest.class.getResourceAsStream("indexerconf_typeA.xml")));
        indexDef.setQueueSubscriptionId("IndexUpdaterA");
        indexDef.setSolrShards(Collections.singletonMap("shard1", "http://somewhereA/"));
        indexerModel.addIndex(indexDef);

        indexDef = indexerModel.newIndex("indexB");
        indexDef.setConfiguration(IOUtils.toByteArray(SmartIndexMQFeedingTest.class.getResourceAsStream("indexerconf_typeB.xml")));
        indexDef.setQueueSubscriptionId("IndexUpdaterB");
        indexDef.setSolrShards(Collections.singletonMap("shard1", "http://somewhereB/"));
        indexerModel.addIndex(indexDef);

        // IndexesInfo will be updated asynchronously: wait for that to happen
        long now = System.currentTimeMillis();
        while (indexesInfo.getIndexInfos().size() != 2) {
            if (System.currentTimeMillis() - now > 10000) {
                fail("IndexesInfo was not updated with the two defined indexes within the expected timeout.");
            }
            Thread.sleep(100);
        }

        //
        // Make subscriptions (there is no IndexerMaster to do it for us + we want them to be in-VM)
        //
        RowLogConfigurationManager rowLogConfMgr = repoSetup.getRowLogConfManager();
        rowLogConfMgr.addSubscription("MQ", "IndexUpdaterA", RowLogSubscription.Type.VM, 1);
        rowLogConfMgr.addSubscription("MQ", "IndexUpdaterB", RowLogSubscription.Type.VM, 1);


        //
        //
        //
        MySolrShardManager solrShardManagerA = new MySolrShardManager();
        solrClientA = solrShardManagerA.getSolrClient();

        MySolrShardManager solrShardManagerB = new MySolrShardManager();
        solrClientB = solrShardManagerB.getSolrClient();

        indexUpdaterA = createIndexUpdater("IndexUpdaterA", "indexerconf_typeA.xml", solrShardManagerA);
        indexUpdaterB = createIndexUpdater("IndexUpdaterB", "indexerconf_typeB.xml", solrShardManagerB);
    }

    private static TrackingIndexUpdater createIndexUpdater(String subscriptionId, String confName,
            SolrShardManager solrShardManager) throws Exception {
        IndexerConf INDEXER_CONF = IndexerConfBuilder.build(SmartIndexMQFeedingTest.class.getResourceAsStream(confName),
                repository);

        IndexLocker indexLocker = new IndexLocker(repoSetup.getZk(), true);
        Indexer indexer = new Indexer("test", INDEXER_CONF, repository, solrShardManager, indexLocker,
                new IndexerMetrics("test"));

        IndexUpdater indexUpdater = new IndexUpdater(indexer, repository, null, indexLocker, repoSetup.getMq(),
                new IndexUpdaterMetrics("test"));

        TrackingIndexUpdater trackingIndexUpdater = new TrackingIndexUpdater(indexUpdater);

        RowLogMessageListenerMapping.INSTANCE.put(subscriptionId, trackingIndexUpdater);

        return trackingIndexUpdater;
    }

    @Test
    public void testRecordTypeBasedRouting() throws Exception {
        //
        // Verifiy initial state
        //
        assertEquals(0, indexUpdaterA.getAndResetEventCount());
        assertEquals(0, indexUpdaterB.getAndResetEventCount());

        //
        // Records of type A and B should only go to their respective indexes
        //
        repository.recordBuilder()
                .defaultNamespace("mqfeedtest")
                .recordType("typeA")
                .field("field1", "value1")
                .create();

        repoSetup.processMQ();

        assertEquals(1, indexUpdaterA.getAndResetEventCount());
        assertEquals(0, indexUpdaterB.getAndResetEventCount());

        assertEquals(1, solrClientA.getAndResetAddCount());
        assertEquals(0, solrClientB.getAndResetAddCount());

        //
        // A record of type C should go to both indexes
        //
        repository.recordBuilder()
                .defaultNamespace("mqfeedtest")
                .recordType("typeC")
                .field("field1", "value1")
                .create();

        repoSetup.processMQ();

        assertEquals(1, indexUpdaterA.getAndResetEventCount());
        assertEquals(1, indexUpdaterB.getAndResetEventCount());

        assertEquals(1, solrClientA.getAndResetAddCount());
        assertEquals(1, solrClientB.getAndResetAddCount());

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
        assertEquals(1, indexUpdaterA.getAndResetEventCount());
        assertEquals(0, indexUpdaterB.getAndResetEventCount());

        assertEquals(1, solrClientA.getAndResetAddCount());
        assertEquals(0, solrClientB.getAndResetAddCount());

        record.setRecordType(new QName("mqfeedtest", "typeB"));
        record.setField(new QName("mqfeedtest", "field1"), "value2"); // can't change only RT, also need field change
        record = repository.update(record);

        repoSetup.processMQ();

        // When changing its type, event should go to both indexes
        assertEquals(1, indexUpdaterA.getAndResetEventCount());
        assertEquals(1, indexUpdaterB.getAndResetEventCount());

        assertEquals(0, solrClientA.getAndResetAddCount());
        assertEquals(0, solrClientA.getAndResetDeleteCount()); // TODO we should have a delete here
        assertEquals(1, solrClientB.getAndResetAddCount());
        assertEquals(1, solrClientB.getAndResetDeleteCount());

        record.setField(new QName("mqfeedtest", "field1"), "value3"); // can't change only RT, also need field change
        record = repository.update(record);

        repoSetup.processMQ();

        // And now event should only go to event B
        assertEquals(0, indexUpdaterA.getAndResetEventCount());
        assertEquals(1, indexUpdaterB.getAndResetEventCount());

        assertEquals(0, solrClientA.getAndResetAddCount());
        assertEquals(1, solrClientB.getAndResetAddCount());
    }

    private static class TrackingIndexUpdater implements RowLogMessageListener {
        private final IndexUpdater delegate;
        private int eventCount = 0;

        public TrackingIndexUpdater(IndexUpdater delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean processMessage(RowLogMessage message) throws InterruptedException {
            eventCount++;
            return delegate.processMessage(message);
        }

        public int getAndResetEventCount() {
            int result = eventCount;
            eventCount = 0;
            return result;
        }
    }

    private static class MySolrShardManager implements SolrShardManager {
        private MySolrClient solrClient = new MySolrClient();

        @Override
        public SolrClient getSolrClient(RecordId recordId) throws ShardSelectorException {
            return solrClient;
        }

        public MySolrClient getSolrClient() {
            return solrClient;
        }
    }

    private static class MySolrClient implements SolrClient {
        private int addCount = 0;
        private int deleteCount = 0;

        @Override
        public String getDescription() {
            return null;
        }

        @Override
        public UpdateResponse add(SolrInputDocument doc) throws SolrClientException, InterruptedException {
            addCount++;
            return null;
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
            return null;
        }

        @Override
        public UpdateResponse commit(boolean waitFlush, boolean waitSearcher) throws SolrClientException, InterruptedException {
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

        public int getAndResetAddCount() {
            int result = addCount;
            addCount = 0;
            return result;
        }

        public int getAndResetDeleteCount() {
            int result = deleteCount;
            deleteCount = 0;
            return result;
        }
    }
}
