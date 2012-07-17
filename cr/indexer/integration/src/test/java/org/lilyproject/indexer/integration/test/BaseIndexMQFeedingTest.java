package org.lilyproject.indexer.integration.test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.tika.io.IOUtils;
import org.lilyproject.hadooptestfw.TestHelper;
import org.lilyproject.indexer.derefmap.DerefMap;
import org.lilyproject.indexer.derefmap.DerefMapHbaseImpl;
import org.lilyproject.indexer.engine.IndexLocker;
import org.lilyproject.indexer.engine.IndexUpdater;
import org.lilyproject.indexer.engine.IndexUpdaterMetrics;
import org.lilyproject.indexer.engine.Indexer;
import org.lilyproject.indexer.engine.IndexerMetrics;
import org.lilyproject.indexer.engine.SolrClient;
import org.lilyproject.indexer.engine.SolrClientException;
import org.lilyproject.indexer.engine.SolrShardManager;
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
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.Repository;
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

import static org.junit.Assert.fail;

/**
 * Tests the functionality related to intelligent feeding of the MQ: rather than dispatching
 * each record change event to each subscription (= each indexer), the messages are only
 * put on the Q for the subscriptions that need them.
 */
public abstract class BaseIndexMQFeedingTest {
    protected static WriteableIndexerModel indexerModel;
    protected static RowLogConfigurationManager rowLogConfMgr;
    protected static IndexesInfo indexesInfo;
    protected static Repository repository;
    protected static TypeManager typeManager;

    protected static List<MySolrShardManager> solrShardManagers = new ArrayList<MySolrShardManager>();
    protected static List<MySolrClient> solrClients = new ArrayList<MySolrClient>();
    protected static List<TrackingIndexUpdater> indexUpdaters = new ArrayList<TrackingIndexUpdater>();

    protected final static RepositorySetup repoSetup = new RepositorySetup() {
        @Override
        public RowLogMessageListener createMQFeeder(RowLog mq) {
            if (indexesInfo == null) {
                throw new RuntimeException("Expected IndexesInfo to be available at this point.");
            }
            return new IndexAwareMQFeeder(mq, getRepository(), indexesInfo);
        }
    };

    public static void baseSetup() throws Exception {
        TestHelper.setupLogging("org.lilyproject.indexer", "org.lilyproject.linkindex",
                "org.lilyproject.rowlog.impl.RowLogImpl");

        repoSetup.setupCore();

        indexerModel = new IndexerModelImpl(repoSetup.getZk());
        PrematureRepository prematureRepository = new PrematureRepositoryImpl();

        indexesInfo = new IndexesInfoImpl(indexerModel, prematureRepository);
        RecordUpdateHook hook = new IndexSelectionRecordUpdateHook(indexesInfo);

        repoSetup.setRecordUpdateHooks(Collections.singletonList(hook));

        repoSetup.setupRepository(true);
        repoSetup.setupMessageQueue(false, true);

        prematureRepository.setRepository(repoSetup.getRepository());

        rowLogConfMgr = repoSetup.getRowLogConfManager();
    }

    protected static void setupTwoIndexes(List<String> confNames) throws Exception {
        // Remove old indexes & subscriptions, if any
        for (IndexDefinition indexDef : indexerModel.getIndexes()) {
            indexerModel.deleteIndex(indexDef.getName());
        }

        // Remove rowlog subscriptions
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
            indexDef.setConfiguration(IOUtils.toByteArray(BaseIndexMQFeedingTest.class.getResourceAsStream(confName)));
            indexDef.setQueueSubscriptionId("IndexUpdater" + i);
            indexDef.setSolrShards(Collections.singletonMap("shard1", "http://somewhere" + i + "/"));
            indexerModel.addIndex(indexDef);

            solrShardManagers.add(new MySolrShardManager());
            solrClients.add(solrShardManagers.get(i).getSolrClient());

            indexUpdaters.add(createIndexUpdater("IndexUpdater" + i, confName, solrShardManagers.get(i)));

            rowLogConfMgr.addSubscription("MQ", "IndexUpdater" + i, RowLogSubscription.Type.VM, 1);
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
            Thread.sleep(100);
        }
    }

    private static void waitForRowLog(int expectedCount) throws InterruptedException {
        RowLog mq = repoSetup.getMq();
        long now = System.currentTimeMillis();
        while (mq.getSubscriptions().size() != expectedCount) {
            if (System.currentTimeMillis() - now > 10000) {
                fail("RowLog was not updated within the expected timeout.");
            }
            Thread.sleep(100);
        }
    }

    private static TrackingIndexUpdater createIndexUpdater(String subscriptionId, String confName,
            SolrShardManager solrShardManager) throws Exception {
        IndexerConf INDEXER_CONF = IndexerConfBuilder.build(BaseIndexMQFeedingTest.class.getResourceAsStream(confName),
                repository);

        IndexLocker indexLocker = new IndexLocker(repoSetup.getZk(), true);
        DerefMap derefMap = DerefMapHbaseImpl.create("test", repoSetup.getHadoopConf(), repository.getIdGenerator());
        Indexer indexer = new Indexer("test", INDEXER_CONF, repository, solrShardManager, indexLocker,
                new IndexerMetrics("test"), derefMap);

        IndexUpdater indexUpdater = new IndexUpdater(indexer, repository, null, indexLocker, repoSetup.getMq(),
                new IndexUpdaterMetrics("test"), derefMap, subscriptionId);

        TrackingIndexUpdater trackingIndexUpdater = new TrackingIndexUpdater(indexUpdater);

        RowLogMessageListenerMapping.INSTANCE.put(subscriptionId, trackingIndexUpdater);

        return trackingIndexUpdater;
    }


    protected static class TrackingIndexUpdater implements RowLogMessageListener {
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

        public int events() {
            int result = eventCount;
            eventCount = 0;
            return result;
        }
    }

    protected static class MySolrShardManager implements SolrShardManager {
        private MySolrClient solrClient = new MySolrClient();

        @Override
        public SolrClient getSolrClient(RecordId recordId) throws ShardSelectorException {
            return solrClient;
        }

        public MySolrClient getSolrClient() {
            return solrClient;
        }
    }

    protected static class MySolrClient implements SolrClient {
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
