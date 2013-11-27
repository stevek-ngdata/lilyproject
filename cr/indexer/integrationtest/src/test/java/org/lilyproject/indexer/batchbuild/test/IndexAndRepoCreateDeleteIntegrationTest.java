package org.lilyproject.indexer.batchbuild.test;


import com.google.common.collect.Sets;
import com.ngdata.hbaseindexer.conf.IndexerConf;
import com.ngdata.hbaseindexer.conf.XmlIndexerConfReader;
import com.ngdata.hbaseindexer.conf.XmlIndexerConfWriter;
import com.ngdata.hbaseindexer.model.api.IndexerDefinition;
import com.ngdata.hbaseindexer.model.api.IndexerDefinitionBuilder;
import com.ngdata.hbaseindexer.model.api.IndexerModelEvent;
import com.ngdata.hbaseindexer.model.api.IndexerModelEventType;
import com.ngdata.hbaseindexer.model.api.IndexerModelListener;
import com.ngdata.hbaseindexer.model.api.WriteableIndexerModel;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.lilyproject.indexer.model.api.IndexUpdateException;
import org.lilyproject.lilyservertestfw.LilyProxy;
import org.lilyproject.repository.api.LRepository;
import org.lilyproject.repository.model.impl.RepositoryModelImpl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.lilyproject.indexer.batchbuild.test.IndexerIntegrationTestUtil.MINS15;
import static org.lilyproject.indexer.model.api.IndexerModelEventType.INDEX_REMOVED;

public class IndexAndRepoCreateDeleteIntegrationTest {

    private static LilyProxy lilyProxy;
    private static IndexerIntegrationTestUtil testUtil;

    private RepositoryModelImpl model;
    private HBaseAdmin hBaseAdmin;
    private WriteableIndexerModel indexerModel;

    @BeforeClass
    public static void startLily() throws Exception {
        lilyProxy = new LilyProxy(null,null,null,true);
        testUtil = new IndexerIntegrationTestUtil(lilyProxy);
    }

    @AfterClass
    public static void stopLily() throws Exception {
        testUtil.stop();
    }

    @Before
    public void setUp() throws Exception {
        model = new RepositoryModelImpl(lilyProxy.getLilyServerProxy().getZooKeeper());
        hBaseAdmin = new HBaseAdmin(lilyProxy.getHBaseProxy().getConf());
        indexerModel = lilyProxy.getLilyServerProxy().getIndexerModel();
    }

    @Test
    public void testDeleteRepositoryRemovesTenantMaps() throws Exception {
        model.delete(testUtil.secundaryRepo.getRepositoryName());
        HTableDescriptor[] descriptors = hBaseAdmin.listTables();
        System.out.println(Arrays.toString(descriptors));
        waitForNoneOfTheseTablesExist(hBaseAdmin, "alternateRepo__record", "deref-backward-secundary",
                "deref-forward-secundary");
        //cleanup
        deleteIndex("secundary");
    }

    @Test(expected = IndexUpdateException.class)
    @Ignore //we no longer check on this
    public void testChangeRepositoryForIndex() throws Exception {
        LRepository firstNewRepo = testUtil.getAlternateTestRespository("repo1");
        LRepository secondNewRepo = testUtil.getAlternateTestRespository("repo2");
        testUtil.createIndex("testChRepo", "dummy", firstNewRepo);

        String lock = indexerModel.lockIndexer("testChRepo");
        try {
            IndexerDefinition index = indexerModel.getIndexer("testChRepo");

            ByteArrayInputStream is = new ByteArrayInputStream(index.getConfiguration());
            IndexerConf indexerConf = new XmlIndexerConfReader().read(is);
            indexerConf.getGlobalParams().put("repository", secondNewRepo.getRepositoryName());
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            XmlIndexerConfWriter.writeConf(indexerConf, os);
            indexerModel.updateIndexer(new IndexerDefinitionBuilder()
                    .startFrom(index)
                    .configuration(os.toByteArray())
                    .build(), lock);
        } finally {
            indexerModel.unlockIndexer(lock, true);
            deleteIndex("testChRepo");
            model.delete("repo1");
            model.delete("repo2");
        }
    }

    @Test
    public void testDropAndRecreateIndex() throws Exception {
        try {
            LRepository firstNewRepo = testUtil.getAlternateTestRespository("repo3");
            LRepository secondNewRepo = testUtil.getAlternateTestRespository("repo4");
            testUtil.createIndex("testBobbyTables", "dummyCORE", firstNewRepo);

            deleteIndex("testBobbyTables");

            testUtil.createIndex("testBobbyTables", "dummyCORE", secondNewRepo);
            lilyProxy.getHBaseProxy().waitOnSepIdle(10000L);

            checkOwner(hBaseAdmin, secondNewRepo.getRepositoryName(),
                    "deref-backward-testBobbyTables", "deref-forward-testBobbyTables");
        } finally {
            deleteIndex("testBobbyTables");
            model.delete("repo3");
            model.delete("repo4");
        }
    }

    public void deleteIndex(final String indexName) throws Exception {
        //set this up so we can wait for it later
        final CountDownLatch latch = new CountDownLatch(1);
        indexerModel.getIndexers(new IndexerModelListener() {
            @Override
            public void process(IndexerModelEvent event) {
                if (indexName.equals(event.getIndexerName())
                        && IndexerModelEventType.INDEXER_DELETED.equals(event.getType())) {
                    latch.countDown();
                }
            }
        });

        //request the delete
        String lock = indexerModel.lockIndexer(indexName);
        try {
            indexerModel.updateIndexer(new IndexerDefinitionBuilder()
                    .startFrom(indexerModel.getIndexer(indexName))
                    .lifecycleState(IndexerDefinition.LifecycleState.DELETE_REQUESTED)
                    .build(), lock);
        } finally {
            indexerModel.unlockIndexer(lock, true);
        }

        //wait for delete to finish.
        latch.await(MINS15, TimeUnit.MILLISECONDS);
    }

    public void waitForNoneOfTheseTablesExist(HBaseAdmin hBaseAdmin, String... tableNames) throws Exception {
        long start = System.currentTimeMillis();
        Set<String> leftOvers = checkTableExistence(hBaseAdmin, tableNames);
        while (leftOvers.size() > 0) {
            Thread.sleep(1000);
            if (System.currentTimeMillis() - start > 1000 * 30)
                throw new AssertionError("Waited too long for tables to get deleted: " + leftOvers);
            leftOvers = checkTableExistence(hBaseAdmin, tableNames);
        }

    }

    public void checkOwner(HBaseAdmin hBaseAdmin, String owner, String... tableNames) throws IOException {
        for (String tableName : tableNames) {
            HTableDescriptor descriptor = hBaseAdmin.getTableDescriptor(Bytes.toBytes(tableName));
            String actualOwner = descriptor.getValue("lilyOwningRepository");
            assertEquals(owner, actualOwner);
        }
    }

    private Set<String> checkTableExistence(HBaseAdmin hBaseAdmin, String[] tableNames) throws IOException {
        Set<String> names = Sets.newHashSet(tableNames);
        Set<String> currenttables = Sets.newHashSet();
        HTableDescriptor[] hTableDescriptors = hBaseAdmin.listTables();
        for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
            currenttables.add(hTableDescriptor.getNameAsString());
        }
        return Sets.intersection(names, currenttables);
    }

}
