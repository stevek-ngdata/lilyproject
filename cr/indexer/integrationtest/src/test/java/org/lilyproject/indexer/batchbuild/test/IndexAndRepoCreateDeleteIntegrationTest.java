package org.lilyproject.indexer.batchbuild.test;


import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilyproject.indexer.model.api.IndexDefinition;
import org.lilyproject.indexer.model.api.IndexGeneralState;
import org.lilyproject.indexer.model.api.IndexUpdateException;
import org.lilyproject.indexer.model.api.WriteableIndexerModel;
import org.lilyproject.lilyservertestfw.LilyProxy;
import org.lilyproject.repository.api.LRepository;
import org.lilyproject.repository.model.impl.RepositoryModelImpl;

public class IndexAndRepoCreateDeleteIntegrationTest {

    private static LilyProxy lilyProxy;
    private static IndexerIntegrationTestUtil testUtil;

    private RepositoryModelImpl model;
    private HBaseAdmin hBaseAdmin;
    private WriteableIndexerModel indexerModel;

    @BeforeClass
    public static void startLily() throws Exception {
        lilyProxy = new LilyProxy();
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
    public void testChangeRepositoryForIndex() throws Exception {
        LRepository firstNewRepo = testUtil.getAlternateTestRespository("repo1");
        LRepository secondNewRepo = testUtil.getAlternateTestRespository("repo2");
        testUtil.createIndex("testChRepo", "dummy", firstNewRepo);

        String lock = indexerModel.lockIndex("testChRepo");
        try {
            IndexDefinition index = indexerModel.getMutableIndex("testChRepo");
            index.setRepositoryName(secondNewRepo.getRepositoryName());
            indexerModel.updateIndex(index, lock);
        } finally {
            indexerModel.unlockIndex(lock, true);
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

            waitForNoneOfTheseTablesExist(hBaseAdmin, "deref-backward-testBobbyTables",
                    "deref-forward-testBobbyTables");

            testUtil.createIndex("testBobbyTables", "dummyCORE", secondNewRepo);

            checkOwner(hBaseAdmin, secondNewRepo.getRepositoryName(),
                    "deref-backward-testBobbyTables", "deref-forward-testBobbyTables");
        } finally {
            deleteIndex("testBobbyTables");
            model.delete("repo3");
            model.delete("repo4");
        }
    }

    public void deleteIndex(String indexName) throws Exception {
        String lock = indexerModel.lockIndex(indexName);
        try {
            IndexDefinition index = indexerModel.getMutableIndex(indexName);
            index.setGeneralState(IndexGeneralState.DELETE_REQUESTED);
            indexerModel.updateIndex(index, lock);
        } finally {
            indexerModel.unlockIndex(lock, true);
        }
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
