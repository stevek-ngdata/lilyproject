package org.lilyproject.indexer.batchbuild.test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilyproject.client.LilyClient;
import org.lilyproject.indexer.engine.DerefMap;
import org.lilyproject.indexer.engine.DerefMapHbaseImpl;
import org.lilyproject.indexer.model.api.IndexBatchBuildState;
import org.lilyproject.indexer.model.api.IndexConcurrentModificationException;
import org.lilyproject.indexer.model.api.IndexDefinition;
import org.lilyproject.indexer.model.api.IndexModelException;
import org.lilyproject.indexer.model.api.IndexNotFoundException;
import org.lilyproject.indexer.model.api.IndexUpdateException;
import org.lilyproject.indexer.model.api.IndexUpdateState;
import org.lilyproject.indexer.model.api.IndexValidityException;
import org.lilyproject.indexer.model.api.WriteableIndexerModel;
import org.lilyproject.lilyservertestfw.LilyProxy;
import org.lilyproject.lilyservertestfw.LilyServerProxy;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.Link;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.solrtestfw.SolrProxy;
import org.lilyproject.util.hbase.HBaseAdminFactory;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.json.JsonFormat;
import org.lilyproject.util.repo.VersionTag;
import org.lilyproject.util.test.TestHomeUtil;
import org.lilyproject.util.zookeeper.ZkConnectException;
import org.lilyproject.util.zookeeper.ZkLockException;

public class BatchBuildTest {
    private static LilyProxy lilyProxy;
    private static LilyClient lilyClient;
    private static Repository repository;
    private static TypeManager typeManager;
    private static SolrServer solrServer;
    private static SolrProxy solrProxy;
    private static LilyServerProxy lilyServerProxy;
    private static HBaseAdmin hbaseAdmin;
    private static WriteableIndexerModel model;
    private static File tmpDir;
    private static int BUILD_TIMEOUT = 240000;

    private FieldType ft1;
    private FieldType ft2;
    private RecordType rt1;

    private final static String INDEX_NAME = "batchtest";
    private static final String COUNTER_NUM_FAILED_RECORDS = "org.lilyproject.indexer.batchbuild.IndexBatchBuildCounters:NUM_FAILED_RECORDS";

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        lilyProxy = new LilyProxy();

        //
        // Make multiple record table splits, so that our MR job will have multiple map tasks
        //

        if (lilyProxy.getMode() == LilyProxy.Mode.CONNECT || lilyProxy.getMode() == LilyProxy.Mode.HADOOP_CONNECT) {
            // The record table will likely already exist and not be recreated, hence we won't be able to change
            // the number of regions. Therefore, drop the table.
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "localhost");
            HBaseAdmin hbaseAdmin = new HBaseAdmin(conf);
            if (hbaseAdmin.tableExists("record")) {
                hbaseAdmin.disableTable("record");
                hbaseAdmin.deleteTable("record");
            }
            HConnectionManager.deleteConnection(hbaseAdmin.getConfiguration(), true);
        }

        // Temp dir where we will create conf dir
        tmpDir = TestHomeUtil.createTestHome("lily-batchbuild-test-");

        File customConfDir = setupConfDirectory(tmpDir);
        String oldCustomConfDir = setProperty("lily.conf.customdir", customConfDir.getAbsolutePath());
        String oldRestoreTemplate = setProperty("lily.lilyproxy.restoretemplatedir", "false");

        try {
            lilyProxy.start();
        } finally {
            // Make sure the properties won't be used by later-running tests
            setProperty("lily.conf.customdir", oldCustomConfDir);
            setProperty("lily.lilyproxy.restoretemplatedir", oldRestoreTemplate);
        }

        solrProxy = lilyProxy.getSolrProxy();
        solrServer = solrProxy.getSolrServer();
        lilyServerProxy = lilyProxy.getLilyServerProxy();
        lilyClient = lilyServerProxy.getClient();
        repository = lilyClient.getRepository();

        // Set the solr schema
        InputStream is = BatchBuildTest.class.getResourceAsStream("solrschema.xml");
        solrProxy.changeSolrSchema(IOUtils.toByteArray(is));
        IOUtils.closeQuietly(is);

        typeManager = repository.getTypeManager();
        FieldType ft1 = typeManager.createFieldType("STRING", new QName("batchindex-test", "field1"),
                Scope.NON_VERSIONED);
        FieldType ft2 =typeManager.createFieldType("LINK", new QName("batchindex-test", "linkField"), Scope.NON_VERSIONED);
        typeManager.recordTypeBuilder()
                .defaultNamespace("batchindex-test")
                .name("rt1")
                .fieldEntry().use(ft1).add()
                .fieldEntry().use(ft2).add()
                .create();


        hbaseAdmin = HBaseAdminFactory.get(LilyClient.getHBaseConfiguration(lilyServerProxy.getZooKeeper()));

        model = lilyServerProxy.getIndexerModel();
        //String lock = model.lockIndex(INDEX_NAME);

        is =  BatchBuildTest.class.getResourceAsStream("indexerconf.xml");
        byte[] indexerConfiguration = IOUtils.toByteArray(is);
        IOUtils.closeQuietly(is);

        IndexDefinition index = model.newIndex(INDEX_NAME);
        Map<String, String> solrShards = new HashMap<String, String>();
        solrShards.put("shard1", "http://localhost:8983/solr");
        index.setSolrShards(solrShards);
        index.setConfiguration(indexerConfiguration);
        index.setUpdateState(IndexUpdateState.DO_NOT_SUBSCRIBE);
        model.addIndex(index);

    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception{
        Closer.close(repository);
        Closer.close(lilyClient);
        Closer.close(solrServer);
        Closer.close(solrProxy);
        Closer.close(lilyServerProxy);
        Closer.close(hbaseAdmin);

        lilyProxy.stop();

    }

    private static String setProperty(String name, String value) {
        String oldValue = System.getProperty(name);
        if (value == null) {
            System.getProperties().remove(name);
        } else {
            System.setProperty(name, value);
        }
        return oldValue;
    }

    private static File setupConfDirectory(File tmpDir) throws Exception {
        File confDir = new File(tmpDir, "conf");

        File generalConfDir = new File(confDir, "general");
        FileUtils.forceMkdir(generalConfDir);

        // Write configuration to activate the decorator
        String tablesXml = "<tables xmlns:conf='http://kauriproject.org/configuration' conf:inherit='shallow'>" +
                "<table name='record'><splits><regionCount>5</regionCount>" +
                "<splitKeys>\\x00020,\\x00040,\\x00060,\\x00080</splitKeys></splits></table>" +
                "</tables>";

        FileUtils.writeStringToFile(new File(generalConfDir, "tables.xml"), tablesXml, "UTF-8");

        return confDir;
    }

    @Before
    public void setup() throws Exception {
        this.ft1 = typeManager.getFieldTypeByName(new QName("batchindex-test", "field1"));
        this.ft2 = typeManager.getFieldTypeByName(new QName("batchindex-test", "linkField"));
        this.rt1 = typeManager.getRecordTypeByName(new QName("batchindex-test", "rt1"), null);


    }

    @Test
    public void testBatchIndex() throws Exception {
        String assertId = "batch-index-test";
        //
        // First create some content
        //
        repository.recordBuilder()
                .id("batch-index-test")
                .recordType(rt1.getName())
                .field(ft1.getName(), "test1")
                .create();

        this.buildAndCommit();

        QueryResponse response = solrServer.query(new SolrQuery("field1:test1*"));
        Assert.assertEquals(1, response.getResults().size());
        Assert.assertEquals("USER." + assertId, response.getResults().get(0).getFieldValue("lily.id"));
    }

    /**
     * Test if the default batch index conf setting works
     *
     * @throws Exception
     */
    @Test
    public void testDefaultBatchIndexConf() throws Exception {
        byte[] defaultConf = getResourceAsByteArray("defaultBatchIndexConf-test2.json");
        setBatchIndexConf(defaultConf, null, false);

        String assertId = "batch-index-test2";
        //
        // First create some content
        //
        repository.recordBuilder()
                .id(assertId)
                .recordType(rt1.getName())
                .field(ft1.getName(), "test2 index")
                .create();

        repository.recordBuilder()
                .id("batch-noindex-test2")
                .recordType(rt1.getName())
                .field(ft1.getName(), "test2 noindex")
                .create();

        // Now start the batch index
        this.buildAndCommit();

        // Check if 1 record and not 2 are in the index
        QueryResponse response = solrServer.query(new SolrQuery("field1:test2*"));
        Assert.assertEquals(1, response.getResults().size());
        Assert.assertEquals("USER." + "batch-index-test2", response.getResults().get(0).getFieldValue("lily.id"));
        // check that  the last used batch index conf = default
        IndexDefinition index = model.getMutableIndex(INDEX_NAME);

        Assert.assertEquals(JsonFormat.deserialize(defaultConf), JsonFormat.deserialize(index.getLastBatchBuildInfo().getBatchIndexConfiguration()));
    }

    /**
     * Test setting a custom batch index conf.
     *
     * @throws Exception
     */
    @Test
    public void testCustomBatchIndexConf() throws Exception {
        Assert.assertTrue(hbaseAdmin.isMasterRunning());
        byte[] defaultConf = getResourceAsByteArray("defaultBatchIndexConf-test2.json");
        setBatchIndexConf(defaultConf, null, false);
        Assert.assertTrue(hbaseAdmin.isMasterRunning());

        String assertId1 = "batch-index-custom-test3";
        String assertId2 = "batch-index-test3";
        //
        // First create some content
        //
        Record recordToChange1 = repository.recordBuilder()
                .id(assertId2)
                .recordType(rt1.getName())
                .field(ft1.getName(), "test3 index run1")
                .create();

        Record recordToChange2 = repository.recordBuilder()
                .id(assertId1)
                .recordType(rt1.getName())
                .field(ft1.getName(), "test3 index run1")
                .create();

        repository.recordBuilder()
                .id("batch-noindex-test3")
                .recordType(rt1.getName())
                .field(ft1.getName(), "test3 noindex run1")
                .create();

        // Index everything with the default conf

        this.buildAndCommit();
        Assert.assertTrue(hbaseAdmin.isMasterRunning());

        SolrDocumentList results = solrServer.query(new SolrQuery("field1:test3*").
                addSortField("lily.id", ORDER.asc)).getResults();
        Assert.assertEquals(2, results.size());
        Assert.assertEquals("USER." + assertId1, results.get(0).getFieldValue("lily.id"));
        Assert.assertEquals("USER." + assertId2, results.get(1).getFieldValue("lily.id"));

        // change some fields and reindex using a specific configuration. Only one of the 2 changes should be picked up
        recordToChange1.setField(ft1.getName(), "test3 index run2");
        recordToChange2.setField(ft1.getName(), "test3 index run2");
        repository.update(recordToChange1);
        repository.update(recordToChange2);

        byte[] batchConf = getResourceAsByteArray("batchIndexConf-test3.json");
        setBatchIndexConf(defaultConf, batchConf, true);
        Assert.assertTrue(hbaseAdmin.isMasterRunning());

        waitForIndexAndCommit(BUILD_TIMEOUT);

        // Check if 1 record and not 2 are in the index
        QueryResponse response = solrServer.query(new SolrQuery("field1:test3\\ index\\ run2"));
        Assert.assertEquals(1, response.getResults().size());
        Assert.assertEquals("USER." + assertId1, response.getResults().get(0).getFieldValue("lily.id"));
        // check that the last used batch index conf = default
        Assert.assertEquals(JsonFormat.deserialize(batchConf), JsonFormat.deserialize(
                model.getMutableIndex(INDEX_NAME).getLastBatchBuildInfo().getBatchIndexConfiguration()));

        // Set things up for run 3 where the default configuration should be used again
        recordToChange1.setField(ft1.getName(), "test3 index run3");
        recordToChange2.setField(ft1.getName(), "test3 index run3");
        repository.update(recordToChange1);
        repository.update(recordToChange2);
        System.out.println("It runs now");
        Assert.assertTrue(hbaseAdmin.isMasterRunning());
        // Now rebuild the index and see if the default indexer has kicked in
        this.buildAndCommit();

        System.out.println("here it stops");
        Assert.assertTrue(hbaseAdmin.isMasterRunning());
        response = solrServer.query(new SolrQuery("field1:test3\\ index\\ run3").
                addSortField("lily.id", ORDER.asc));
        Assert.assertEquals(2, response.getResults().size());
        Assert.assertEquals("USER." + assertId1, response.getResults().get(0).getFieldValue("lily.id"));
        Assert.assertEquals("USER." + assertId2, response.getResults().get(1).getFieldValue("lily.id"));
        // check that the last used batch index conf = default
        Assert.assertEquals(JsonFormat.deserialize(defaultConf), JsonFormat.deserialize(
                model.getMutableIndex(INDEX_NAME).getLastBatchBuildInfo().getBatchIndexConfiguration()));
    }

    /**
     * This test should cause a failure when adding a custom batchindex conf without setting a buildrequest
     *
     * @throws Exception
     */
    @Test(expected = org.lilyproject.indexer.model.api.IndexValidityException.class)
    public void testCustomBatchIndexConf_NoBuild() throws Exception {
        setBatchIndexConf(getResourceAsByteArray("defaultBatchIndexConf-test2.json"),
                getResourceAsByteArray("batchIndexConf-test3.json"), false);
    }

    private byte[] getResourceAsByteArray(String name) throws IOException {
        InputStream is = null;
        try {
            is = BatchBuildTest.class.getResourceAsStream(name);
            return IOUtils.toByteArray(is);
        } finally {
            IOUtils.closeQuietly(is);
        }
    }

    @Test
    public void testClearDerefMap() throws Exception {
        DerefMap derefMap = DerefMapHbaseImpl.create(INDEX_NAME, LilyClient.getHBaseConfiguration(lilyServerProxy.getZooKeeper()), repository.getIdGenerator());

        Record linkedRecord = repository.recordBuilder()
            .id("deref-test-linkedrecord")
            .recordType(rt1.getName())
            .field(ft1.getName(), "deref test linkedrecord")
            .create();

        Record record = repository.recordBuilder()
            .id("deref-test-main")
            .recordType(rt1.getName())
            .field(ft1.getName(), "deref test main")
            .field(ft2.getName(), new Link(linkedRecord.getId()))
            .create();

        SchemaId vtag = typeManager.getFieldTypeByName(VersionTag.LAST).getId();
        DerefMap.DependantRecordIdsIterator it = null;

        try {
            it = derefMap.findDependantsOf(linkedRecord.getId(), ft1.getId(), vtag);
            Assert.assertTrue(!it.hasNext());
        } finally {
            it.close();
        }

        setBatchIndexConf(getResourceAsByteArray("batchIndexConf-testClearDerefmap-false.json"), null, false);

        buildAndCommit();

        QueryResponse response = solrServer.query(new SolrQuery("field1:deref\\ test\\ main"));
        Assert.assertEquals(1, response.getResults().size());

        try {
            it = derefMap.findDependantsOf(linkedRecord.getId(), ft1.getId(), vtag);
            Assert.assertTrue(it.hasNext());
        } finally {
            it.close();
        }

        setBatchIndexConf(null,getResourceAsByteArray("batchIndexConf-testClearDerefmap-true.json"), true);
        waitForIndexAndCommit(BUILD_TIMEOUT);

        try {
            it = derefMap.findDependantsOf(linkedRecord.getId(), ft1.getId(), vtag);
            Assert.assertTrue(!it.hasNext());
        } finally {
            it.close();
        }
    }

    private void buildAndCommit() throws Exception {
        boolean success = lilyServerProxy.batchBuildIndex(INDEX_NAME, BUILD_TIMEOUT);
        if (success) {
            solrServer.commit();
        } else {
            Assert.fail("Batch build did not end after " + BUILD_TIMEOUT + " millis");
        }
    }

    private void waitForIndexAndCommit(long timeout) throws Exception {
        boolean indexSuccess = false;
        try {
            // Now wait until its finished
            long tryUntil = System.currentTimeMillis() + timeout;
            while (System.currentTimeMillis() < tryUntil) {
                Thread.sleep(100);
                IndexDefinition definition = model.getIndex(INDEX_NAME);
                if (definition.getBatchBuildState() == IndexBatchBuildState.INACTIVE) {
                    Long amountFailed = definition.getLastBatchBuildInfo().getCounters()
                            .get(COUNTER_NUM_FAILED_RECORDS);
                    boolean successFlag = definition.getLastBatchBuildInfo().getSuccess();
                    indexSuccess = successFlag && (amountFailed == null || amountFailed == 0L);
                }
            }
        } catch (Exception e) {
            throw new Exception("Error checking if batch index job ended.", e);
        }

        if (!indexSuccess) {
            Assert.fail("Batch build did not end after " + BUILD_TIMEOUT + " millis");
        } else {
            solrServer.commit();
        }

    }

    private static void setBatchIndexConf (byte[] defaultConf, byte[] customConf, boolean buildNow)
            throws IndexValidityException, KeeperException, InterruptedException, ZkConnectException,
            IndexModelException, IndexNotFoundException, ZkLockException, IndexUpdateException,
            IndexConcurrentModificationException{
        String lock = model.lockIndex(INDEX_NAME);

        try {
            IndexDefinition index = model.getMutableIndex(INDEX_NAME);
            if (defaultConf != null)
                index.setDefaultBatchIndexConfiguration(defaultConf);
            if (customConf != null)
                index.setBatchIndexConfiguration(customConf);
            if (buildNow)
                index.setBatchBuildState(IndexBatchBuildState.BUILD_REQUESTED);

            model.updateIndex(index, lock);
        } finally {
            model.unlockIndex(lock);
        }
    }
}
