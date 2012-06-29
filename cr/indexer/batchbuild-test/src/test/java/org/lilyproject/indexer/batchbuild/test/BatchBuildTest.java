package org.lilyproject.indexer.batchbuild.test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilyproject.indexer.model.api.IndexBatchBuildState;
import org.lilyproject.indexer.model.api.IndexDefinition;
import org.lilyproject.indexer.model.api.IndexUpdateState;
import org.lilyproject.indexer.model.api.WriteableIndexerModel;
import org.lilyproject.lilyservertestfw.LilyProxy;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.solrtestfw.SolrProxy;
import org.lilyproject.util.json.JsonFormat;
import org.lilyproject.util.test.TestHomeUtil;

public class BatchBuildTest {
    private static LilyProxy lilyProxy;
    private static File tmpDir;
    private static int BUILD_TIMEOUT = 120000;

    private FieldType ft1;
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

        // Set the solr schema
        InputStream is = BatchBuildTest.class.getResourceAsStream("solrschema.xml");
        lilyProxy.getSolrProxy().changeSolrSchema(IOUtils.toByteArray(is));
        IOUtils.closeQuietly(is);

        TypeManager typeManager = lilyProxy.getLilyServerProxy().getClient().getRepository().getTypeManager();
        FieldType ft1 = typeManager.createFieldType("STRING", new QName("batchindex-test", "field1"),
                Scope.NON_VERSIONED);
        typeManager.recordTypeBuilder()
                .defaultNamespace("batchindex-test")
                .name("rt1")
                .fieldEntry().use(ft1).add()
                .create();

        // Set the index
        lilyProxy.getLilyServerProxy().addIndexFromResource(INDEX_NAME,
                "org/lilyproject/indexer/batchbuild/test/indexerconf.xml", 500);

        WriteableIndexerModel model = lilyProxy.getLilyServerProxy().getIndexerModel();
        String lock = model.lockIndex(INDEX_NAME);
        try {
            IndexDefinition index = model.getMutableIndex("batchtest");
            index.setUpdateState(IndexUpdateState.DO_NOT_SUBSCRIBE);
            model.updateIndex(index, lock);

        } finally {
            model.unlockIndex(lock);
        }
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
        Repository repository = lilyProxy.getLilyServerProxy().getClient().getRepository();
        TypeManager typeManager = repository.getTypeManager();

        this.ft1 = typeManager.getFieldTypeByName(new QName("batchindex-test", "field1"));
        this.rt1 = typeManager.getRecordTypeByName(new QName("batchindex-test", "rt1"), null);

    }

    @Test
    public void testBatchIndex() throws Exception {
        Repository repository = lilyProxy.getLilyServerProxy().getClient().getRepository();
        SolrProxy solrProxy = lilyProxy.getSolrProxy();

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

        QueryResponse response = solrProxy.getSolrServer().query(new SolrQuery("field1:test1*"));
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
        Repository repository = lilyProxy.getLilyServerProxy().getClient().getRepository();
        SolrProxy solrProxy = lilyProxy.getSolrProxy();

        WriteableIndexerModel model = lilyProxy.getLilyServerProxy().getIndexerModel();
        String lock = model.lockIndex(this.INDEX_NAME);
        byte[] defaultConf = null;
        try {
            IndexDefinition index = model.getMutableIndex(this.INDEX_NAME);
            defaultConf = getResourceAsByteArray("defaultBatchIndexConf-test2.json");
            index.setDefaultBatchIndexConfiguration(defaultConf);
            model.updateIndex(index, lock);
        } finally {
            model.unlockIndex(lock);
        }

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
        QueryResponse response = solrProxy.getSolrServer().query(new SolrQuery("field1:test2*"));
        Assert.assertEquals(1, response.getResults().size());
        Assert.assertEquals("USER." + "batch-index-test2", response.getResults().get(0).getFieldValue("lily.id"));
        // check that  the last used batch index conf = default
        IndexDefinition index = model.getMutableIndex(this.INDEX_NAME);
        
        Assert.assertEquals(JsonFormat.deserialize(defaultConf), JsonFormat.deserialize(index.getLastBatchBuildInfo().getBatchIndexConfiguration()));
    }

    /**
     * Test setting a custom batch index conf.
     * 
     * @throws Exception
     */
    @Test
    public void testCustomBatchIndexConf() throws Exception {
        Repository repository = lilyProxy.getLilyServerProxy().getClient().getRepository();
        SolrProxy solrProxy = lilyProxy.getSolrProxy();
        WriteableIndexerModel model = lilyProxy.getLilyServerProxy().getIndexerModel();

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
        SolrDocumentList results = solrProxy.getSolrServer().query(new SolrQuery("field1:test3*").
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
        ;
        byte[] defaultConf = getResourceAsByteArray("defaultBatchIndexConf-test2.json");
        ;
        String lock = model.lockIndex(this.INDEX_NAME);
        try {
            IndexDefinition index = model.getMutableIndex(this.INDEX_NAME);
            index.setDefaultBatchIndexConfiguration(defaultConf);
            index.setBatchIndexConfiguration(batchConf);
            index.setBatchBuildState(IndexBatchBuildState.BUILD_REQUESTED);
            model.updateIndex(index, lock);
        } finally {
            model.unlockIndex(lock);
        }

        boolean indexSuccess = false;
        try {
            // Now wait until its finished
            long tryUntil = System.currentTimeMillis() + BUILD_TIMEOUT;
            while (System.currentTimeMillis() < tryUntil) {
                Thread.sleep(100);
                IndexDefinition definition = model.getIndex(INDEX_NAME);
                if (definition.getBatchBuildState() == IndexBatchBuildState.INACTIVE) {
                    Long amountFailed = definition.getLastBatchBuildInfo().getCounters()
                            .get(COUNTER_NUM_FAILED_RECORDS);
                    boolean successFlag = definition.getLastBatchBuildInfo().getSuccess();
                    indexSuccess = successFlag && (amountFailed == null || amountFailed == 0L);
                } else if (definition.getBatchBuildState() == IndexBatchBuildState.BUILDING) {
                    Assert.assertEquals(JsonFormat.deserialize(batchConf), 
                            JsonFormat.deserialize(definition.getActiveBatchBuildInfo().getBatchIndexConfiguration()));
                }
            }
        } catch (Exception e) {
            throw new Exception("Error checking if batch index job ended.", e);
        }

        if (!indexSuccess) {
            Assert.fail("Batch build did not end after " + BUILD_TIMEOUT + " millis");
        } else {
            lilyProxy.getSolrProxy().getSolrServer().commit();
        }

        // Check if 1 record and not 2 are in the index
        QueryResponse response = solrProxy.getSolrServer().query(new SolrQuery("field1:test3\\ index\\ run2"));
        Assert.assertEquals(1, response.getResults().size());
        Assert.assertEquals("USER." + assertId1, response.getResults().get(0).getFieldValue("lily.id"));
        // check that the last used batch index conf = default
        Assert.assertEquals(JsonFormat.deserialize(batchConf), JsonFormat.deserialize(
                model.getMutableIndex(this.INDEX_NAME).getLastBatchBuildInfo().getBatchIndexConfiguration()));

        // Set things up for run 3 where the default configuration should be used again
        recordToChange1.setField(ft1.getName(), "test3 index run3");
        recordToChange2.setField(ft1.getName(), "test3 index run3");
        repository.update(recordToChange1);
        repository.update(recordToChange2);

        // Now rebuild the index and see if the default indexer has kicked in
        this.buildAndCommit();
        response = solrProxy.getSolrServer().query(new SolrQuery("field1:test3\\ index\\ run3").
                addSortField("lily.id", ORDER.asc));
        Assert.assertEquals(2, response.getResults().size());
        Assert.assertEquals("USER." + assertId1, response.getResults().get(0).getFieldValue("lily.id"));
        Assert.assertEquals("USER." + assertId2, response.getResults().get(1).getFieldValue("lily.id"));
        // check that the last used batch index conf = default
        Assert.assertEquals(JsonFormat.deserialize(defaultConf), JsonFormat.deserialize(
                model.getMutableIndex(this.INDEX_NAME).getLastBatchBuildInfo().getBatchIndexConfiguration()));
    }

    /**
     * This test should cause a failure when adding a custom batchindex conf without setting a buildrequest
     * 
     * @throws Exception
     */
    @Test(expected = org.lilyproject.indexer.model.api.IndexValidityException.class)
    public void testCustomBatchIndexConf_NoBuild() throws Exception {
        WriteableIndexerModel model = lilyProxy.getLilyServerProxy().getIndexerModel();
        String lock = model.lockIndex(this.INDEX_NAME);
        try {
            IndexDefinition index = model.getMutableIndex(this.INDEX_NAME);
            index.setDefaultBatchIndexConfiguration(getResourceAsByteArray("defaultBatchIndexConf-test2.json"));
            index.setBatchIndexConfiguration(getResourceAsByteArray("batchIndexConf-test3.json"));
            model.updateIndex(index, lock);
        } finally {
            model.unlockIndex(lock);
        }
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

    private void buildAndCommit() throws Exception {
        boolean success = lilyProxy.getLilyServerProxy().batchBuildIndex(this.INDEX_NAME, BUILD_TIMEOUT);
        if (success) {
            lilyProxy.getSolrProxy().getSolrServer().commit();
        } else {
            Assert.fail("Batch build did not end after " + BUILD_TIMEOUT + " millis");
        }
    }

}
