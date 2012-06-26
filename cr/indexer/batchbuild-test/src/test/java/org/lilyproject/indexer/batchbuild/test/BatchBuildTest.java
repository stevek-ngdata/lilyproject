package org.lilyproject.indexer.batchbuild.test;

import java.io.File;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilyproject.client.LilyClient;
import org.lilyproject.indexer.master.BatchIndexBuilder;
import org.lilyproject.indexer.model.api.IndexDefinition;
import org.lilyproject.indexer.model.api.WriteableIndexerModel;
import org.lilyproject.indexer.model.impl.IndexerModelImpl;
import org.lilyproject.indexer.model.indexerconf.IndexerConfBuilder;
import org.lilyproject.lilyservertestfw.LilyProxy;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.solrtestfw.SolrProxy;
import org.lilyproject.util.test.TestHomeUtil;

public class BatchBuildTest {
    private static LilyProxy lilyProxy;
    private static File tmpDir;
    private static WriteableIndexerModel model;

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
        
        model = new IndexerModelImpl(lilyProxy.getLilyServerProxy().getZooKeeper());
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
    
    @Test
    public void testBatchIndex() throws Exception {
        LilyClient client = lilyProxy.getLilyServerProxy().getClient();
        SolrProxy solrProxy = lilyProxy.getSolrProxy();
        
        InputStream is = BatchBuildTest.class.getResourceAsStream("solrschema.xml");
        solrProxy.changeSolrSchema(IOUtils.toByteArray(is));        
        IOUtils.closeQuietly(is);
        
        QueryResponse response = solrProxy.getSolrServer().query(new SolrQuery("*:*"));
        Assert.assertEquals(0, response.getResults().size());

        //
        // First create some content
        //
        Repository repository = client.getRepository();
        TypeManager typeManager = repository.getTypeManager();
        
        FieldType ft1 = typeManager.createFieldType("STRING", new QName("batchindex-test", "field1"), Scope.NON_VERSIONED);
        
        RecordType rt1 = typeManager.recordTypeBuilder()
                .defaultNamespace("batchindex-test")
                .name("rt1")
                .fieldEntry().use(ft1).add()
                .create();
        
        lilyProxy.getLilyServerProxy().addIndexFromResource("batchtest", "org/lilyproject/indexer/batchbuild/test/indexerconf.xml", 5);
        
            repository.recordBuilder()
                    .id("batch-index-test")
                    .recordType(rt1.getName())
                    .field(ft1.getName(), "foo bar bar")
                    .create();
            
            
            boolean success = lilyProxy.getLilyServerProxy().batchBuildIndex("batchtest", 90000);
            if (success)
                solrProxy.commit();
            else
                Assert.fail("Batch build did not end after 90000 millis");
            
            response = solrProxy.getSolrServer().query(new SolrQuery("*:*"));
            Assert.assertEquals(1, response.getResults().size());
        
    
    }

}
