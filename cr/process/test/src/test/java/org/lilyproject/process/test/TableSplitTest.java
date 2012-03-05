package org.lilyproject.process.test;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilyproject.client.LilyClient;
import org.lilyproject.lilyservertestfw.LilyProxy;
import org.lilyproject.repository.api.*;
import org.lilyproject.util.test.TestHomeUtil;

import java.io.File;
import java.io.IOException;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This test verifies that pre-splitted record tables work correctly (for UUID-based record id's).
 */
public class TableSplitTest {
    private static LilyProxy lilyProxy;
    private static File tmpDir;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        lilyProxy = new LilyProxy();

        //
        // Make multiple record table splits
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
        tmpDir = TestHomeUtil.createTestHome("lily-tablesplit-test-");

        File customConfDir = setupConfDirectory(tmpDir);
        System.setProperty("lily.conf.customdir", customConfDir.getAbsolutePath());

        try {
            lilyProxy = new LilyProxy();
            lilyProxy.start();
        } finally {
            // Make sure it's properties won't be used by later-running tests
            System.getProperties().remove("lily.conf.customdir");
        }
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        if (lilyProxy != null)
            lilyProxy.stop();
        TestHomeUtil.cleanupTestHome(tmpDir);

        if (lilyProxy.getMode() == LilyProxy.Mode.CONNECT || lilyProxy.getMode() == LilyProxy.Mode.HADOOP_CONNECT) {
            // We're in connect mode, drop the record table again so that the remainder of the tests
            // don't have the overhead of the extra splits
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "localhost");
            HBaseAdmin hbaseAdmin = new HBaseAdmin(conf);
            if (hbaseAdmin.tableExists("record")) {
                hbaseAdmin.disableTable("record");
                hbaseAdmin.deleteTable("record");
            }
            HConnectionManager.deleteConnection(hbaseAdmin.getConfiguration(), true);
        }
    }

    private static File setupConfDirectory(File tmpDir) throws Exception {
        File confDir = new File(tmpDir, "conf");

        File generalConfDir = new File(confDir, "general");
        FileUtils.forceMkdir(generalConfDir);

        // Write configuration to activate the decorator
        String tablesXml = "<tables xmlns:conf='http://kauriproject.org/configuration' conf:inherit='shallow'>" +
                "<table name='record'><splits><regionCount>3</regionCount><splitKeyPrefix>\\x01</splitKeyPrefix>" +
                "</splits></table>" +
                "</tables>";

        FileUtils.writeStringToFile(new File(generalConfDir, "tables.xml"), tablesXml, "UTF-8");

        return confDir;
    }

    @Test
    public void testOne() throws Exception {
        LilyClient client = lilyProxy.getLilyServerProxy().getClient();

        //
        // Create some records
        //
        Repository repository = client.getRepository();
        TypeManager typeManager = repository.getTypeManager();
        IdGenerator idGenerator = repository.getIdGenerator();

        FieldType ft1 = typeManager.createFieldType("STRING", new QName("test", "field1"), Scope.NON_VERSIONED);

        RecordType rt1 = typeManager.recordTypeBuilder()
                .defaultNamespace("test")
                .name("rt1")
                .fieldEntry().use(ft1).add()
                .create();

        for (int i = 0; i < 300; i++) {
            repository.recordBuilder()
                    .recordType(rt1.getName())
                    .field(ft1.getName(), "foo bar bar")
                    .create();
        }

        //
        // Count number of records in each region
        //
        HTable table = new HTable(lilyProxy.getHBaseProxy().getConf(), "record");
        for (HRegionInfo regionInfo : table.getRegionsInfo().keySet()) {
            Scan scan = new Scan();
            scan.setStartRow(regionInfo.getStartKey());
            scan.setStopRow(regionInfo.getEndKey());
            
            ResultScanner scanner = table.getScanner(scan);
            int count = 0;
            for (Result result : scanner) {
                count++;
            }
            
            assertTrue("Number of splits in region " + regionInfo.getRegionNameAsString(), count > 85);
        }

    }

}
