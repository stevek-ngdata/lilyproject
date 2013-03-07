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
package org.lilyproject.mapreduce.test;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilyproject.client.LilyClient;
import org.lilyproject.lilyservertestfw.LilyProxy;
import org.lilyproject.mapreduce.LilyMapReduceUtil;
import org.lilyproject.mapreduce.testjobs.Test1Mapper;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.RecordScan;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.util.test.TestHomeUtil;

import static org.junit.Assert.assertEquals;

public class MapReduceTest {
    private static LilyProxy lilyProxy;
    private static File tmpDir;

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
        tmpDir = TestHomeUtil.createTestHome("lily-mapreduce-test-");

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

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        if (lilyProxy != null) {
            lilyProxy.stop();
        }
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
        String tablesXml = "<tables xmlns:conf='http://lilyproject.org/configuration' conf:inherit='shallow'>" +
                "<table name='record'><splits><regionCount>5</regionCount>" +
                "<splitKeys>\\x00020,\\x00040,\\x00060,\\x00080</splitKeys></splits></table>" +
                "</tables>";

        FileUtils.writeStringToFile(new File(generalConfDir, "tables.xml"), tablesXml, "UTF-8");

        return confDir;
    }

    @Test
    public void testOne() throws Exception {
        LilyClient client = lilyProxy.getLilyServerProxy().getClient();

        //
        // First create some content
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

        for (int i = 0; i < 100; i++) {
            repository.recordBuilder()
                    .id(String.format("%1$03d", i))
                    .recordType(rt1.getName())
                    .field(ft1.getName(), "foo bar bar")
                    .create();
        }

        //
        // Launch MapReduce job
        //
        {
            Configuration config = HBaseConfiguration.create();

            config.set("mapred.job.tracker", "localhost:9001");
            config.set("fs.defaultFS", "hdfs://localhost:8020");

            Job job = new Job(config, "Test1");
            job.setJarByClass(Test1Mapper.class);

            job.setMapperClass(Test1Mapper.class);

            job.setOutputFormatClass(NullOutputFormat.class);

            job.setNumReduceTasks(0);

            LilyMapReduceUtil.initMapperJob(null, "localhost", client, job);

            boolean b = job.waitForCompletion(true);
            if (!b) {
                throw new IOException("error with job!");
            }

            // Verify some counters
            assertEquals("Number of launched map tasks", 5L, getTotalLaunchedMaps(job));
            assertEquals("Number of input records", 100L, getTotalInputRecords(job));
        }


        //
        // Launch a job with a custom scan
        //
        {
            Configuration config = HBaseConfiguration.create();

            config.set("mapred.job.tracker", "localhost:9001");
            config.set("fs.defaultFS", "hdfs://localhost:8020");

            Job job = new Job(config, "Test1");
            job.setJarByClass(Test1Mapper.class);

            job.setMapperClass(Test1Mapper.class);

            job.setOutputFormatClass(NullOutputFormat.class);

            job.setNumReduceTasks(0);

            RecordScan scan = new RecordScan();
            scan.setStartRecordId(idGenerator.newRecordId(String.format("%1$03d", 15)));
            scan.setStopRecordId(idGenerator.newRecordId(String.format("%1$03d", 25)));

            LilyMapReduceUtil.initMapperJob(scan, "localhost", client, job);

            boolean b = job.waitForCompletion(true);
            if (!b) {
                throw new IOException("error with job!");
            }

            // expect 2 map tasks: our scan crossed the 020 border
            assertEquals("Number of launched map tasks", 2L, getTotalLaunchedMaps(job));
            assertEquals("Number of input records", 10L, getTotalInputRecords(job));

            /*
            for (CounterGroup cgroup: job.getCounters()) {
                for (Counter counter : cgroup) {
                    System.out.println(cgroup.getName() + " -> " + counter.getName() + " = " + counter.getValue());
                }
            }
            */
        }

    }

    private long getTotalLaunchedMaps(Job job) throws IOException {
        return job.getCounters().findCounter("org.apache.hadoop.mapreduce.JobCounter", "TOTAL_LAUNCHED_MAPS").getValue();
    }

    private long getTotalInputRecords(Job job) throws IOException {
        return job.getCounters().findCounter("org.apache.hadoop.mapreduce.TaskCounter", "MAP_INPUT_RECORDS").getValue();
    }
}
