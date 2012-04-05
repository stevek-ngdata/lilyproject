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
package org.lilyproject.hadooptestfw;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.lilyproject.cli.BaseCliTool;
import org.lilyproject.hadooptestfw.fork.HBaseTestingUtility;
import org.lilyproject.util.Version;
import org.lilyproject.util.test.TestHomeUtil;

import java.io.File;

/**
 * Utility to easily launch a full HBase with a temporary storage. Intended to be used to run testcases
 * against (see HBaseProxy connect mode).
 */
public class HadoopLauncher extends BaseCliTool {
    private File baseTempDir;
    private Configuration conf;

    @Override
    protected String getCmdName() {
        return "launch-hadoop";
    }

    @Override
    protected String getVersion() {
        return Version.readVersion("org.lilyproject", "lily-hadoop-test-fw");
    }

    public static void main(String[] args) throws Exception {
        new HadoopLauncher().start(args);
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        int result = super.run(cmd);
        if (result != 0)
            return result;

        System.out.println("Starting...");

        conf = HBaseConfiguration.create();

        baseTempDir = TestHomeUtil.createTestHome("launch-hadoop-");

        HBaseTestingUtility testUtil = HBaseTestingUtilityFactory.create(conf, baseTempDir, true);
        testUtil.startMiniCluster(1);
        testUtil.startMiniMapReduceCluster(1);

        System.out.println("-------------------------");
        System.out.println("Minicluster is up");
        System.out.println();
        System.out.println("To connect to this HBase, use the following properties:");
        System.out.println("hbase.zookeeper.quorum=localhost");
        System.out.println("hbase.zookeeper.property.clientPort=2181");
        System.out.println();
        System.out.println("In Java code, create the HBase configuration like this:");
        System.out.println("Configuration conf = HBaseConfiguration.create();");
        System.out.println("conf.set(\"hbase.zookeeper.quorum\", \"localhost\");");
        System.out.println("conf.set(\"hbase.zookeeper.property.clientPort\", \"2181\");");
        System.out.println();
        System.out.println("For MapReduce, use:");
        System.out.println("Configuration conf = new Configuration();");
        System.out.println("conf.set(\"mapred.job.tracker\", \"localhost:" +
                testUtil.getMRCluster().getJobTrackerPort() + "\");");
        System.out.println("Job job = new Job(conf);");
        System.out.println();
        System.out.println("JobTracker web ui:   http://localhost:" +
                testUtil.getMRCluster().getJobTrackerRunner().getJobTrackerInfoPort());
        System.out.println("HDFS web ui:         http://" + conf.get("dfs.http.address"));
        System.out.println("HBase master web ui: http://localhost:" +
                testUtil.getHBaseCluster().getMaster().getInfoServer().getPort());
        System.out.println("-------------------------");

        return 0;
    }

}
