package org.lilyproject.lilyservertestfw.launcher;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.lilyproject.testfw.HBaseTestingUtilityFactory;

import java.io.File;
import java.util.List;

public class HadoopLauncherService implements LauncherService {
    private org.lilyproject.testfw.fork.HBaseTestingUtility hbaseTestUtility;
    private Configuration conf;
    private File testHome;

    private Log log = LogFactory.getLog(getClass());

    @Override
    public void addOptions(List<Option> options) {
    }

    @Override
    public int setup(CommandLine cmd, File testHome) throws Exception {
        this.testHome = testHome;
        return 0;
    }

    @Override
    public int start(List<String> postStartupInfo) throws Exception {
        conf = HBaseConfiguration.create();

        hbaseTestUtility = HBaseTestingUtilityFactory.create(conf, testHome);
        hbaseTestUtility.startMiniCluster(1);
        hbaseTestUtility.startMiniMapReduceCluster(1);

        postStartupInfo.add("-------------------------");
        postStartupInfo.add("HDFS is running");
        postStartupInfo.add("");
        postStartupInfo.add("HDFS web ui: http://" + conf.get("dfs.http.address"));
        postStartupInfo.add("");

        postStartupInfo.add("-------------------------");
        postStartupInfo.add("HBase is running");
        postStartupInfo.add("");
        postStartupInfo.add("HBase master web ui: http://localhost:" +
                hbaseTestUtility.getHBaseCluster().getMaster().getInfoServer().getPort());
        postStartupInfo.add("");
        postStartupInfo.add("To connect to this HBase, use the following properties:");
        postStartupInfo.add("hbase.zookeeper.quorum=localhost");
        postStartupInfo.add("hbase.zookeeper.property.clientPort=2181");
        postStartupInfo.add("");
        postStartupInfo.add("In Java code, create the HBase configuration like this:");
        postStartupInfo.add("Configuration conf = HBaseConfiguration.create();");
        postStartupInfo.add("conf.set(\"hbase.zookeeper.quorum\", \"localhost\");");
        postStartupInfo.add("conf.set(\"hbase.zookeeper.property.clientPort\", \"2181\");");
        postStartupInfo.add("");

        postStartupInfo.add("-------------------------");
        postStartupInfo.add("MapReduce is running");
        postStartupInfo.add("");
        postStartupInfo.add("JobTracker web ui: http://localhost:" +
                hbaseTestUtility.getMRCluster().getJobTrackerRunner().getJobTrackerInfoPort());
        postStartupInfo.add("");
        postStartupInfo.add("Configuration conf = new Configuration();");
        postStartupInfo.add("conf.set(\"mapred.job.tracker\", \"localhost:" +
                hbaseTestUtility.getMRCluster().getJobTrackerPort() + "\");");
        postStartupInfo.add("Job job = new Job(conf);");
        postStartupInfo.add("");

        return 0;
    }

    @Override
    public void stop() {
        if (hbaseTestUtility != null) {
            try {
                hbaseTestUtility.shutdownMiniHBaseCluster();
            } catch (Throwable t) {
                log.error("Error shutting down MiniHBaseCluster", t);
            }

            try {
                hbaseTestUtility.shutdownMiniMapReduceCluster();
            } catch (Throwable t) {
                log.error("Error shutting down MiniMapReduceCluster", t);
            }

            try {
                hbaseTestUtility.shutdownMiniDFSCluster();
            } catch (Throwable t) {
                log.error("Error shutting down MiniDFSCluster", t);
            }

            try {
                hbaseTestUtility.shutdownMiniZKCluster();
            } catch (Throwable t) {
                log.error("Error shutting down MiniZKCluster", t);
            }
        }
    }

    public Configuration getConf() {
        return conf;
    }
}
