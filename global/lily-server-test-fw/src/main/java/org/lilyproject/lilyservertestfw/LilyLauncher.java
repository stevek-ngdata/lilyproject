package org.lilyproject.lilyservertestfw;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.lilyproject.cli.BaseCliTool;
import org.lilyproject.testfw.HBaseTestingUtilityFactory;
import org.lilyproject.testfw.TestHelper;
import org.lilyproject.testfw.TestHomeUtil;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class LilyLauncher extends BaseCliTool {
    private Option enableHadoopOption;
    private Option enableSolrOption;
    private Option enableLilyOption;

    /** Informational messages to be outputted after startup has completed. */
    private List<String> postStartupInfo = new ArrayList<String>();

    private File testHome;
    private Configuration conf;

    @Override
    protected String getCmdName() {
        return "launch-test-lily";
    }

    @Override
    public List<Option> getOptions() {
        List<Option> options = super.getOptions();

        enableHadoopOption = OptionBuilder
                .withDescription("Start Hadoop (DFS/MR/ZK/HBase)")
                .withLongOpt("hadoop")
                .create("hadoop");
        options.add(enableHadoopOption);

        enableSolrOption = OptionBuilder
                .withDescription("Start Solr")
                .withLongOpt("solr")
                .create("solr");
        options.add(enableSolrOption);

        enableLilyOption = OptionBuilder
                .withDescription("Start Lily")
                .withLongOpt("lily")
                .create("lily");
        options.add(enableLilyOption);

        return options;
    }

    public static void main(String[] args) throws Exception {
        new LilyLauncher().start(args);
    }


    @Override
    public int run(CommandLine cmd) throws Exception {
        int result = super.run(cmd);
        if (result != 0)
            return result;

        //
        // Figure out what to start
        //
        boolean enableHadoop = cmd.hasOption(enableHadoopOption.getOpt());
        boolean enableSolr = cmd.hasOption(enableSolrOption.getOpt());
        boolean enableLily = cmd.hasOption(enableLilyOption.getOpt());

        // If none of the services are explicitly enabled, we default to starting them all. Otherwise
        // we only start those that are enabled.
        if (!enableHadoop && !enableSolr && !enableLily) {
            enableHadoop = true;
            enableSolr = true;
            enableLily = true;
        }

        //
        //
        //
        testHome = TestHomeUtil.createTestHome();

        TestHelper.setupConsoleLogging("INFO");
        TestHelper.setupOtherDefaults();

        //
        // Start the services
        //
        if (enableHadoop) {
            startHadoop();
        }

        if (enableSolr) {
            startSolr();
        }

        if (enableLily) {
            startLily();
        }


        //
        // Finished startup, print messages
        //
        for (String msg : postStartupInfo) {
            System.out.println(msg);
        }

        return 0;
    }

    private void startHadoop() throws Exception{
        conf = HBaseConfiguration.create();

        org.lilyproject.testfw.fork.HBaseTestingUtility testUtil = HBaseTestingUtilityFactory.create(conf, testHome);
        testUtil.startMiniCluster(1);
        testUtil.startMiniMapReduceCluster(1);

        postStartupInfo.add("-------------------------");
        postStartupInfo.add("HDFS is running");
        postStartupInfo.add("");
        postStartupInfo.add("HDFS web ui: http://" + conf.get("dfs.http.address"));
        postStartupInfo.add("");

        postStartupInfo.add("-------------------------");
        postStartupInfo.add("HBase is running");
        postStartupInfo.add("");
        postStartupInfo.add("HBase master web ui: http://localhost:" + testUtil.getHBaseCluster().getMaster().getInfoServer().getPort());
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
        postStartupInfo.add("JobTracker web ui: http://localhost:" + testUtil.getMRCluster().getJobTrackerRunner().getJobTrackerInfoPort());
        postStartupInfo.add("");
        postStartupInfo.add("Configuration conf = new Configuration();");
        postStartupInfo.add("conf.set(\"mapred.job.tracker\", \"localhost:" + testUtil.getMRCluster().getJobTrackerPort() + "\");");
        postStartupInfo.add("Job job = new Job(conf);");
        postStartupInfo.add("");
    }

    private void startSolr() {

    }

    private void startLily() {

    }

}

