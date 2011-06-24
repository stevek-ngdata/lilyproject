package org.lilyproject.lilyservertestfw.launcher;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.cli.BaseCliTool;
import org.lilyproject.testfw.CleanupUtil;
import org.lilyproject.testfw.TestHelper;
import org.lilyproject.util.test.TestHomeUtil;

import javax.management.ObjectName;
import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;

public class LilyLauncher extends BaseCliTool implements LilyLauncherMBean {
    private Option enableHadoopOption;
    private Option enableSolrOption;
    private Option enableLilyOption;

    private File testHome;

    private HadoopLauncherService hadoopService = new HadoopLauncherService();
    private SolrLauncherService solrService = new SolrLauncherService();
    private LilyLauncherService lilyService = new LilyLauncherService();

    private List<LauncherService> allServices = new ArrayList<LauncherService>();
    private List<LauncherService> enabledServices = new ArrayList<LauncherService>();

    boolean enableHadoop;
    boolean enableSolr;
    boolean enableLily;

    private Log log = LogFactory.getLog(getClass());

    public LilyLauncher() {
        allServices.add(solrService);
        allServices.add(hadoopService);
        allServices.add(lilyService);
    }

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

        
        for (LauncherService service : allServices) {
            service.addOptions(options);
        }
        
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
        enableHadoop = cmd.hasOption(enableHadoopOption.getOpt());
        enableSolr = cmd.hasOption(enableSolrOption.getOpt());
        enableLily = cmd.hasOption(enableLilyOption.getOpt());

        // If none of the services are explicitly enabled, we default to starting them all. Otherwise
        // we only start those that are enabled.
        if (!enableHadoop && !enableSolr && !enableLily) {
            enableHadoop = true;
            enableSolr = true;
            enableLily = true;
        }

        if (enableHadoop)
            enabledServices.add(hadoopService);
        if (enableSolr)
            enabledServices.add(solrService);
        if (enableLily)
            enabledServices.add(lilyService);

        //
        //
        //
        testHome = TestHomeUtil.createTestHome("lily-launcher-");

        TestHelper.setupConsoleLogging("WARN");
        TestHelper.setupOtherDefaults();

        //
        // Start the services
        //

        // Informational messages to be outputted after startup has completed.
        List<String> postStartupInfo = new ArrayList<String>();

        for (LauncherService service : enabledServices) {
            if ((result = service.setup(cmd, testHome)) != 0)
                return result;
        }

        for (LauncherService service : enabledServices) {
            if ((result = service.start(postStartupInfo)) != 0)
                return result;
        }

        // Register MBean
        ManagementFactory.getPlatformMBeanServer().registerMBean(this, new ObjectName("LilyLauncher:name=Launcher"));

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(new ShutdownHook()));

        //
        // Finished startup, print messages
        //
        for (String msg : postStartupInfo) {
            System.out.println(msg);
        }

        return 0;
    }

    private class ShutdownHook implements Runnable {
        @Override
        public void run() {
            try {
                //
                // Attempt to shutdown everything
                //
                lilyService.stop();

                solrService.stop();

                hadoopService.stop();

                //
                // Cleanup temp data dir
                //
                System.out.println("Deleting " + testHome.getPath());
                TestHomeUtil.cleanupTestHome(testHome);

                System.out.println("Bye!");
            } catch (Throwable t) {
                log.info("Error in " + getCmdName() + " shutdown hook", t);
            }
        }
    }

    public void resetLilyState() {
        // TODO should make this synchronized or maybe better check against concurrent calling
        try {
            long before = System.currentTimeMillis();
            if (!enableLily || !enableHadoop || !enableSolr) {
                throw new Exception("resetLilyState is only supported when all services are running");
            }

            // Stop Lily
            System.out.println("Stopping Lily");
            lilyService.stop();
            System.out.println("Lily stopped");

            // Clear HBase tables
            System.out.println("Clearing HBase tables");
            CleanupUtil cleanupUtil = new CleanupUtil(hadoopService.getConf(), "localhost:2181");
            cleanupUtil.cleanTables();

            // Clear Lily ZooKeeper state
            System.out.println("Clearing Lily's ZooKeeper state");
            cleanupUtil.cleanZooKeeper();

            // TODO: clean blob store directory, what else?

            // TODO: clear Solr state

            // Start Lily
            System.out.println("Starting Lily");
            lilyService.start(new ArrayList<String>());

            System.out.println("Reset Lily state took " + (System.currentTimeMillis() - before) + " ms");
        } catch (Exception e) {
            System.out.println("Error while resetting Lily state: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Error while resetting Lily state: " + e.getMessage());
        }
    }
}

