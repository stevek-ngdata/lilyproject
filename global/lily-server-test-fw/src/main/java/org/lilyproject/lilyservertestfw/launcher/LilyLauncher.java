package org.lilyproject.lilyservertestfw.launcher;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.cli.BaseCliTool;
import org.lilyproject.testfw.TestHelper;
import org.lilyproject.testfw.TestHomeUtil;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class LilyLauncher extends BaseCliTool {
    private Option enableHadoopOption;
    private Option enableSolrOption;
    private Option enableLilyOption;

    private File testHome;

    private LauncherService hadoopService = new HadoopLauncherService();
    private LauncherService solrService = new SolrLauncherService();
    private LauncherService lilyService = new LilyLauncherService();

    private List<LauncherService> allServices = new ArrayList<LauncherService>();
    private List<LauncherService> enabledServices = new ArrayList<LauncherService>();

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

        if (enableHadoop)
            enabledServices.add(hadoopService);
        if (enableSolr)
            enabledServices.add(solrService);
        if (enableLily)
            enabledServices.add(lilyService);

        //
        //
        //
        testHome = TestHomeUtil.createTestHome();

        TestHelper.setupConsoleLogging("INFO");
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
}

