package org.lilyproject.lilyservertestfw.launcher;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.util.ReflectionUtils;
import org.lilyproject.cli.BaseCliTool;
import org.lilyproject.hadooptestfw.CleanupUtil;
import org.lilyproject.hadooptestfw.TestHelper;
import org.lilyproject.util.test.TestHomeUtil;

import javax.management.ObjectName;
import java.io.*;
import java.lang.management.ManagementFactory;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class LilyLauncher extends BaseCliTool implements LilyLauncherMBean {
    private Option enableHadoopOption;
    private Option enableSolrOption;
    private Option enableLilyOption;
    private Option dataDirOption;

    private File testHome;
    private boolean clearData = true;

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

        dataDirOption = OptionBuilder
                .withDescription("Directory where data should be stored, instead of a temporary directory. " +
                        "Can be used to restart from previous state.")
                .hasArg()
                .withArgName("path")
                .withLongOpt("data-dir")
                .create("d");
        options.add(dataDirOption);

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
        // Determine directory below which all services will store their data
        //
        if (cmd.hasOption(dataDirOption.getOpt())) {
            String dataDir = cmd.getOptionValue(dataDirOption.getOpt());
            testHome = new File(dataDir);
            if (testHome.exists() && !testHome.isDirectory()) {
                System.err.println("Specified data directory exists and is not a directory:");
                System.err.println(testHome.getAbsolutePath());
                return -1;
            } else if (testHome.exists() && testHome.isDirectory() && testHome.list().length > 0) {
                System.out.println("Specified data directory exists: will re-use data from previous run!");
            } else if (!testHome.exists()) {
                FileUtils.forceMkdir(testHome);
            }
            // If the user specified the storage directory, do not delete it
            clearData = false;
        } else {
            testHome = TestHomeUtil.createTestHome("lily-launcher-");
        }

        //
        // Setup logging
        //
        TestHelper.setupConsoleLogging("WARN");
        TestHelper.setupOtherDefaults();

        //
        // Start the services
        //

        // Informational messages to be outputted after startup has completed.
        List<String> postStartupInfo = new ArrayList<String>();

        for (LauncherService service : enabledServices) {
            if ((result = service.setup(cmd, testHome, clearData)) != 0)
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
                if (clearData) {
                    System.out.println("Deleting " + testHome.getPath());
                    TestHomeUtil.cleanupTestHome(testHome);
                }

                System.out.println("Bye!");
            } catch (Throwable t) {
                log.info("Error in " + getCmdName() + " shutdown hook", t);
            }
        }
    }

    private AtomicBoolean resetRunning = new AtomicBoolean();

    public void resetLilyState() {
        Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
        
        if (!resetRunning.compareAndSet(false, true)) {
            throw new RuntimeException("There is already a Lily state reset running.");
        }

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
            HConnectionManager.deleteConnection(hadoopService.getConf(), true);

            // Clear Lily ZooKeeper state
            System.out.println("Clearing Lily's ZooKeeper state");
            cleanupUtil.cleanZooKeeper();

            // Clear Blobs on hdfs blobstore
            String dfsUri = "hdfs://localhost:8020/lily/blobs";
            System.out.println("Clearing HDFS Blob Store on " + dfsUri);
            cleanupUtil.cleanBlobStore(new URI(dfsUri));

            // Clear Solr state
            int response = sendSolrUpdateRequest("<update><delete><query>*:*</query></delete><commit/></update>");
            if (response != 200) {
                throw new RuntimeException("Solr delete all docs: expected 200 status but it is " + response);
            }

            // The following is useful to observer what threads were not stopped properly after stopping Lily
            if (System.getProperty("lily.launcher.threaddump-after-lily-stop") != null) {
                ReflectionUtils.printThreadInfo(new PrintWriter("launcher-threadump-after-lily-stop"), "Thread dump");
            }

            // Start Lily
            System.out.println("Starting Lily");
            lilyService.start(new ArrayList<String>());

            System.out.println("Reset Lily state took " + (System.currentTimeMillis() - before) + " ms");
        } catch (Exception e) {
            System.out.println("Error while resetting Lily state: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Error while resetting Lily state: " + e.getMessage(), e);
        } finally {
            resetRunning.set(false);
        }
    }

    private int sendSolrUpdateRequest(String request) throws IOException {
        URL url = new URL("http://localhost:8983/solr/update");
        HttpURLConnection conn = (HttpURLConnection)url.openConnection();
        conn.setDoOutput(true);
        conn.setRequestProperty("Content-Type", "text/xml");
        OutputStream os = conn.getOutputStream();
        os.write(request.getBytes("UTF-8"));
        os.close();
        int response = conn.getResponseCode();
        conn.disconnect();
        return response;
    }
}

