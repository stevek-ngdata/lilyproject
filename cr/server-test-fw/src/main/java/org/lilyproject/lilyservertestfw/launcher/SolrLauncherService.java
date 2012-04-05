package org.lilyproject.lilyservertestfw.launcher;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.lilyservertestfw.TemplateDir;
import org.lilyproject.solrtestfw.SolrTestingUtility;
import org.lilyproject.util.xml.DocumentHelper;
import org.w3c.dom.Document;

import java.io.File;
import java.util.List;

public class SolrLauncherService implements LauncherService {
    private Option schemaOption;
    private Option commitOption;

    private String autoCommitSetting;
    private String schema;
    private File testHome;
    private boolean clearData;

    private SolrTestingUtility solrTestingUtility;
    private int autoCommitTime = -1;

    private Log log = LogFactory.getLog(getClass());

    @Override
    public void addOptions(List<Option> options) {
        schemaOption = OptionBuilder
                .withArgName("schema.xml")
                .hasArg()
                .withDescription("Solr schema file name")
                .withLongOpt("schema")
                .create("s");
        options.add(schemaOption);

        commitOption = OptionBuilder
                .withArgName("seconds")
                .hasArg()
                .withDescription("Auto commit index within this amount of seconds (default: no auto commit)")
                .withLongOpt("commit")
                .create("c");
        options.add(commitOption);
    }

    @Override
    public int setup(CommandLine cmd, File testHome, boolean clearData) throws Exception {
        this.testHome = new File(testHome, TemplateDir.SOLR_DIR);
        FileUtils.forceMkdir(testHome);
        this.clearData = clearData;

        schema = cmd.getOptionValue(schemaOption.getOpt());
        if (schema != null) {
            int result = checkSolrSchema(schema);
            if (result != 0)
                return result;
        }

        autoCommitSetting = "";
        if (cmd.hasOption(commitOption.getOpt())) {
            try {
                autoCommitTime = Integer.parseInt(cmd.getOptionValue(commitOption.getOpt()));
                autoCommitSetting = "<autoCommit><maxTime>" + (autoCommitTime * 1000) + "</maxTime></autoCommit>";
            } catch (NumberFormatException e) {
                System.err.println("commit option should specify an integer, not: " + cmd.getOptionValue(commitOption.getOpt()));
                return 1;
            }
        }

        return 0;
    }

    private int checkSolrSchema(String schema) {
        File schemaFile = new File(schema);
        if (!schemaFile.exists()) {
            System.err.println("Specified Solr schema file does not exist:");
            System.err.println(schemaFile.getAbsolutePath());
            return 1;
        }

        Document document;
        try {
            document = DocumentHelper.parse(schemaFile);
        } catch (Exception e) {
            System.err.println("Error reading or parsing Solr schema file.");
            System.err.println();
            e.printStackTrace();
            return 1;
        }

        if (!document.getDocumentElement().getLocalName().equals("schema")) {
            System.err.println("A Solr schema file should have a <schema> root element, which the following file");
            System.err.println("has not:");
            System.err.println(schemaFile.getAbsolutePath());
            return 1;
        }

        return 0;
    }

    @Override
    public int start(List<String> postStartupInfo) throws Exception {
        solrTestingUtility = new SolrTestingUtility(testHome, clearData);
        solrTestingUtility.setAutoCommitSetting(autoCommitSetting);
        byte[] schemaData = schema == null ? null : FileUtils.readFileToByteArray(new File(schema));
        solrTestingUtility.setSchemaData(schemaData);

        solrTestingUtility.start();

        postStartupInfo.add("-----------------------------------------------");
        postStartupInfo.add("Solr is running");
        postStartupInfo.add("");
        postStartupInfo.add("Use this as Solr URL when creating an index:");
        postStartupInfo.add("http://localhost:8983/solr");
        postStartupInfo.add("");
        postStartupInfo.add("Web GUI available at:");
        postStartupInfo.add("http://localhost:8983/solr/admin/");
        postStartupInfo.add("");
        if (autoCommitTime == -1) {
            postStartupInfo.add("Index is not auto-committed, you can commit it using:");
            postStartupInfo.add("curl http://localhost:8983/solr/update -H 'Content-type:text/xml' --data-binary '<commit/>'");
        } else {
            postStartupInfo.add("Index auto commit: " + autoCommitTime + " seconds");
        }
        postStartupInfo.add("");

        return 0;
    }

    @Override
    public void stop() {
        if (solrTestingUtility != null) {
            if (solrTestingUtility.getServer() != null) {
                try {
                    solrTestingUtility.getServer().stop();
                } catch (Throwable t) {
                    log.error("Error shutting down Solr/Jetty", t);
                }
            }
            solrTestingUtility = null;
        }
    }

    public SolrTestingUtility getSolrTestingUtility() {
        return solrTestingUtility;
    }
}
