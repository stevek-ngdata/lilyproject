/*
 * Copyright 2007 Outerthought bvba and Schaubroeck nv
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
package org.lilyproject.runtime.cli;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.text.DateFormat;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.LogManager;

import javax.management.JMException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.runtime.LilyRTException;
import org.lilyproject.runtime.LilyRuntime;
import org.lilyproject.runtime.LilyRuntimeSettings;
import org.lilyproject.runtime.configuration.ConfManagerImpl;
import org.lilyproject.runtime.rapi.Mode;
import org.lilyproject.runtime.model.*;
import org.lilyproject.runtime.repository.ArtifactRepository;
import org.lilyproject.runtime.repository.ChainedMaven2StyleArtifactRepository;
import org.lilyproject.runtime.repository.Maven2StyleArtifactRepository;
import org.lilyproject.util.xml.SimpleNamespaceContext;
import org.lilyproject.util.io.IOUtils;
import org.slf4j.bridge.SLF4JBridgeHandler;
import org.w3c.dom.Document;

@SuppressWarnings({"AccessStaticViaInstance"})
public class LilyRuntimeCli {
    protected final Log infolog = LogFactory.getLog(LilyRuntime.INFO_LOG_CATEGORY);
    private static String DEFAULT_CONF_DIR = "conf";

    public static void main(String[] args) throws Exception {
        new LilyRuntimeCli().run(args);
    }

    private LilyRuntimeCli() {

    }

    private void run(String[] args) throws Exception {
        // Forward JDK logging to SLF4J
        LogManager.getLogManager().reset();
        LogManager.getLogManager().getLogger("").addHandler(new SLF4JBridgeHandler());
        LogManager.getLogManager().getLogger("").setLevel(Level.ALL);

        Options cliOptions = new Options();

        Option confDirsOption = OptionBuilder
        .withArgName("confdir")
        .hasArg()
        .withDescription("The Lily runtime configuration directory. Can be multiple paths separated by " +
                File.pathSeparator)
        .withLongOpt("confdir")
        .create('c');
        cliOptions.addOption(confDirsOption);

        Option repositoryLocationOption = OptionBuilder
                .withArgName("maven-repo-path")
                .hasArg()
                .withDescription("Location of the (Maven-style) artifact repository. Use comma-separated entries to " +
                        "specify multiple locations which will be searched in the order as specified.")
                .withLongOpt("repository")
                .create('r');
        cliOptions.addOption(repositoryLocationOption);

        Option disabledModulesOption = OptionBuilder
                .withArgName("mod-id1,mod-id2,...")
                .hasArg()
                .withDescription("Comma-separated list of modules that should be disabled.")
                .withLongOpt("disable-modules")
                .create('i');
        cliOptions.addOption(disabledModulesOption);

        Option disableClassSharingOption = OptionBuilder
                .withDescription("Disable optional sharing of classes between modules")
                .withLongOpt("disable-class-sharing")
                .create('d');
        cliOptions.addOption(disableClassSharingOption);

        Option consoleLoggingOption = OptionBuilder
                .withArgName("loglevel")
                .hasArg()
                .withDescription("Enable logging to console for the root log category with specified loglevel " +
                        "(debug, info, warn, error)")
                .withLongOpt("console-logging")
                .create('l');
        cliOptions.addOption(consoleLoggingOption);

        Option consoleLogCatOption = OptionBuilder
                .withArgName("logcategory")
                .hasArg()
                .withDescription("Enable console logging only for this category")
                .withLongOpt("console-log-category")
                .create('m');
        cliOptions.addOption(consoleLogCatOption);

        Option logConfigurationOption = OptionBuilder
                .withArgName("config")
                .hasArg()
                .withDescription("Log4j configuration file (properties or .xml)")
                .withLongOpt("log-configuration")
                .create("o");
        cliOptions.addOption(logConfigurationOption);

        Option classLoadingLoggingOption = OptionBuilder
                .withDescription("Print information about the classloader setup (at startup).")
                .withLongOpt("classloader-log")
                .create("z");
        cliOptions.addOption(classLoadingLoggingOption);

        Option verboseOption = OptionBuilder
                .withDescription("Prints lots of information.")
                .withLongOpt("verbose")
                .create("v");
        cliOptions.addOption(verboseOption);

        Option quietOption = OptionBuilder
                .withDescription("Suppress normal output.")
                .withLongOpt("quiet")
                .create("q");
        cliOptions.addOption(quietOption);

        Option sourceLocationsOption = OptionBuilder
                .withArgName("sourcelocationfile")
                .hasArg()
                .withDescription("Path to property file containing alternate source location directory for artifacts.")
                .withLongOpt("source-locations")
                .create("s");
        cliOptions.addOption(sourceLocationsOption);

        Option modeOption = OptionBuilder
                .withArgName("modename")
                .hasArg()
                .withDescription("The runtime mode: prototype, production")
                .withLongOpt("runtime-mode")
                .create("p");
        cliOptions.addOption(modeOption);
        
        Option versionOption = OptionBuilder
            .withDescription("Don't start the service, only dump the version info string for the module defined with -Dlilyruntime.info.module")
            .withLongOpt("version")
            .create("V");
        cliOptions.addOption(versionOption);      
        
        Option helpOption = new Option("h", "help", false, "Shows help");
        cliOptions.addOption(helpOption);

        CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;
        boolean showHelp = false;
        try {
            cmd = parser.parse(cliOptions, args);
        } catch (ParseException e) {
            showHelp = true;
        }

        if (showHelp || cmd.hasOption(helpOption.getOpt())) {
            printHelp(cliOptions);
            System.exit(1);
        }

        Logging.setupLogging(cmd.hasOption(verboseOption.getOpt()), cmd.hasOption(quietOption.getOpt()),
                cmd.hasOption(classLoadingLoggingOption.getOpt()), cmd.getOptionValue(logConfigurationOption.getOpt()),
                cmd.getOptionValue(consoleLoggingOption.getOpt()), cmd.getOptionValue(consoleLogCatOption.getOpt()));

        try {
            Logging.registerLog4jMBeans();
        } catch (JMException e) {
            infolog.error("Unable to register log4j JMX control", e);
        }

        infolog.info("Starting the Lily Runtime.");
        
        List<File> confDirs = new ArrayList<File>();

        if (!cmd.hasOption(confDirsOption.getOpt())) {
            File confDir = new File(DEFAULT_CONF_DIR).getAbsoluteFile();
            if (!confDir.exists()) {
                System.out.println("Default configuration directory " + DEFAULT_CONF_DIR +
                        " not found in current directory: " + confDir.getAbsolutePath());
                System.out.println("To specify another location, use the -" + confDirsOption.getOpt() + " argument");
                System.exit(1);
            }
            confDirs.add(confDir);
        } else {
            String confPathArg = cmd.getOptionValue(confDirsOption.getOpt());
            String[] confPaths = confPathArg.split(File.pathSeparator);
            for (String confPath : confPaths) {
                confPath = confPath.trim();
                if (confPath.length() == 0)
                    continue;
                File confDir = new File(confPath);
                if (!confDir.exists()) {
                    System.out.println("Specified configuration directory does not exist: " + confDir.getAbsolutePath());
                    System.exit(1);
                }
                confDirs.add(confDir);
            }
        }

        ArtifactRepository artifactRepository;

        if (cmd.hasOption(repositoryLocationOption.getOpt())) {
            artifactRepository = new ChainedMaven2StyleArtifactRepository(cmd.getOptionValue(repositoryLocationOption.getOpt()));
        } else {
            File maven2Repository = findLocalMavenRepository();
            infolog.info("Using local Maven repository at " + maven2Repository.getAbsolutePath());
            artifactRepository = new Maven2StyleArtifactRepository(maven2Repository);
        }


        Set<String> disabledModuleIds = getDisabledModuleIds(cmd.getOptionValue(disabledModulesOption.getOpt()));

        SourceLocations sourceLocations;
        if (cmd.hasOption(sourceLocationsOption.getOpt())) {
            File file = new File(cmd.getOptionValue(sourceLocationsOption.getOpt())).getAbsoluteFile();
            if (!file.exists()) {
                System.out.println("The specified source locations property file does not exist: " + file.getAbsolutePath());
                System.exit(1);
            }

            InputStream is = null;
            try {
                is = new FileInputStream(file);
                sourceLocations = new SourceLocations(is, file.getParent());
            } catch (Throwable t) {
                throw new LilyRTException("Problem reading source locations property file.", t);
            } finally {
                IOUtils.closeQuietly(is);
            }
        } else {
            sourceLocations = new SourceLocations();
        }



        LilyRuntimeSettings settings = new LilyRuntimeSettings();
        settings.setConfManager(new ConfManagerImpl(confDirs));
        settings.setDisabledModuleIds(disabledModuleIds);
        settings.setRepository(artifactRepository);
        settings.setSourceLocations(sourceLocations);
        settings.setEnableArtifactSharing(!cmd.hasOption(disableClassSharingOption.getOpt()));

        LilyRuntime runtime = new LilyRuntime(settings);

        if (cmd.hasOption(modeOption.getOpt())) {
            String optionValue = cmd.getOptionValue(modeOption.getOpt());
            Mode mode = Mode.byName(optionValue);
            runtime.setMode(mode);
        }
        
        if (cmd.hasOption(versionOption.getOpt())) {
            System.out.println(runtime.buildModel().moduleInfo(System.getProperty("lilyruntime.info.module")));
            System.exit(0);
        }

        try {
            runtime.start();
            Runtime.getRuntime().addShutdownHook(new Thread(new ShutdownHandler(runtime)));
            printStartedMessage();
        } catch (Throwable e) {
            e.printStackTrace();
            System.err.println("Startup failed. Will try to shutdown and exit.");
            try {
                runtime.stop();
            } finally {
                System.exit(1);
            }
        }

    }

    private void printStartedMessage() {
        DateFormat dateFormat = DateFormat.getDateTimeInstance(DateFormat.LONG, DateFormat.LONG);
        String now = dateFormat.format(new Date());
        infolog.info("Lily Runtime started [" + now + "]");
    }

    private void printHelp(Options cliOptions) {
        HelpFormatter help = new HelpFormatter();
        help.printHelp("lily-runtime", cliOptions, true);
    }

    private Set<String> getDisabledModuleIds(String spec) {
        if (spec == null)
            return Collections.emptySet();

        Set<String> ids = new HashSet<String>();

        String[] items = spec.split(",");
        for (String item : items) {
            item = item.trim();
            if (item.length() > 0)
                ids.add(item);
        }

        return ids;
    }

    // This method is duplicated in LilyRuntimeCliLauncher, so if you modify it
    // here, it is likely useful to copy you modifications there too.
    private File findLocalMavenRepository() {
        String homeDir = System.getProperty("user.home");
        File mavenSettingsFile = new File(homeDir + "/.m2/settings.xml");
        if (mavenSettingsFile.exists()) {
            try {
                DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
                dbf.setNamespaceAware(true);
                DocumentBuilder db = dbf.newDocumentBuilder();
                Document document = db.parse(mavenSettingsFile);
                XPath xpath = XPathFactory.newInstance().newXPath();
                SimpleNamespaceContext nc = new SimpleNamespaceContext();
                nc.addPrefix("m", "http://maven.apache.org/POM/4.0.0");
                xpath.setNamespaceContext(nc);

                String localRepository = xpath.evaluate("string(/m:settings/m:localRepository)", document);
                if (localRepository != null && localRepository.length() > 0) {
                    return new File(localRepository);
                }

                // Usage of the POM namespace in settings.xml is optional, so also try without namespace
                localRepository = xpath.evaluate("string(/settings/localRepository)", document);
                if (localRepository != null && localRepository.length() > 0) {
                    return new File(localRepository);
                }
            } catch (Exception e) {
                System.err.println("Error reading Maven settings file at " + mavenSettingsFile.getAbsolutePath());
                e.printStackTrace();
                System.exit(1);
            }
        }
        return new File(homeDir + "/.m2/repository");
    }

    public static class ShutdownHandler implements Runnable {
        private final LilyRuntime runtime;

        public ShutdownHandler(LilyRuntime runtime) {
            this.runtime = runtime;
        }

        public void run() {
            runtime.stop();
        }
    }
}
