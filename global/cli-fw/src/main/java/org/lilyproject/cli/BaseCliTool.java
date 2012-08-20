/*
 * Copyright 2010 Outerthought bvba
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
package org.lilyproject.cli;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.xml.DOMConfigurator;
import org.lilyproject.util.exception.StackTracePrinter;

/**
 * Base framework for Lily CLI tools. Purpose is to have some uniformity in the CLI tools and to avoid
 * code duplication.
 *
 * Subclasses can override:
 * <ul>
 * <li>{@link #getCmdName()}
 * <li>{@link #getOptions()}
 * <li>{@link #processOptions(org.apache.commons.cli.CommandLine)}
 * <li>{@link #run(org.apache.commons.cli.CommandLine)}
 * <li>{@link #reportThrowable(Throwable)}
 * </ul>
 */
public abstract class BaseCliTool {
    protected Option helpOption;
    protected Option versionOption;
    protected Option logConfOption;
    protected Option dumpLogConfOption;
    private Options cliOptions;

    protected void start(String[] args) {
        int result = 1;
        try {
            System.out.println();
            result = runBase(args);
        } catch (Throwable t) {
            reportThrowable(t);
        } finally {
            try {
                cleanup();
            } catch (Throwable t) {
                System.err.println("Error during cleanup:");
                t.printStackTrace();
            }
        }
        System.out.println();

        if (result != 0)
            System.exit(result);
    }

    protected void reportThrowable(Throwable throwable) {
        StackTracePrinter.printStackTrace(throwable);
    }

    /**
     * Return the CLI options. When overriding, call super and add your own
     * options.
     */
    public List<Option> getOptions() {
        List<Option> options = new ArrayList<Option>();

        helpOption = new Option("h", "help", false, "Shows help");
        options.add(helpOption);

        versionOption = new Option("v", "version", false, "Shows the version");
        options.add(versionOption);

        logConfOption = OptionBuilder
                .withArgName("config")
                .hasArg()
                .withDescription("log4j config file (.properties or .xml)")
                .create("log");
        options.add(logConfOption);

        dumpLogConfOption = OptionBuilder
                .withDescription("Dump default log4j configuration")
                .create("dumplog");
        options.add(dumpLogConfOption);

        return options;
    }

    /**
     * The name of this CLI tool, used in the help message.
     */
    protected abstract String getCmdName();

    /**
     * Override this to perform cleanup after the tool has run, also in case
     * an exception occurred (= called from a finally block).
     */
    protected void cleanup() {

    }

    /**
     * Process option values, typically this performs basic stuff like reading
     * the option value and validating it. First always call super, if non-zero
     * is returned, then return this value immediately.
     */
    protected int processOptions(CommandLine cmd) throws Exception {
        if (cmd.hasOption(helpOption.getOpt())) {
            printHelp();
            return 1;
        }

        if (cmd.hasOption(versionOption.getOpt())) {
            System.out.println(getVersion());
            return 1;
        }

        if (cmd.hasOption(dumpLogConfOption.getOpt())) {
            IOUtils.copy(BaseCliTool.class.getResourceAsStream("log4j.properties"), System.out);
            return 1;
        }

        File logConfFile = null;
        if (cmd.hasOption(logConfOption.getOpt())) {
            logConfFile = new File(cmd.getOptionValue(logConfOption.getOpt()));
            if (!logConfFile.exists()) {
                System.err.println("Specified log4j configuration file does not exist:");
                System.err.println(logConfFile);
            }
        }
        setupLogging(logConfFile);

        return 0;
    }

    /**
     * Perform the actual action. First always call super, if non-zero is returned,
     * then return this value immediately.
     */
    public int run(CommandLine cmd) throws Exception {
        return 0;
    }

    private void setupLogging(File logConfFile) {
        // Reset any configuration log4j might already have loaded (from classpath, cwd, ...).
        LogManager.resetConfiguration();

        // If the user did not specify a configuration file, default to log4j.properties in the working dir
        if (logConfFile == null) {
            File defaultConf = new File("log4j.properties");
            if (defaultConf.exists()) {
                logConfFile = defaultConf;
                System.out.println("Using log4j.properties from working directory: " + logConfFile.getAbsolutePath());
            }
        }

        if (logConfFile == null) {
            // Use the built-in config
            PropertyConfigurator.configure(BaseCliTool.class.getResource("log4j.properties"));
        } else if (logConfFile.getName().endsWith(".xml")) {
            DOMConfigurator.configure(logConfFile.getAbsolutePath());
        } else {
            PropertyConfigurator.configure(logConfFile.getAbsolutePath());
        }
    }

    private int runBase(String[] args) throws Exception {
        //
        // Set up options
        //
        cliOptions = new Options();

        for (Option option : getOptions()) {
            cliOptions.addOption(option);
        }

        //
        // Parse options
        //
        CommandLineParser parser = new PosixParser();
        CommandLine cmd;
        try {
            cmd = parser.parse(cliOptions, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            System.out.println();

            printHelp();
            return 1;
        }

        //
        // Process options
        //
        int result = processOptions(cmd);
        if (result != 0)
            return result;


        //
        // Run tool
        //
        return run(cmd);

    }

    protected void printHelp() throws IOException {
        printHelpHeader();
        HelpFormatter help = new HelpFormatter();
        help.printHelp(getCmdName(), cliOptions, true);
        printHelpFooter();
    }

    protected void printHelpHeader() throws IOException {
        String className = getClass().getName();
        String helpHeaderPath = className.replaceAll(Pattern.quote("."), "/") + "_help_header.txt";
        InputStream is = getClass().getClassLoader().getResourceAsStream(helpHeaderPath);
        if (is != null) {
            IOUtils.copy(is, System.out);
            System.out.println();
            System.out.println();
        }
    }

    protected void printHelpFooter() throws IOException {
        String className = getClass().getName();
        String helpHeaderPath = className.replaceAll(Pattern.quote("."), "/") + "_help_footer.txt";
        InputStream is = getClass().getClassLoader().getResourceAsStream(helpHeaderPath);
        if (is != null) {
            System.out.println();
            IOUtils.copy(is, System.out);
            System.out.println();
        }
    }

    protected abstract String getVersion();
}
