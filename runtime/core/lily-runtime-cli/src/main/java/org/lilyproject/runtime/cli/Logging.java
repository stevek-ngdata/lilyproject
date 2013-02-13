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

import org.apache.log4j.*;
import org.apache.log4j.jmx.HierarchyDynamicMBean;
import org.apache.log4j.spi.LoggerRepository;
import org.apache.log4j.xml.DOMConfigurator;
import org.lilyproject.runtime.KauriRuntime;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.Enumeration;

public class Logging {

    public static void setupLogging(boolean verbose, boolean quiet, boolean classLoadingLog, String logConfLocation,
            String consoleLoggingLevel, String consoleLogCategory) {

        ConsoleAppender consoleAppender = new ConsoleAppender();
        consoleAppender.setName("console appender");
        consoleAppender.setLayout(new PatternLayout("[%-5p][%d{ABSOLUTE}][%-10.10t] %c - %m%n"));
        consoleAppender.activateOptions();

        // Since we set up logging very early on, it is not expected that log4j will already have loaded
        // something. But just to avoid possible double configuration, reset the configuration.
        LogManager.resetConfiguration();

        // If the user did not specify a configuration file, default to log4j.properties in the working dir
        if (logConfLocation == null) {
            File defaultConf = new File("log4j.properties");
            if (defaultConf.exists()) {
                logConfLocation = defaultConf.getAbsolutePath();
                System.out.println("Using log4j.properties from working directory: " + logConfLocation);
            }
        }

        boolean hasConsoleAppender = false;

        if (logConfLocation != null) {
            if (logConfLocation.endsWith(".xml")) {
                DOMConfigurator.configure(logConfLocation);
            } else {
                PropertyConfigurator.configure(logConfLocation);
            }

            // Check if the user's log configuration is already logging to the console, in order to avoid
            // double logging in case the user would also use the console logging options (see below)
            for (Enumeration it = Logger.getRootLogger().getAllAppenders(); it.hasMoreElements(); ) {
                Appender appender = (Appender)it.nextElement();
                if (appender instanceof ConsoleAppender) {
                    hasConsoleAppender = true;
                }
            }

        } else {
            // If there is no log configuration specified, and console logging is not enabled,
            // then default to printing warn and higher messages to the console
            System.out.println("Note: it is recommended to specify a log configuration. Will print error logs to the console.");
            Logger logger = Logger.getRootLogger();
            logger.setLevel(Level.WARN);
            logger.addAppender(consoleAppender);
            hasConsoleAppender = true;
        }

        if (consoleLoggingLevel != null) {
            Level level = null;
            if (consoleLoggingLevel.equalsIgnoreCase("trace"))
                level = Level.TRACE;
            else if (consoleLoggingLevel.equalsIgnoreCase("debug"))
                level = Level.DEBUG;
            else if (consoleLoggingLevel.equalsIgnoreCase("info"))
                level = Level.INFO;
            else if (consoleLoggingLevel.equalsIgnoreCase("warn"))
                level = Level.WARN;
            else if (consoleLoggingLevel.equalsIgnoreCase("error"))
                level = Level.ERROR;
            else if (consoleLoggingLevel.equalsIgnoreCase("fatal"))
                level = Level.FATAL;
            else
                System.err.println("Unrecognized log level: " + consoleLoggingLevel);

            if (level != null) {
                System.out.println("Setting console output for log level " + level.toString() + " on category " + consoleLogCategory);
                Logger logger = consoleLogCategory == null ? Logger.getRootLogger() : Logger.getLogger(consoleLogCategory);
                logger.setLevel(level);

                if (!hasConsoleAppender) {
                    Logger rootLogger = Logger.getRootLogger();
                    rootLogger.addAppender(consoleAppender);
                    hasConsoleAppender = true;
                }
            }
        }

        if (quiet) {
            Logger logger = Logger.getLogger("org.lilyproject.runtime");
            logger.setLevel(Level.ERROR);
            return;
        }

        if (verbose) {
            Logger logger = Logger.getLogger("org.lilyproject.runtime");
            logger.setLevel(Level.DEBUG);
            logger.addAppender(consoleAppender);
            return;
        }

        Logger logger = Logger.getLogger(KauriRuntime.INFO_LOG_CATEGORY);
        logger.setLevel(Level.INFO);
        if (!hasConsoleAppender)
            logger.addAppender(consoleAppender);

        if (classLoadingLog) {
            logger = Logger.getLogger(KauriRuntime.CLASSLOADING_LOG_CATEGORY);
            logger.setLevel(Level.INFO);
            if (!hasConsoleAppender)
                logger.addAppender(consoleAppender);

            logger = Logger.getLogger(KauriRuntime.CLASSLOADING_REPORT_CATEGORY);
            logger.setLevel(Level.INFO);
            if (!hasConsoleAppender)
                logger.addAppender(consoleAppender);
        } else {
            // Always print classloader warnings
            logger = Logger.getLogger(KauriRuntime.CLASSLOADING_LOG_CATEGORY);
            logger.setLevel(Level.WARN);
            if (!hasConsoleAppender)
                logger.addAppender(consoleAppender);
        }
    }

    /**
     * This method was copied from Apache ZooKeeper, but it is a pretty common snippet
     * that can be found in many places.
     *
     * Register the log4j JMX mbeans. Set environment variable
     * "kauri.jmx.log4j.disable" to true to disable registration.
     * http://logging.apache.org/log4j/1.2/apidocs/index.html?org/apache/log4j/jmx/package-summary.html
     * @throws javax.management.JMException if registration fails
     */
    public static void registerLog4jMBeans() throws JMException {
        if (Boolean.getBoolean("kauri.jmx.log4j.disable")) {
            return;
        }

        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

        // Create and Register the top level Log4J MBean
        HierarchyDynamicMBean hdm = new HierarchyDynamicMBean();

        ObjectName mbo = new ObjectName("log4j:hierarchy=default");
        mbs.registerMBean(hdm, mbo);

        // Add the root logger to the Hierarchy MBean
        Logger rootLogger = Logger.getRootLogger();
        hdm.addLoggerMBean(rootLogger.getName());

        // Get each logger from the Log4J Repository and add it to
        // the Hierarchy MBean created above.
        LoggerRepository r = LogManager.getLoggerRepository();
        Enumeration enumer = r.getCurrentLoggers();
        Logger logger;

        while (enumer.hasMoreElements()) {
            logger = (Logger) enumer.nextElement();
            hdm.addLoggerMBean(logger.getName());
        }
    }
}
