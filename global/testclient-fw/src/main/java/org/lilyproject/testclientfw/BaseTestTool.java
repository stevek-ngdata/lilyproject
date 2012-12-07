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
package org.lilyproject.testclientfw;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.joda.time.DateTime;
import org.lilyproject.cli.BaseZkCliTool;
import org.lilyproject.cli.OptionUtil;
import org.lilyproject.clientmetrics.HBaseMetrics;
import org.lilyproject.clientmetrics.HBaseMetricsPlugin;
import org.lilyproject.clientmetrics.LilyMetrics;
import org.lilyproject.clientmetrics.ListMetricsPlugin;
import org.lilyproject.clientmetrics.Metrics;
import org.lilyproject.util.concurrent.WaitPolicy;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.zookeeper.StateWatchingZooKeeper;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

public abstract class BaseTestTool extends BaseZkCliTool {
    private Option workersOption;

    private Option verboseOption;

    private Option hbaseMetricsOption;

    private Option lilyMetricsOption;

    protected int workers;

    protected boolean verbose;

    protected boolean useHbaseMetrics;

    protected boolean useLilyMetrics;

    protected HBaseMetrics hbaseMetrics;

    private LilyMetrics lilyMetrics;

    protected PrintStream metricsStream;

    protected ThreadPoolExecutor executor;

    protected Metrics metrics;

    private ZooKeeperItf zk;

    private Configuration hbaseConf;

    protected int getDefaultWorkers() {
        return 2;
    }

    @Override
    public List<Option> getOptions() {
        List<Option> options = super.getOptions();

        workersOption = OptionBuilder
                .withArgName("count")
                .hasArg()
                .withDescription("Number of workers (threads)")
                .withLongOpt("workers")
                .create("w");
        options.add(workersOption);

        verboseOption = OptionBuilder
                .withDescription("Be verbose")
                .withLongOpt("verbose")
                .create("vb");
        options.add(verboseOption);

        hbaseMetricsOption = OptionBuilder
                .withDescription("Enable HBase metrics options (requires JMX on default port 10102)")
                .withLongOpt("hbase-metrics")
                .create("hm");
        options.add(hbaseMetricsOption);

        lilyMetricsOption = OptionBuilder
                .withDescription("Enable Lily metrics options (requires JMX on default port 10202)")
                .withLongOpt("lily-metrics")
                .create("lm");
        options.add(lilyMetricsOption);

        return options;
    }

    @Override
    protected int processOptions(CommandLine cmd) throws Exception {
        int result = super.processOptions(cmd);
        if (result != 0)
            return result;

        workers = OptionUtil.getIntOption(cmd, workersOption, getDefaultWorkers());

        if (cmd.hasOption(verboseOption.getOpt())) {
            verbose = true;
        }

        if (cmd.hasOption(hbaseMetricsOption.getOpt())) {
            useHbaseMetrics = true;
        }

        if (cmd.hasOption(lilyMetricsOption.getOpt())) {
            useLilyMetrics = true;
        }

        return 0;
    }

    @Override
    protected void cleanup() {
        Closer.close(zk);

        // Close any HBase connections used for the metrics
        HConnectionManager.deleteAllConnections(true);
        super.cleanup();
    }

    public void setupMetrics() throws Exception {
        String metricsFileName = getClass().getSimpleName() + "-metrics";

        System.out.println();
        System.out.println("Setting up metrics to file " + metricsFileName);

        File metricsFile = Util.getOutputFileRollOldOne(metricsFileName);

        HBaseAdmin hbaseAdmin = new HBaseAdmin(getHBaseConf());

        hbaseMetrics = new HBaseMetrics(hbaseAdmin);
        lilyMetrics = new LilyMetrics(getZooKeeper());

        ListMetricsPlugin plugins = new ListMetricsPlugin();
        HBaseMetricsPlugin metricsPlugin = new HBaseMetricsPlugin(hbaseMetrics, hbaseAdmin, useHbaseMetrics);
        plugins.add(metricsPlugin);
        addMetricsPlugins(plugins);

        metricsStream = new PrintStream(new FileOutputStream(metricsFile));
        metrics = new Metrics(metricsStream, plugins);
        metrics.setThreadCount(workers);

        metrics.startHeader();

        metricsStream.println("Now: " + new DateTime());
        metricsStream.println("Number of threads used: " + workers);
        metricsStream.println("More threads might increase number of ops/sec, but typically also increases time spent");
        metricsStream.println("in each individual operation.");
        metricsStream.println();
        metricsStream.println("About the ops/sec (if present):");
        metricsStream.println("  - interval ops/sec = number of ops by the complete time of the interval");
        metricsStream.println("  - real ops/sec = number of ops by the time they took");
        metricsStream.println("The interval ops/sec looks at how many of the operations have been done in");
        metricsStream.println("the interval, but includes thus the time spent doing other kinds of operations");
        metricsStream.println("or the overhead of the test tool itself. Therefore, you would expect the");
        metricsStream.println("real ops/sec to be better (higher) than the interval ops/sec. But when using");
        metricsStream.println("multiple threads, this is not always the case, since each thread runs for the");
        metricsStream.println("duration of the interval, e.g. 3 threads running for 30s makes 90s time passed");
        metricsStream.println("by the threads together. Another issue is that sometimes operations are very");
        metricsStream.println("quick but that their time is measured with a too low granularity.");
        metricsStream.println();
        metricsStream.println("About hbaseLoad (if present): this is currently the same as the number of regions");
        metricsStream.println("   deployed on the region server.");
        metricsStream.println("About hbaseRequestCount (if present): this is number of HBase requests per seconds");
        metricsStream.println();

        outputGeneralMetricReports();

        metrics.endHeader();

        System.out.println("Metrics ready, summary will be outputted every " + (metrics.getIntervalDuration() / 1000) + "s");
        System.out.println("Follow them using tail -f " + metricsFileName);
        System.out.println();
    }

    protected void addMetricsPlugins(ListMetricsPlugin plugins) {

    }

    public void finishMetrics() throws Exception {
        metrics.finish();

        metrics.startFooter();
        outputGeneralMetricReports();
        metrics.endFooter();

        hbaseMetrics.close();
        lilyMetrics.close();
    }

    private void outputGeneralMetricReports() throws Exception {
        hbaseMetrics.outputHBaseState(metricsStream);
        hbaseMetrics.outputRegionCountByServer(metricsStream);

        if (useHbaseMetrics) {
            hbaseMetrics.outputRegionServersInfo(metricsStream);
        }

        if (useLilyMetrics) {
            lilyMetrics.outputLilyServerInfo(metricsStream);
        }
    }

    public void startExecutor() {
        executor = new ThreadPoolExecutor(workers, workers, 10, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(100));
        executor.setRejectedExecutionHandler(new WaitPolicy());
    }

    public void stopExecutor() throws InterruptedException {
        executor.shutdown();
        // There might be quite some jobs waiting in the queue, and we don't care how long they take to run
        // to an end
        boolean successfulFinish = executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        if (!successfulFinish) {
            System.out.println("Executor did not end successfully");
        }
        executor = null;
    }

    public Configuration getHBaseConf() {
        if (hbaseConf == null) {
            hbaseConf = HBaseConfiguration.create();

            // TODO
            if (zkConnectionString.contains(":")) {
                System.err.println("ATTENTION: do not include port numbers in zookeeper connection string when using features/tests that use HBase.");
            }

            hbaseConf.set("hbase.zookeeper.quorum", zkConnectionString);
        }
        return hbaseConf;
    }

    protected ZooKeeperItf getZooKeeper() throws IOException {
        if (zk == null) {
            zk = new StateWatchingZooKeeper(zkConnectionString, zkSessionTimeout);
        }
        return zk;
    }
}
