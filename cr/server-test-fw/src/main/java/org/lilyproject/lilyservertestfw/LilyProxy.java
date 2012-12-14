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
package org.lilyproject.lilyservertestfw;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Set;

import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.PrefixFileFilter;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilyproject.hadooptestfw.HBaseProxy;
import org.lilyproject.solrtestfw.SolrDefinition;
import org.lilyproject.solrtestfw.SolrProxy;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.jmx.JmxLiaison;
import org.lilyproject.util.test.TestHomeUtil;

public class LilyProxy {
    private static final String TEMP_DIR_PREFIX = "lily-proxy-";
    private HBaseProxy hbaseProxy;
    private LilyServerProxy lilyServerProxy;
    private SolrProxy solrProxy;
    private Mode mode;
    private File testHome;
    private boolean started = false;
    private boolean hasBeenStarted = false;
    private boolean clearData = true;

    public enum Mode {EMBED, CONNECT, HADOOP_CONNECT}

    public static String MODE_PROP_NAME = "lily.lilyproxy.mode";
    public static String TESTHOME_PROP_NAME = "lily.lilyproxy.dir";
    public static String CLEARDATA_PROP_NAME = "lily.lilyproxy.clear";
    public static String RESTORE_TEMPLATE_DIR_PROP_NAME = "lily.lilyproxy.restoretemplatedir";

    public LilyProxy() throws IOException {
        this(null);
    }

    public LilyProxy(Mode mode) throws IOException {
        this(mode, null, null);
    }

    /**
     * Creates a new LilyProxy
     *
     * @param mode      either EMBED, CONNECT or HADOOP_CONNECT
     * @param testHome  the directory in which to store data and logfiles. Can only be used in EMBED mode.
     * @param clearData if true, clear the data when stopping the LilyProxy.
     *                  Should be used together with the testHome parameter and can only be used in EMBED mode.
     * @throws IOException
     */
    public LilyProxy(Mode mode, File testHome, Boolean clearData) throws IOException {
        if (mode == null) {
            String modeProp = System.getProperty(MODE_PROP_NAME);
            if (modeProp == null || modeProp.equals("") || modeProp.equals("embed")) {
                this.mode = Mode.EMBED;
            } else if (modeProp.equals("connect")) {
                this.mode = Mode.CONNECT;
            } else if (modeProp.equals("hadoop-connect")) {
                this.mode = Mode.HADOOP_CONNECT;
            } else {
                throw new RuntimeException("Unexpected value for " + MODE_PROP_NAME + ": " + modeProp);
            }
        } else {
            this.mode = mode;
        }

        if (testHome != null)
            setTestHome(testHome);
        else {
            String testHomeProp = System.getProperty(TESTHOME_PROP_NAME);
            if (testHomeProp != null)
                setTestHome(new File(testHomeProp));
        }

        if (clearData != null) {
            this.clearData = clearData;
        } else {
            this.clearData = Boolean.parseBoolean(System.getProperty(CLEARDATA_PROP_NAME, "true"));
        }

        HBaseProxy.Mode hbaseMode;
        SolrProxy.Mode solrMode;
        LilyServerProxy.Mode lilyServerMode;

        // LilyProxy imposes its mode on all of the specific Proxy's. This is because the special resetLilyState
        // operation in case of connect requires they are all in the same mode.
        // The special HADOOP_CONNECT mode is mainly intended for Lily's own tests: many tests suppose only
        // hadoop is running externally because they launch specific parts of the Lily implementation themselves,
        // combined with the fact that the mode should be the same for all tests.
        switch (this.mode) {
            case EMBED:
                hbaseMode = HBaseProxy.Mode.EMBED;
                solrMode = SolrProxy.Mode.EMBED;
                lilyServerMode = LilyServerProxy.Mode.EMBED;
                break;
            case CONNECT:
                hbaseMode = HBaseProxy.Mode.CONNECT;
                solrMode = SolrProxy.Mode.CONNECT;
                lilyServerMode = LilyServerProxy.Mode.CONNECT;
                break;
            case HADOOP_CONNECT:
                hbaseMode = HBaseProxy.Mode.CONNECT;
                solrMode = SolrProxy.Mode.EMBED;
                lilyServerMode = LilyServerProxy.Mode.EMBED;
                break;
            default:
                throw new RuntimeException("Unexpected mode: " + this.mode);
        }

        hbaseProxy = new HBaseProxy(hbaseMode, this.clearData);
        if (this.mode == Mode.CONNECT) {
            // we'll do the reset through the special JMX call
            hbaseProxy.setCleanStateOnConnect(false);
        }
        hbaseProxy.setEnableMapReduce(true);
        solrProxy = new SolrProxy(solrMode, this.clearData);
        lilyServerProxy = new LilyServerProxy(lilyServerMode, this.clearData);
    }

    public Mode getMode() {
        return mode;
    }

    public void start() throws Exception {
        start(null, null);
    }

    public void start(byte[] solrSchemaData) throws Exception {
        start(solrSchemaData, null);
    }

    public void start(byte[] solrSchemaData, byte[] solrConfigData) throws Exception {
        if (solrSchemaData != null || solrConfigData != null) {
            start(new SolrDefinition(solrSchemaData, solrConfigData));
        } else {
            start((SolrDefinition) null);
        }
    }

    public void start(SolrDefinition solrDef) throws Exception {
        if (started) {
            throw new IllegalStateException("LilyProxy is already started.");
        } else {
            started = true;
        }

        cleanOldTmpDirs();

        if (hasBeenStarted && this.mode == Mode.EMBED) {
            // In embed mode, we can't support multiple start-stop sequences since
            // HBase/Hadoop does not shut down all processes synchronously.
            throw new IllegalStateException("LilyProxy can only be started once in a JVM when using embed mode.");
        } else {
            hasBeenStarted = true;
        }

        System.out.println("LilyProxy mode: " + mode);

        if (mode == Mode.CONNECT) {
            // First reset the state
            System.out.println("Calling reset state flag on externally launched Lily...");
            try {
                String hostport = "localhost:10102";
                JMXServiceURL url =
                        new JMXServiceURL("service:jmx:rmi://" + hostport + "/jndi/rmi://" + hostport + "/jmxrmi");
                JMXConnector connector = JMXConnectorFactory.connect(url);
                connector.connect();
                ObjectName lilyLauncher = new ObjectName("LilyLauncher:name=Launcher");
                connector.getMBeanServerConnection()
                        .invoke(lilyLauncher, "resetLilyState", new Object[0], new String[0]);
                connector.close();
            } catch (Exception e) {
                throw new Exception("Resetting Lily state failed.", e);
            }
            System.out.println("State reset done.");
        }

        if (mode == Mode.EMBED || mode == Mode.HADOOP_CONNECT) {
            if (testHome == null)
                testHome = TestHomeUtil.createTestHome(TEMP_DIR_PREFIX);

            if (mode == Mode.EMBED)
                hbaseProxy.setTestHome(new File(testHome, TemplateDir.HADOOP_DIR));
            solrProxy.setTestHome(new File(testHome, TemplateDir.SOLR_DIR));
            lilyServerProxy.setTestHome(new File(testHome, TemplateDir.LILYSERVER_DIR));
        }

        if (mode == Mode.EMBED && Boolean.parseBoolean(System.getProperty(RESTORE_TEMPLATE_DIR_PROP_NAME, "true"))) {
            TemplateDir.restoreTemplateDir(testHome);
        }

        hbaseProxy.start();
        solrProxy.start(solrDef);
        lilyServerProxy.start();
    }

    public void stop() throws Exception {
        Closer.close(lilyServerProxy);
        Closer.close(solrProxy);
        Closer.close(hbaseProxy);

        if (clearData && testHome != null) {
            try {
                FileUtils.deleteDirectory(testHome);
            } catch (IOException e) {
                // We're logging this instead of throwing the exception
                // since deleting the folder often fails in Windows.
                // Throwing the exception would also fail the testcase although
                // it is only the cleanup of the temporary folder that failed.
                System.out.println(
                        "Warning! LilyProxy.stop() failed to delete folder: " + testHome.getAbsolutePath() + ", " +
                                e.getMessage());
            }
        }

        started = false;
    }

    public void setTestHome(File testHome) throws IOException {
        if (mode != Mode.EMBED) {
            throw new RuntimeException("testHome should only be set when mode is EMBED");
        }
        this.testHome = testHome;
        System.setProperty("test.build.data", testHome.getAbsolutePath());
    }

    public File getTestHome() {
        return testHome;
    }

    public void cleanOldTmpDirs() {
        if (clearData) {
            File tempDirectory = FileUtils.getTempDirectory();
            File[] files = tempDirectory.listFiles((FilenameFilter) new PrefixFileFilter(TEMP_DIR_PREFIX));
            for (File file : files) {
                FileUtils.deleteQuietly(file);
            }
        }
    }

    public HBaseProxy getHBaseProxy() {
        return hbaseProxy;
    }

    public LilyServerProxy getLilyServerProxy() {
        return lilyServerProxy;
    }

    public SolrProxy getSolrProxy() {
        return solrProxy;
    }

    /**
     * Waits for all SEP events to be processed and if successful commits the solr index.
     *
     * <p>The canonical usage in tests is along these lines:</p>
     *
     * <pre>Assert.assertTrue("Processing events took too long", lilyProxy.waitSepEventsProcessed(60000L));</pre>
     *
     * @param timeout the maximum time to wait
     * @return false if the timeout was reached before all events were processed
     */
    public boolean waitSepEventsProcessed(long timeout) throws Exception {
        return waitSepEventsProcessed(timeout, true);
    }

    /**
     * Waits for all SEP events to be processed and optionally commits the solr index.
     *
     * @param timeout the maximum time to wait
     * @return false if the timeout was reached before all events were processed
     */
    public boolean waitSepEventsProcessed(long timeout, boolean commitSolr) throws Exception {
        boolean success = hbaseProxy.waitOnReplication(timeout);
        if (success && commitSolr) {
            solrProxy.commit();
        }
        return success;
    }
}
