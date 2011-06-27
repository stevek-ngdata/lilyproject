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
package org.lilyproject.solrtestfw;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.test.TestHomeUtil;

import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class SolrProxy {
    private Mode mode;

    public enum Mode { EMBED, CONNECT }
    private static String SOLR_MODE_PROP_NAME = "lily.solrproxy.mode";

    private SolrTestingUtility solrTestingUtility;
    private SolrServer solrServer;

    private MultiThreadedHttpConnectionManager connectionManager;
    private HttpClient httpClient;

    private String uri;

    private File testHome;

    public SolrProxy() throws IOException {
        this(null);
    }

    public SolrProxy(Mode mode) throws IOException {
        if (mode == null) {
            String solrModeProp = System.getProperty(SOLR_MODE_PROP_NAME);
            if (solrModeProp == null || solrModeProp.equals("") || solrModeProp.equals("embed")) {
                this.mode = Mode.EMBED;
            } else if (solrModeProp.equals("connect")) {
                this.mode = Mode.CONNECT;
            } else {
                throw new RuntimeException("Unexpected value for " + SOLR_MODE_PROP_NAME + ": " + solrModeProp);
            }
        } else {
            this.mode = mode;
        }
    }

    public void setTestHome(File testHome) throws IOException {
        if (mode != Mode.EMBED) {
            throw new RuntimeException("testHome should only be set when mode is EMBED");
        }
        this.testHome = testHome;
    }

    private void initTestHome() throws IOException {
        if (testHome == null) {
            testHome = TestHomeUtil.createTestHome("lily-solrproxy-");
        }

        FileUtils.forceMkdir(testHome);
        FileUtils.cleanDirectory(testHome);
    }

    public void start() throws Exception {
        start(null);
    }

    public void start(String schemaLocation) throws Exception {
        System.out.println("SolrProxy mode: " + mode);

        switch (mode) {
            case EMBED:
                initTestHome();
                System.out.println("SolrProxy embedded mode temp dir: " + testHome.getAbsolutePath());
                solrTestingUtility = new SolrTestingUtility(testHome);
                if (schemaLocation != null) {
                    solrTestingUtility.setSchemaLocation("classpath:" + schemaLocation);
                }
                solrTestingUtility.start();
                this.uri = solrTestingUtility.getUri();
                solrServer = new CommonsHttpSolrServer(uri, httpClient);
                break;
            case CONNECT:
                this.uri = "http://localhost:8983/solr";
                solrServer = new CommonsHttpSolrServer(uri, httpClient);
                break;
            default:
                throw new RuntimeException("Unexpected mode: " + mode);
        }

        connectionManager = new MultiThreadedHttpConnectionManager();
        connectionManager.getParams().setDefaultMaxConnectionsPerHost(5);
        connectionManager.getParams().setMaxTotalConnections(50);
        httpClient = new HttpClient(connectionManager);
    }
    
    public void stop() throws Exception {
        Closer.close(solrTestingUtility);

        Closer.close(connectionManager);
        connectionManager = null;
        httpClient = null;

        solrServer = null;
    }

    public SolrServer getSolrServer() {
        return solrServer;
    }
    
    public String getUri() {
        return uri;
    }

    public void changeSolrSchema(File file) throws Exception {
        FileInputStream fis = new FileInputStream(file);
        try {
            changeSolrSchema(fis);
        } finally {
            Closer.close(fis);
        }
    }

    /**
     *
     * @param is InputStream to read the schema from, stream will not be closed.
     */
    public void changeSolrSchema(InputStream is) throws Exception {
        //
        // In case of CONNECT mode, set up JMX connection
        //
        ObjectName lilyLauncher = new ObjectName("LilyLauncher:name=Launcher");
        JMXConnector jmxConnector = null;
        if (mode == Mode.CONNECT) {
            String hostport = "localhost:10102";
            JMXServiceURL url = new JMXServiceURL("service:jmx:rmi://" + hostport + "/jndi/rmi://" + hostport + "/jmxrmi");
            jmxConnector = JMXConnectorFactory.connect(url);
            jmxConnector.connect();
        }


        try {
            //
            // Find out location of Solr home dir
            //
            File solrHomeDir;
            switch (mode) {
                case EMBED:
                    solrHomeDir = solrTestingUtility.getSolrHomeDir();
                    break;
                case CONNECT:
                    String solrHome;
                    solrHome = (String)jmxConnector.getMBeanServerConnection().getAttribute(lilyLauncher, "SolrHome");
                    if (solrHome != null) {
                        solrHomeDir = new File(solrHome);
                    } else {
                        throw new Exception("Solr is not enabled in the Lily launcher.");
                    }
                    break;
                default:
                    throw new RuntimeException("Unexpected mode: " + mode);
            }

            //
            // Write the schema file
            //
            File solrConfDir = new File(solrHomeDir, "conf");
            FileUtils.forceMkdir(solrConfDir);
            File schemaFile = new File(solrConfDir, "schema.xml");
            FileUtils.copyInputStreamToFile(is, schemaFile);
            System.out.println("Wrote new Solr schema to " + schemaFile.getAbsolutePath());

            //
            // Restart Solr
            //
            switch (mode) {
                case EMBED:
                    solrTestingUtility.restartServletContainer();
                    break;
                case CONNECT:
                    jmxConnector.getMBeanServerConnection().invoke(lilyLauncher, "restartSolr", new Object[0], new String[0]);
                    break;
                default:
                    throw new RuntimeException("Unexpected mode: " + mode);
            }
        } finally {
            Closer.close(jmxConnector);
        }
    }
}
