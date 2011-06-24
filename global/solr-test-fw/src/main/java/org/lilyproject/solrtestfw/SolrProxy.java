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
import org.lilyproject.util.test.TestHomeUtil;

import java.io.File;
import java.io.IOException;

public class SolrProxy {
    private Mode mode;

    public enum Mode { EMBED, CONNECT }
    private static String SOLR_MODE_PROP_NAME = "lily.solrproxy.mode";

    private SolrTestingUtility solrTestingUtility;
    private SolrServer solrServer;

    private HttpClient httpClient;

    private String uri;

    private File solrHomeDir;

    public SolrProxy() throws IOException {
        this(null, null);
    }

    /**
     *
     * @param testDir storage dir to use in case of embedded mode (a subdir will be made)
     */
    public SolrProxy(Mode mode, File testDir) throws IOException {
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

        MultiThreadedHttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();
        connectionManager.getParams().setDefaultMaxConnectionsPerHost(5);
        connectionManager.getParams().setMaxTotalConnections(50);
        httpClient = new HttpClient(connectionManager);

        if (this.mode == Mode.EMBED) {
            if (testDir == null) {
                solrHomeDir = TestHomeUtil.createTestHome("lily-solrproxy-");
            } else {
                solrHomeDir = new File(testDir, "solrproxy");
                FileUtils.forceMkdir(solrHomeDir);
            }
        }
    }

    public void start(String schemaLocation) throws Exception {
        System.out.println("SolrProxy mode: " + mode);

        switch (mode) {
            case EMBED:
                FileUtils.forceMkdir(solrHomeDir);
                FileUtils.cleanDirectory(solrHomeDir);
                System.out.println("SolrProxy embedded mode temp dir: " + solrHomeDir.getAbsolutePath());
                solrTestingUtility = new SolrTestingUtility(solrHomeDir);
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
    }
    
    public void stop() throws Exception {
        solrServer = null;
        if (solrTestingUtility != null)
            solrTestingUtility.stop();
    }

    public SolrServer getSolrServer() {
        return solrServer;
    }
    
    public String getUri() {
        return uri;
    }
}
