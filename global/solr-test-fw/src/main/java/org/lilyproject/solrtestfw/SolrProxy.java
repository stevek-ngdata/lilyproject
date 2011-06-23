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
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;

public class SolrProxy {
    private static Mode MODE;

    private enum Mode { EMBED, CONNECT };
    private static String SOLR_MODE_PROP_NAME = "solr.test.mode";

    private SolrTestingUtility solrTestingUtility;
    private SolrServer solrServer;

    private HttpClient httpClient;

    private String uri;

    public SolrProxy() {
        MultiThreadedHttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();
        connectionManager.getParams().setDefaultMaxConnectionsPerHost(5);
        connectionManager.getParams().setMaxTotalConnections(50);
        httpClient = new HttpClient(connectionManager);
    }

    public void start(String schemaLocation, String solrUri) throws Exception {
        String solrModeProp = System.getProperty(SOLR_MODE_PROP_NAME);
        if (solrModeProp == null || solrModeProp.equals("") || solrModeProp.equals("embed")) {
            MODE = Mode.EMBED;
        } else if (solrModeProp.equals("connect")) {
            MODE = Mode.CONNECT;
        } else {
            throw new RuntimeException("Unexpected value for " + SOLR_MODE_PROP_NAME + ": " + solrModeProp);
        }
        
        switch (MODE) {
        case EMBED:
            solrTestingUtility = new SolrTestingUtility(schemaLocation);
            solrTestingUtility.start();
            this.uri = solrTestingUtility.getUri();
            solrServer = new CommonsHttpSolrServer(uri, httpClient);
            break;
        case CONNECT:
            this.uri = solrUri;
            solrServer = new CommonsHttpSolrServer(uri, httpClient);
            break;
        default:
            throw new RuntimeException("Unexpected mode: " + MODE);
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
