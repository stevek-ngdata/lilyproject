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

import org.mortbay.jetty.Server;
import org.mortbay.jetty.webapp.WebAppContext;

import java.io.*;

public class SolrTestingUtility {
    private int solrPort = 8983;
    private Server server;
    private String schemaLocation;
    private String autoCommitSetting;
    private String solrWarPath;
    private TempSolrHome solrHome;

    public SolrTestingUtility() {
    }

    public String getSchemaLocation() {
        return schemaLocation;
    }

    public void setSchemaLocation(String schemaLocation) {
        this.schemaLocation = schemaLocation;
    }

    public String getAutoCommitSetting() {
        return autoCommitSetting;
    }

    public void setAutoCommitSetting(String autoCommitSetting) {
        this.autoCommitSetting = autoCommitSetting;
    }

    public String getSolrWarPath() {
        return solrWarPath;
    }

    public void setSolrWarPath(String solrWarPath) {
        this.solrWarPath = solrWarPath;
    }

    public void start() throws Exception {
        solrHome = new TempSolrHome();

        solrHome.copyDefaultConfigToSolrHome(autoCommitSetting == null ? "" : autoCommitSetting);

        if (schemaLocation != null) {
            if (schemaLocation.startsWith("classpath:")) {
                solrHome.copySchemaFromResource(schemaLocation.substring("classpath:".length()));
            } else {
                solrHome.copySchemaFromFile(new File(schemaLocation));
            }
        } else {
            solrHome.copySchemaFromResource("org/lilyproject/solrtestfw/conftemplate/schema.xml");
        }

        solrHome.setSystemProperties();


        // Launch Solr
        if (solrWarPath == null) {
            solrWarPath = System.getProperty("solr.war");
        }

        if (solrWarPath == null || !new File(solrWarPath).exists()) {
            System.out.println();
            System.out.println("------------------------------------------------------------------------");
            System.out.println("Solr not found at");
            System.out.println(solrWarPath);
            System.out.println("Verify setting of solr.war system property");
            System.out.println("------------------------------------------------------------------------");
            System.out.println();
            throw new Exception("Solr war not found at " + solrWarPath);
        }

        server = new Server(solrPort);
        server.addHandler(new WebAppContext(solrWarPath, "/solr"));

        server.start();
    }

    public String getUri() {
        return "http://localhost:" + solrPort + "/solr";
    }

    public Server getServer() {
        return server;
    }

    public void stop() throws Exception {
        if (server != null)
            server.stop();

        if (solrHome != null)
            solrHome.cleanup();
    }

}
