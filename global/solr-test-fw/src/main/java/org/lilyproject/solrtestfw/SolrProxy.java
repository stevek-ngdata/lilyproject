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

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.test.TestHomeUtil;
import org.lilyproject.util.xml.DocumentHelper;
import org.lilyproject.util.xml.XPathUtils;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class SolrProxy {
    private Mode mode;

    public enum Mode { EMBED, CONNECT }
    public static String SOLR_MODE_PROP_NAME = "lily.solrproxy.mode";

    private SolrTestingUtility solrTestingUtility;
    private Map<String, SolrServer> solrServers = new HashMap<String, SolrServer>();

    private ThreadSafeClientConnManager connectionManager;
    private HttpClient httpClient;

    private String uri;

    private File testHome;

    private final boolean clearData;

    private final boolean enableSolrCloud;

    public SolrProxy() throws IOException {
        this(null);
    }

    public SolrProxy(Mode mode) throws IOException {
        this(mode, true);
    }

    public SolrProxy(Mode mode, boolean clearData) throws IOException {
        this(mode, clearData, false);
    }

    /**
     * Creates a new SolrProxy
     * @param mode either EMBED or CONNECT
     * @param clearData it true, clears the data directories upon shutdown
     * @param enableSolrCloud if true starts solr in cloud mode
     * @throws IOException
     */
    public SolrProxy(Mode mode, boolean clearData, boolean enableSolrCloud) throws IOException {
        this.clearData = clearData;
        this.enableSolrCloud = enableSolrCloud;

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
    }

    public void start() throws Exception {
        start(null, null);
    }

    /**
     * @deprecated use {@link #start(SolrDefinition)} instead
     */
    @Deprecated
    public void start(byte[] solrSchemaData, byte[] solrConfigData) throws Exception {
        if (solrSchemaData != null || solrConfigData != null) {
            start(new SolrDefinition(solrSchemaData, solrConfigData));
        } else {
            start(null);
        }
    }

    public void start(SolrDefinition solrDef) throws Exception {
        System.out.println("SolrProxy mode: " + mode);

        solrDef = solrDef != null ? solrDef : new SolrDefinition();

        switch (mode) {
            case EMBED:
                initTestHome();
                System.out.println("SolrProxy embedded mode temp dir: " + testHome.getAbsolutePath());
                solrTestingUtility = new SolrTestingUtility(testHome, clearData, enableSolrCloud);
                solrTestingUtility.setSolrDefinition(solrDef);
                solrTestingUtility.start();
                this.uri = solrTestingUtility.getUri();
                initSolrServers(solrDef);
                break;
            case CONNECT:
                this.uri = "http://localhost:8983/solr";
                changeSolrDefinition(solrDef);
                break;
            default:
                throw new RuntimeException("Unexpected mode: " + mode);
        }

        connectionManager = new ThreadSafeClientConnManager();
        connectionManager.setDefaultMaxPerRoute(5);
        connectionManager.setMaxTotal(50);
        httpClient = new DefaultHttpClient(connectionManager);
    }

    public void stop() throws Exception {
        Closer.close(solrTestingUtility);

        Closer.close(connectionManager);
        connectionManager = null;
        httpClient = null;

        solrServers = null;
    }

    private void initSolrServers(SolrDefinition solrDef) throws MalformedURLException {
        solrServers.clear();
        for (SolrDefinition.CoreDefinition core : solrDef.getCores()) {
            SolrServer solrServer = new HttpSolrServer(getUri(core.getName()), httpClient);
            solrServers.put(core.getName(), solrServer);
        }
    }

    /**
     * Returns the SolrServer object for the core0 core.
     */
    public SolrServer getSolrServer() {
        return solrServers.get("core0");
    }

    public SolrServer getSolrServer(String coreName) {
        return solrServers.get(coreName);
    }

    /**
     * Commits the solr index of all Solr cores.
     */
    public void commit() throws Exception {
        for (SolrServer solr : solrServers.values()) {
            solr.commit();
        }
    }

    public void commit(String coreName) throws Exception {
        solrServers.get(coreName).commit();
    }

    public String getUri() {
        return uri;
    }

    public String getUri(String coreName) {
        return uri + "/" + coreName + "/";
    }

    /**
     * @deprecated use {@link #changeSolrDefinition} instead
     */
    @Deprecated
    public void changeSolrConfig(byte[] newConfigData) throws Exception {
        changeSolrDefinition(new SolrDefinition(null, newConfigData));
    }

    /**
     * @deprecated use {@link #changeSolrDefinition} instead
     */
    @Deprecated
    public void changeSolrSchema(byte[] newSchemaData) throws Exception {
        changeSolrDefinition(new SolrDefinition(newSchemaData, null));
    }

    /**
     * @deprecated use {@link #changeSolrDefinition} instead
     */
    @Deprecated
    public void changeSolrSchemaAndConfig(byte[] newSchemaData, byte[] newConfigData) throws Exception {
        changeSolrDefinition(new SolrDefinition(newSchemaData, newConfigData));
    }

    public void changeSolrDefinition(SolrDefinition solrDef) throws Exception {
        Document doc = readCoreStatus();

        // Find out Solr home. There will always be at least one core, its parent dir should be the home dir
        File solrCore0Dir = new File(XPathUtils.evalString(
                "/response/lst[@name='status']/lst[1]/str[@name='instanceDir']", doc));
        File solrHomeDir = solrCore0Dir.getParentFile();

        //
        // Below we will remove the existing cores, write the new configuration (and delete existing data),
        // and then finally create new cores. Solr doesn't seem to be happy with dynamically unloading and
        // recreating the default core (and we want a default core for people working core-unaware), therefore
        // there is always a core called core0 which is reload rather than unloaded.
        //

        // Remove existing cores
        for (String coreName : getSolrCoreNames()) {
            // Solr doesn't support UNLOAD'ing and later re-CREATE of the default core
            if (!coreName.equals(SolrDefinition.DEFAULT_CORE_NAME)) {
                performCoreAction("UNLOAD", coreName, null);
            }
        }

        // Write new cores
        // TODO support autoCommitSetting?
        SolrHomeDirSetup.write(solrHomeDir, solrDef, null);

        // Create cores
        for (SolrDefinition.CoreDefinition core : solrDef.getCores()) {
            // Solr doesn't support UNLOAD'ing and later re-CREATE of the default core
            if (!core.getName().equals(SolrDefinition.DEFAULT_CORE_NAME)) {
                performCoreAction("CREATE", core.getName(), "&name=" + core.getName() + "&instanceDir="
                    + URLEncoder.encode(new File(solrHomeDir, core.getName()).getAbsolutePath(), "UTF-8"));
            }
        }

        // Reload default core
        performCoreAction("RELOAD", SolrDefinition.DEFAULT_CORE_NAME, null);

        initSolrServers(solrDef);
    }

    /**
     * <b>NOT FOR PUBLIC USE.</b> Returns the list of cores active in Solr.
     */
    public static List<String> getSolrCoreNames() throws IOException, SAXException, ParserConfigurationException {
        Document doc = readCoreStatus();

        List<String> result = new ArrayList<String>();

        final Pattern CORE_NAME_FROM_INSTANCE_DIR = Pattern.compile("^.*/([^/]*)/$");
        NodeList instanceDirs = XPathUtils.evalNodeList(
                "/response/lst[@name='status']/lst/str[@name='instanceDir']", doc);

        for (int i = 0; i < instanceDirs.getLength(); i++) {
            // Get the Solr core name from the instance dir. We can't access the core name directly due to SOLR-2905
            String instanceDir = instanceDirs.item(i).getTextContent();
            Matcher matcher = CORE_NAME_FROM_INSTANCE_DIR.matcher(instanceDir);
            if (matcher.matches()) {
                String coreName = matcher.group(1);
                result.add(coreName);
            } else {
                throw new RuntimeException("Unexpected: Solr instance dir did not match regex: " + instanceDir);
            }
        }

        return result;
    }

    private static Document readCoreStatus() throws IOException, SAXException, ParserConfigurationException {
        URL coreStatusURL = new URL("http://localhost:8983/solr/admin/cores?action=STATUS");
        HttpURLConnection coreStatusConn = (HttpURLConnection)coreStatusURL.openConnection();
        coreStatusConn.connect();
        if (coreStatusConn.getResponseCode() != 200) {
            throw new RuntimeException("Fetch Solr core status: expected status 200 but got: " +
                    coreStatusConn.getResponseCode());
        }
        InputStream is = coreStatusConn.getInputStream();
        Document doc = DocumentHelper.parse(is);
        is.close();
        coreStatusConn.disconnect();
        return doc;
    }

    private void performCoreAction(String action, String coreName, String moreParams) throws IOException {
        moreParams = moreParams == null ? "" : moreParams;
        String url = "http://localhost:8983/solr/admin/cores?action=" + action + "&core=" + coreName + moreParams;
        URL coreActionURL = new URL(url);
        HttpURLConnection conn = (HttpURLConnection)coreActionURL.openConnection();
        conn.connect();
        int response = conn.getResponseCode();
        conn.disconnect();
        if (response != 200) {
            throw new RuntimeException("Core " + action + ": expected status 200 but got: " + response + ": "
                    + conn.getResponseMessage());
        }
    }
}
