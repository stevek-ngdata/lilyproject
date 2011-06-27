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

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.NullInputStream;
import org.lilyproject.util.MavenUtil;
import org.lilyproject.util.test.TestHomeUtil;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.webapp.WebAppContext;

import java.io.*;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SolrTestingUtility {
    private int solrPort = 8983;
    private Server server;
    private byte[] schemaData;
    private String autoCommitSetting;
    private String solrWarPath;
    private File solrHomeDir;
    private File solrCoreDir;
    private File solrConfDir;

    public SolrTestingUtility() throws IOException {
        this(null);
    }

    public SolrTestingUtility(File solrHomeDir) throws IOException {
        if (solrHomeDir == null) {
            this.solrHomeDir = TestHomeUtil.createTestHome("lily-solrtesthome-");
        } else {
            this.solrHomeDir = solrHomeDir;
        }
    }

    public void setSchemaData(byte[] schemaData) {
        this.schemaData = schemaData;
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

    public File getSolrHomeDir() {
        return solrHomeDir;
    }

    public void start() throws Exception {
        solrCoreDir = new File(solrHomeDir, "core0");
        solrConfDir = new File(solrCoreDir, "conf");
        FileUtils.forceMkdir(solrConfDir);

        writeCoresConf();
        copyDefaultConfigToSolrHome(autoCommitSetting == null ? "" : autoCommitSetting);

        if (schemaData == null) {
            schemaData = IOUtils.toByteArray(getClass().getResourceAsStream("conftemplate/schema.xml"));
        }
        writeSchema();

        setSystemProperties();


        // Determine location of Solr war file:
        //  - either provided by setSolrWarPath()
        //  - or provided via system property solr.war
        //  - finally use default, assuming availability in local repository
        if (solrWarPath == null) {
            solrWarPath = System.getProperty("solr.war");
        }
        if (solrWarPath == null) {
            Properties properties = new Properties();
            InputStream is = getClass().getResourceAsStream("solr.properties");
            if (is != null) {
                properties.load(is);
                is.close();
                String solrVersion = properties.getProperty("solr.version");
                solrWarPath = MavenUtil.findLocalMavenRepository().getAbsolutePath() +
                        "/org/apache/solr/solr-webapp/" + solrVersion + "/solr-webapp-" + solrVersion + ".war";
            }
        }

        if (solrWarPath == null || !new File(solrWarPath).exists()) {
            System.out.println();
            System.out.println("------------------------------------------------------------------------");
            System.out.println("Solr not found at");
            System.out.println(solrWarPath);
            System.out.println("------------------------------------------------------------------------");
            System.out.println();
            throw new Exception("Solr war not found at " + solrWarPath);
        }

        server = createServer();
        server.start();
    }

    private Server createServer() {
        Server server = new Server(solrPort);
        server.addHandler(new WebAppContext(solrWarPath, "/solr"));
        return server;
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

        if (solrHomeDir != null) {
            FileUtils.deleteDirectory(solrHomeDir);
        }
    }

    /**
     * Restarts the servlet container without throwing away the data.
     */
    public void restartServletContainer() throws Exception {
        server.stop();
        server.join();

        // Somehow restarting the same server object does not work, so create a new one
        server = createServer();
        server.start();
    }

    private void setSystemProperties() {
        System.setProperty("solr.solr.home", solrHomeDir.getAbsolutePath());
    }

    private void writeCoresConf() throws FileNotFoundException {
        File coresFile = new File(solrHomeDir, "solr.xml");
        PrintWriter writer = new PrintWriter(coresFile);
        writer.println("<solr persistent='false'>");
        writer.println(" <cores adminPath='/admin/cores' defaultCoreName='core0'>");
        writer.println("  <core name='core0' instanceDir='core0'>");
        writer.println("    <property name='solr.data.dir' value='${solr.solr.home}/core0/data'/>");
        writer.println("  </core>");
        writer.println(" </cores>");
        writer.println("</solr>");
        writer.close();
    }

    private void copyDefaultConfigToSolrHome(String autoCommitSetting) throws IOException {
        copyResourceFiltered("org/lilyproject/solrtestfw/conftemplate/solrconfig.xml",
                new File(solrConfDir, "solrconfig.xml"), autoCommitSetting);
        createEmptyFile(new File(solrConfDir, "synonyms.txt"));
        createEmptyFile(new File(solrConfDir, "stopwords.txt"));
        createEmptyFile(new File(solrConfDir, "protwords.txt"));
    }

    private void writeSchema() throws IOException {
        FileUtils.writeByteArrayToFile(new File(solrConfDir, "schema.xml"), schemaData);
    }

    private void copyResource(String path, File destination) throws IOException {
        InputStream is = getClass().getClassLoader().getResourceAsStream(path);
        FileUtils.copyInputStreamToFile(is, destination);
        is.close();
    }

    private void copyResourceFiltered(String path, File destination, String autoCommitSetting) throws IOException {

        InputStream is = getClass().getClassLoader().getResourceAsStream(path);
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));

        FileWriter writer = new FileWriter(destination);

        String placeholder = Pattern.quote("<!--AUTOCOMMIT_PLACEHOLDER-->");
        String replacement = Matcher.quoteReplacement(autoCommitSetting);

        String line;
        while ((line = reader.readLine()) != null) {
            line = line.replaceAll(placeholder, replacement);
            writer.write(line);
            writer.write('\n');
        }

        reader.close();
        writer.close();
    }

    private void createEmptyFile(File destination) throws IOException {
        FileUtils.copyInputStreamToFile(new NullInputStream(0), destination);
    }
}
