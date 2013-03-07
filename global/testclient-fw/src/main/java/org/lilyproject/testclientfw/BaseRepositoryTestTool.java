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

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.zookeeper.KeeperException;
import org.lilyproject.client.LilyClient;
import org.lilyproject.client.NoServersException;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.zookeeper.ZkConnectException;

public abstract class BaseRepositoryTestTool extends BaseTestTool {
    private static final String DEFAULT_SOLR_URL = "http://localhost:8983/solr";
    private static final String DEFAULT_SOLRCLOUD_URL = "localhost:2181/solr";

    protected String solrUrl;

    private Option solrOption;

    private Option solrCloudOption;

    protected SolrServer solrServer;

    protected LilyClient lilyClient;

    protected Repository repository;

    protected IdGenerator idGenerator;

    protected TypeManager typeManager;

    protected String NS = "tests";

    protected boolean useSolrCloud;

    @Override
    public List<Option> getOptions() {
        List<Option> options = super.getOptions();

        solrOption = OptionBuilder
                .withArgName("URL")
                .hasArg()
                .withDescription("URL of Solr. If using cloud mode then use the zookeeper connection string for Solr")
                .withLongOpt("solr")
                .create("s");

        solrCloudOption = OptionBuilder
                .withDescription("Connect to a Solr cloud server")
                .withLongOpt("solrcloud")
                .create("sc");

        options.add(solrOption);
        options.add(solrCloudOption);

        return options;
    }

    @Override
    protected int processOptions(CommandLine cmd) throws Exception {
        int result = super.processOptions(cmd);
        if (result != 0) {
            return result;
        }

        useSolrCloud = cmd.hasOption(solrCloudOption.getOpt());
        if (!cmd.hasOption(solrOption.getOpt())) {
            solrUrl = useSolrCloud ? DEFAULT_SOLRCLOUD_URL : DEFAULT_SOLR_URL;
        } else {
            solrUrl = useSolrCloud ? zkConnectionString + "/solr": cmd.getOptionValue(solrOption.getOpt());
        }

        return 0;
    }

    @Override
    protected void cleanup() {
        Closer.close(lilyClient);
        super.cleanup();
    }

    public void setupLily() throws IOException, ZkConnectException, NoServersException, InterruptedException,
            KeeperException, RepositoryException {
        lilyClient = new LilyClient(getZooKeeper());
        repository = lilyClient.getRepository();
        idGenerator = repository.getIdGenerator();
        typeManager = repository.getTypeManager();
    }

    public void setupSolr() throws MalformedURLException {
        if (useSolrCloud) {
            System.out.println("Using SolrCloud instance with ZooKeeper " + solrUrl);
            solrServer = new CloudSolrServer(solrUrl);
        } else {
            System.out.println("Using Solr instance at " + solrUrl);

            ThreadSafeClientConnManager connectionManager = new ThreadSafeClientConnManager();
            connectionManager.setDefaultMaxPerRoute(5);
            connectionManager.setMaxTotal(50);
            HttpClient httpClient = new DefaultHttpClient(connectionManager);

            solrServer = new HttpSolrServer(solrUrl, httpClient);
        }
    }
}
