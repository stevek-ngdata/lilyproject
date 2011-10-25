package org.lilyproject.testclientfw;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.zookeeper.KeeperException;
import org.lilyproject.client.LilyClient;
import org.lilyproject.client.NoServersException;
import org.lilyproject.repository.api.*;
import org.lilyproject.util.zookeeper.ZkConnectException;

public abstract class BaseRepositoryTestTool extends BaseTestTool {
    private static final String DEFAULT_SOLR_URL = "http://localhost:8983/solr";

    protected String solrUrl;

    private Option solrOption;

    protected SolrServer solrServer;

    protected LilyClient lilyClient;

    protected Repository repository;

    protected IdGenerator idGenerator;

    protected TypeManager typeManager;

    protected String NS = "tests";

    @Override
    public List<Option> getOptions() {
        List<Option> options = super.getOptions();

        solrOption = OptionBuilder
                .withArgName("URL")
                .hasArg()
                .withDescription("URL of Solr")
                .withLongOpt("solr")
                .create("s");

        options.add(solrOption);

        return options;
    }

    @Override
    protected int processOptions(CommandLine cmd) throws Exception {
        int result = super.processOptions(cmd);
        if (result != 0)
            return result;

        if (!cmd.hasOption(solrOption.getOpt())) {
            solrUrl = DEFAULT_SOLR_URL;
        } else {
            solrUrl = cmd.getOptionValue(solrOption.getOpt());
        }

        return 0;
    }

    public void setupLily() throws IOException, ZkConnectException, NoServersException, InterruptedException,
            KeeperException, RepositoryException {
        lilyClient = new LilyClient(getZooKeeper());
        repository = lilyClient.getRepository();
        idGenerator = repository.getIdGenerator();
        typeManager = repository.getTypeManager();
    }

    public void setupSolr() throws MalformedURLException {
        System.out.println("Using Solr instance at " + solrUrl);

        MultiThreadedHttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();
        connectionManager.getParams().setDefaultMaxConnectionsPerHost(5);
        connectionManager.getParams().setMaxTotalConnections(50);
        HttpClient httpClient = new HttpClient(connectionManager);

        solrServer = new CommonsHttpSolrServer(solrUrl, httpClient);
    }
}
