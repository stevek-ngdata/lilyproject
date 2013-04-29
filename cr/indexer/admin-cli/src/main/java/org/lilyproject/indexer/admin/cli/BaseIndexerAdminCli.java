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
package org.lilyproject.indexer.admin.cli;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.KeeperException;
import org.lilyproject.cli.BaseZkCliTool;
import org.lilyproject.client.LilyClient;
import org.lilyproject.indexer.model.api.IndexBatchBuildState;
import org.lilyproject.indexer.model.api.IndexDefinition;
import org.lilyproject.indexer.model.api.IndexGeneralState;
import org.lilyproject.indexer.model.api.IndexUpdateState;
import org.lilyproject.indexer.model.api.IndexValidityException;
import org.lilyproject.indexer.model.api.WriteableIndexerModel;
import org.lilyproject.indexer.model.impl.IndexerModelImpl;
import org.lilyproject.indexer.model.indexerconf.IndexerConfBuilder;
import org.lilyproject.indexer.model.indexerconf.IndexerConfException;
import org.lilyproject.indexer.model.sharding.ShardingConfigException;
import org.lilyproject.util.Version;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.zookeeper.StateWatchingZooKeeper;
import org.lilyproject.util.zookeeper.ZooKeeperItf;
import org.lilyproject.util.zookeeper.ZooKeeperOperation;

public abstract class BaseIndexerAdminCli extends BaseZkCliTool {

    protected Option forceOption;
    protected Option nameOption;
    protected Option solrShardsOption;
    protected Option shardingConfigurationOption;
    protected Option configurationOption;
    protected Option generalStateOption;
    protected Option updateStateOption;
    protected Option buildStateOption;
    protected Option outputFileOption;
    protected Option batchIndexConfigurationOption;
    protected Option defaultBatchIndexConfigurationOption;
    protected Option batchIndexTablesOption;
    protected Option defaultBatchIndexTablesOption;
    protected Option printBatchConfigurationOption;
    protected Option printShardingConfigurationOption;
    protected Option solrCollectionOption;
    protected Option solrZkOption;
    protected Option solrModeOption;
    protected Option enableDerefMapOption;

    protected String indexName;
    protected Map<String, String> solrShards;
    protected IndexGeneralState generalState;
    protected IndexUpdateState updateState;
    protected IndexBatchBuildState buildState;
    protected byte[] indexerConfiguration;
    protected byte[] shardingConfiguration;
    protected byte[] batchIndexConfiguration;
    protected byte[] defaultBatchIndexConfiguration;
    protected List<String> batchIndexTables;
    protected List<String> defaultBatchIndexTables;
    protected WriteableIndexerModel model;
    private ZooKeeperItf zk;
    protected String outputFileName;
    protected boolean printBatchConfiguration;
    protected boolean printShardingConfiguration;
    protected String solrCollection;
    protected String solrZk;
    protected SolrMode solrMode;
    protected Boolean enableDerefMap;

    protected enum SolrMode {
        CLASSIC, CLOUD
    }

    public BaseIndexerAdminCli() {
        // Here we instantiate various options, but it is up to subclasses to decide which ones
        // they acutally want to use (see getOptions() method).
        forceOption = OptionBuilder
                .withDescription("Skips optional validations.")
                .withLongOpt("force")
                .create("f");

        nameOption = OptionBuilder
                .withArgName("name")
                .hasArg()
                .withDescription("Index name.")
                .withLongOpt("name")
                .create("n");

        solrShardsOption = OptionBuilder
                .withArgName("solr-shards")
                .hasArg()
                .withDescription("Comma-separated list of 'shardname:URL' pairs pointing to Solr instances.")
                .withLongOpt("solr-shards")
                .create("s");

        shardingConfigurationOption = OptionBuilder
                .withArgName("shardingconfig.json")
                .hasOptionalArg()
                .withDescription("Sharding configuration. If no value than the sharding configuration will be removed.")
                .withLongOpt("sharding-config")
                .create("p");

        configurationOption = OptionBuilder
                .withArgName("indexerconfig.xml")
                .hasArg()
                .withDescription("Indexer configuration.")
                .withLongOpt("indexer-config")
                .create("c");

        defaultBatchIndexConfigurationOption = OptionBuilder
                .withArgName("batchconfig.json")
                .hasOptionalArg()
                .withDescription("Default configuration for batch builds in this index. If no value is provided" +
                        "then the default batch index configuration will be removed.")
                .withLongOpt("default-batch-config")
                .create("dbi");

        batchIndexConfigurationOption = OptionBuilder
                .withArgName("batchconfig.json")
                .hasArg()
                .withDescription("Configuration for the current batch build of this index. Build state must be set" +
                        " to BUILD_REQUESTED.")
                .withLongOpt("batch-config")
                .create("bi");

        batchIndexTablesOption = OptionBuilder
                .withArgName("batch-tables")
                .hasArg()
                .withDescription("Comma-separated list of tables to be included in the next batch index rebuild")
                .withLongOpt("batch-tables")
                .create("t");

        defaultBatchIndexTablesOption = OptionBuilder
                .withArgName("default-batch-tables")
                .hasArg()
                .withDescription("Comma-separated list of default tables to be included in batch index rebuilds")
                .withLongOpt("default-batch-tables")
                .create("dt");

        generalStateOption = OptionBuilder
                .withArgName("state")
                .hasArg()
                .withDescription("General state, one of: " + IndexGeneralState.ACTIVE + ", " +
                        IndexGeneralState.DISABLED + ", " + IndexGeneralState.DELETE_REQUESTED)
                .withLongOpt("state")
                .create("i");

        updateStateOption = OptionBuilder
                .withArgName("state")
                .hasArg()
                .withDescription("Update state, one of: " + getStates(IndexUpdateState.values()))
                .withLongOpt("update-state")
                .create("u");

        buildStateOption = OptionBuilder
                .withArgName("state")
                .hasArg()
                .withDescription("Build state, only: " + IndexBatchBuildState.BUILD_REQUESTED)
                .withLongOpt("build-state")
                .create("b");

        outputFileOption = OptionBuilder
                .withArgName("filename")
                .hasArg()
                .withDescription("Output file name")
                .withLongOpt("output-file")
                .create("o");

        printBatchConfigurationOption = OptionBuilder
                .withDescription("Print the batch index configuration")
                .withLongOpt("print-batch-conf")
                .create("pbc");

        printShardingConfigurationOption = OptionBuilder
                .withDescription("Print the sharding configuration.")
                .create("pp");

        solrCollectionOption =  OptionBuilder
                .withArgName("collection")
                .hasArg()
                .withDescription("Solr collection")
                .withLongOpt("collection")
                .create("sc");

        solrZkOption =  OptionBuilder
                .withArgName("Zookeeper connection string")
                .hasArg()
                .withDescription("Zookeeper connection string for Solr")
                .withLongOpt("solr-zk")
                .create("sz");

        solrModeOption = OptionBuilder
                .withArgName("Solr mode")
                .hasArg()
                .withDescription("Solr mode (valid values are: classic | cloud)")
                .withLongOpt("solr-mode")
                .create("sm");

        enableDerefMapOption =  OptionBuilder
                .withArgName("enable deref map")
                .hasArg()
                .withDescription("By default a deref map is maintained in HBase to resolve dependencies " +
                        "between indexed records through dereference expressions. This is not used during batch " +
                        "index building, hence setting this option to false it is a performance improvement if you " +
                        "only ever plan to populate your index through batch index building.")
                .withLongOpt("enable-derefmap")
                .create("edm");
    }

    @Override
    protected void reportThrowable(Throwable throwable) {
        if (throwable instanceof IndexValidityException) {
            System.out.println("ATTENTION");
            System.out.println("---------");
            System.out.println("The index could not be created or updated because:");
            printExceptionMessages(throwable);
        } else {
            super.reportThrowable(throwable);
        }
    }

    protected static void printExceptionMessages(Throwable throwable) {
        Throwable cause = throwable;
        while (cause != null) {
            String prefix = "";
            if (cause instanceof IndexValidityException) {
                prefix = "Index definition: ";
            } else if (cause instanceof IndexerConfException) {
                prefix = "Indexer configuration: ";
            } else if (cause instanceof ShardingConfigException) {
                prefix = "Sharding configuration: ";
            }
            System.out.println(prefix + cause.getMessage());
            cause = cause.getCause();
        }
    }

    @Override
    public List<Option> getOptions() {
        List<Option> options = super.getOptions();

        options.add(forceOption);

        return options;
    }

    @Override
    protected String getVersion() {
        return Version.readVersion("org.lilyproject", "lily-indexer-admin-cli");
    }

    @Override
    protected int processOptions(CommandLine cmd) throws Exception {
        int result = super.processOptions(cmd);
        if (result != 0) {
            return result;
        }

        if (cmd.hasOption(nameOption.getOpt())) {
            indexName = cmd.getOptionValue(nameOption.getOpt());
        }

        if (cmd.hasOption(solrShardsOption.getOpt())) {
            solrShards = new HashMap<String, String>();
            String[] solrShardEntries = cmd.getOptionValue(solrShardsOption.getOpt()).split(",");
            Set<String> addresses = new HashSet<String>();
            // Be helpful to the user and validate the URIs are syntactically correct
            for (String shardEntry : solrShardEntries) {
                int sep = shardEntry.indexOf(':');
                if (sep == -1) {
                    System.out.println("Solr shards should be specified as 'name:URL' pairs, which the following is not:");
                    System.out.println(shardEntry);
                    return 1;
                }

                String shardName = shardEntry.substring(0, sep).trim();
                if (shardName.length() == 0) {
                    System.out.println("Zero-length shard name in the following shard entry:");
                    System.out.println(shardEntry);
                    return 1;
                }

                if (shardName.equals("http")) {
                    System.out.println("You forgot to specify a shard name for the Solr shard " + shardEntry);
                    return 1;
                }

                String shardAddress = shardEntry.substring(sep + 1).trim();
                try {
                    URI uri = new URI(shardAddress);
                    if (!uri.isAbsolute()) {
                        System.out.println("Not an absolute URI: " + shardAddress);
                        return 1;
                    }
                } catch (URISyntaxException e) {
                    System.out.println("Invalid Solr shard URI: " + shardAddress);
                    System.out.println(e.getMessage());
                    return 1;
                }

                if (solrShards.containsKey(shardName)) {
                    System.out.println("Duplicate shard name: " + shardName);
                    return 1;
                }

                if (addresses.contains(shardAddress)) {
                    if (!cmd.hasOption(forceOption.getOpt())) {
                        System.out.println("You have two shards pointing to the same URI:");
                        System.out.println(shardAddress);
                        System.out.println();
                        System.out.println("If this is what you want, use the --" + forceOption.getLongOpt() +
                                " option to bypass this check.");
                        return 1;
                    }
                }

                addresses.add(shardAddress);

                solrShards.put(shardName, shardAddress);
            }
        }

        if (cmd.hasOption(shardingConfigurationOption.getOpt())) {
            String optionValue = cmd.getOptionValue(shardingConfigurationOption.getOpt());
            if (optionValue != null) {
                File configurationFile = new File(optionValue);

                if (!configurationFile.exists()) {
                    System.out.println("Specified sharding configuration file not found:");
                    System.out.println(configurationFile.getAbsolutePath());
                    return 1;
                }
                shardingConfiguration = FileUtils.readFileToByteArray(configurationFile);
            } else {
                shardingConfiguration = new byte[0];
            }

        }

        if (cmd.hasOption(configurationOption.getOpt())) {
            File configurationFile = new File(cmd.getOptionValue(configurationOption.getOpt()));

            if (!configurationFile.exists()) {
                System.out.println("Specified indexer configuration file not found:");
                System.out.println(configurationFile.getAbsolutePath());
                return 1;
            }

            indexerConfiguration = FileUtils.readFileToByteArray(configurationFile);

            if (!cmd.hasOption(forceOption.getOpt())) {
                LilyClient lilyClient = null;
                try {
                    lilyClient = new LilyClient(zkConnectionString, 10000);
                    IndexerConfBuilder.build(new ByteArrayInputStream(indexerConfiguration), lilyClient.getRepository());
                } catch (Exception e) {
                    System.out.println(); // separator line because LilyClient might have produced some error logs
                    System.out.println("Failed to parse & build the indexer configuration.");
                    System.out.println();
                    System.out.println("If this problem occurs because no Lily node is available");
                    System.out.println("or because certain field types or record types do not exist,");
                    System.out.println("then you can skip this validation using the option --" + forceOption.getLongOpt());
                    System.out.println();
                    if (e instanceof IndexerConfException) {
                        printExceptionMessages(e);
                    } else {
                        e.printStackTrace();
                    }
                    return 1;
                } finally {
                    Closer.close(lilyClient);
                }
            }
        }

        if (cmd.hasOption(generalStateOption.getOpt())) {
            String stateName = cmd.getOptionValue(generalStateOption.getOpt());
            try {
                generalState = IndexGeneralState.valueOf(stateName.toUpperCase());
            } catch (IllegalArgumentException e) {
                System.out.println("Invalid index state: " + stateName);
                return 1;
            }
        }

        if (cmd.hasOption(updateStateOption.getOpt())) {
            String stateName = cmd.getOptionValue(updateStateOption.getOpt());
            try {
                updateState = IndexUpdateState.valueOf(stateName.toUpperCase());
            } catch (IllegalArgumentException e) {
                System.out.println("Invalid index update state: " + stateName);
                return 1;
            }
        }

        if (cmd.hasOption(buildStateOption.getOpt())) {
            String stateName = cmd.getOptionValue(buildStateOption.getOpt());
            try {
                buildState = IndexBatchBuildState.valueOf(stateName.toUpperCase());
            } catch (IllegalArgumentException e) {
                System.out.println("Invalid index build state: " + stateName);
                return 1;
            }

            if (buildState != IndexBatchBuildState.BUILD_REQUESTED) {
                System.out.println("The build state can only be set to " + IndexBatchBuildState.BUILD_REQUESTED);
                return 1;
            }
        }

        if (cmd.hasOption(outputFileOption.getOpt())) {
            outputFileName = cmd.getOptionValue(outputFileOption.getOpt());
            File file = new File(outputFileName);

            if (!cmd.hasOption(forceOption.getOpt()) && file.exists()) {
                System.out.println("The specified output file already exists:");
                System.out.println(file.getAbsolutePath());
                System.out.println();
                System.out.println("Use --" + forceOption.getLongOpt() + " to overwrite it.");
            }
        }

        if (cmd.hasOption(defaultBatchIndexConfigurationOption.getOpt())) {
            String fileName = cmd.getOptionValue(defaultBatchIndexConfigurationOption.getOpt());
            if (fileName != null) {
                File configurationFile = new File(fileName);

                if (!configurationFile.exists()) {
                    System.out.println("Specified default batch build configuration file not found:");
                    System.out.println(configurationFile.getAbsolutePath());
                    return 1;
                }

                defaultBatchIndexConfiguration = FileUtils.readFileToByteArray(configurationFile);
            } else {
                defaultBatchIndexConfiguration = new byte[0];
            }
        }

        if (cmd.hasOption(batchIndexConfigurationOption.getOpt())) {
            File configurationFile = new File(cmd.getOptionValue(
                    batchIndexConfigurationOption.getOpt()));

            if (!configurationFile.exists()) {
                System.out.println("Specified batch build configuration file not found:");
                System.out.println(configurationFile.getAbsolutePath());
                return 1;
            }

            batchIndexConfiguration = FileUtils.readFileToByteArray(configurationFile);
        }

        if (cmd.hasOption(batchIndexTablesOption.getOpt())) {
            batchIndexTables = Lists.newArrayList(cmd.getOptionValue(batchIndexTablesOption.getOpt()).split(","));
        }

        if (cmd.hasOption(defaultBatchIndexTablesOption.getOpt())) {
            defaultBatchIndexTables = Lists.newArrayList(
                    cmd.getOptionValue(defaultBatchIndexTablesOption.getOpt()).split(","));
        }

        if (cmd.hasOption(solrCollectionOption.getOpt())) {
            solrCollection = cmd.getOptionValue(solrCollectionOption.getOpt());
        }

        if (cmd.hasOption(solrZkOption.getOpt())) {
            solrZk = cmd.getOptionValue(solrZkOption.getOpt());
        }

        if (cmd.hasOption(solrModeOption.getOpt())) {
            if (cmd.getOptionValue(solrModeOption.getOpt()).equals("classic")) {
                solrMode = SolrMode.CLASSIC;
            } else if (cmd.getOptionValue(solrModeOption.getOpt()).equals("cloud")) {
                solrMode = SolrMode.CLOUD;
            } else {
                System.out.printf("Invalid solr mode: '%s' (should be 'cloud' or 'classic')\n", solrModeOption.getOpt());
                return 1;
            }
        }

        if (cmd.hasOption(enableDerefMapOption.getOpt())) {
            enableDerefMap = Boolean.valueOf(cmd.getOptionValue(enableDerefMapOption.getOpt()));
        }

        printBatchConfiguration = cmd.hasOption(printBatchConfigurationOption.getOpt());

        printShardingConfiguration = cmd.hasOption(printShardingConfigurationOption.getOpt());

        return 0;
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        int result = super.run(cmd);
        if (result != 0) {
            return result;
        }

        zk = new StateWatchingZooKeeper(zkConnectionString, zkSessionTimeout);

        boolean lilyNodeExists = zk.retryOperation(new ZooKeeperOperation<Boolean>() {
            @Override
            public Boolean execute() throws KeeperException, InterruptedException {
                return zk.exists("/lily", false) != null;
            }
        });

        if (!lilyNodeExists) {
            if (!cmd.hasOption(forceOption.getOpt())) {
                System.out.println("No /lily node found in ZooKeeper. Are you sure you are connecting to the right");
                System.out.println("ZooKeeper? If so, use the option --" + forceOption.getLongOpt() +
                        " to bypass this check.");
                return 1;
            } else {
                System.out.println("No /lily node found in ZooKeeper. Will continue anyway since you supplied --" +
                        forceOption.getLongOpt());
                System.out.println();
            }
        }

        model = new IndexerModelImpl(zk);

        // Perform some extra validation which we can only do now that we have access
        // to the indexer model: check that any specified Solr shard URIs are not the
        // same as those of other indexes.
        if (solrShards != null) {
            Collection<IndexDefinition> indexes = model.getIndexes();
            for (String uri : solrShards.values()) {
                for (IndexDefinition index : indexes) {
                    if (indexName != null && index.getName().equals(indexName)) {
                        continue;
                    }

                    for (String uri2 : index.getSolrShards().values()) {
                        if (uri.equals(uri2)) {
                            System.out.println("The following Solr shard URI:");
                            System.out.println(uri);
                            System.out.println("is already in use by the index " + index.getName());
                            if (!cmd.hasOption(forceOption.getOpt())) {
                                System.out.println("If you are ok with this, use the --" + forceOption.getLongOpt() +
                                        " option to bypass this check.");
                                return 1;
                            } else {
                                System.out.println("Since --" + forceOption.getLongOpt() +
                                        " is specified, this will be ignored.");
                                System.out.println();
                            }
                        }
                    }
                }
            }
        }

        return 0;
    }

    @Override
    protected void cleanup() {
        Closer.close(model);
        Closer.close(zk);
        super.cleanup();
    }

    private String getStates(Enum[] values) {
        StringBuilder builder = new StringBuilder();
        for (Enum value : values) {
            if (builder.length() > 0) {
                builder.append(", ");
            }
            builder.append(value);
        }
        return builder.toString();
    }

    protected OutputStream getOutput() throws FileNotFoundException {
        if (outputFileName == null) {
            return System.out;
        } else {
            return new FileOutputStream(outputFileName);
        }
    }

    protected boolean validateSolrOptions(SolrMode oldSolrMode, SolrMode newSolrMode) {
        if (newSolrMode == SolrMode.CLASSIC) {
            // oldSolrMode==null means we are adding an index.
            // oldSolrMode!=newSolrMode means we are updating the index' mode
            if ((oldSolrMode == null || oldSolrMode != newSolrMode) && (solrShards == null || solrShards.isEmpty())) {
                System.out.println("In solr classic mode, you must specify shards with --" + solrShardsOption.getLongOpt());
                return true;
            }
            if (solrZk != null) {
                System.out.println("Conflicting options used: Solr zookeeper connection cannot be used with solr classic mode");
                return true;
            }
            if (solrCollection != null) {
                System.out.println("Conflicting options used: Solr collection cannot be used with solr classic mode");
                return true;
            }
        } else if (newSolrMode == SolrMode.CLOUD) {
            if (solrShards != null) {
                System.out.println("Conflicting options used: Solr shards cannot be used with solr cloud mode");
                return true;
            }
            if (shardingConfiguration != null) {
                System.out.println("Conflicting options used: Solr shards cannot be used with solr cloud mode");
                return true;
            }
        }
        return false;
    }
}
