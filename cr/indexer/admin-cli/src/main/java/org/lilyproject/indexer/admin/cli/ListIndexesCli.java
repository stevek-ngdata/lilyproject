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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;
import org.lilyproject.indexer.model.api.ActiveBatchBuildInfo;
import org.lilyproject.indexer.model.api.BatchBuildInfo;
import org.lilyproject.indexer.model.api.IndexDefinition;
import org.lilyproject.indexer.model.api.IndexDefinitionNameComparator;
import org.lilyproject.util.json.JsonFormat;

public class ListIndexesCli extends BaseIndexerAdminCli {
    @Override
    protected String getCmdName() {
        return "lily-list-indexes";
    }

    public static void main(String[] args) {
        new ListIndexesCli().start(args);
    }

    @Override
    public List<Option> getOptions() {
        List<Option> options = super.getOptions();
        options.add(this.printBatchConfigurationOption);
        options.add(this.printShardingConfigurationOption);
        return options;
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        int result = super.run(cmd);
        if (result != 0) {
            return result;
        }

        List<IndexDefinition> indexes = new ArrayList<IndexDefinition>(model.getIndexes());
        Collections.sort(indexes, IndexDefinitionNameComparator.INSTANCE);

        System.out.println("Number of indexes: " + indexes.size());
        System.out.println();

        for (IndexDefinition index : indexes) {
            System.out.println(index.getName());

            if (index.getRepositoryName() != null){
                System.out.println("  + Repository: " + index.getRepositoryName());
            }

            System.out.println("  + General state: " + index.getGeneralState());
            System.out.println("  + Update state: " + index.getUpdateState());
            System.out.println("  + Batch build state: " + index.getBatchBuildState());
            System.out.println("  + Queue subscription ID: " + index.getQueueSubscriptionId());
            if (!index.isEnableDerefMap()) {
                System.out.println("  + Dereference Map: disabled");
            }
            if (index.getSolrShards() != null && !index.getSolrShards().isEmpty()) {
                System.out.println("  + Solr Mode: CLASSIC");
                System.out.println("    + Solr shards:");
                for (Map.Entry<String, String> shard : index.getSolrShards().entrySet()) {
                    System.out.println("      + " + shard.getKey() + ": " + shard.getValue());
                }
                if (printShardingConfiguration) {
                    System.out.println("    + Sharding configuration: " + prettyPrintJson(index.getShardingConfiguration(), 6));
                }
            } else {
                System.out.println("  + Solr Mode: CLOUD");
                System.out.println("    + Solr zookeeper: " + index.getZkConnectionString());
                System.out.println("    + Solr collection: " +
                        (index.getSolrCollection() != null ? index.getSolrCollection() : "none (using solr default)"));
            }

            if (this.printBatchConfiguration) {
                System.out.println("  + Default batch build config : " +
                        prettyPrintJson(index.getDefaultBatchIndexConfiguration(), 4));
            }

            ActiveBatchBuildInfo activeBatchBuild = index.getActiveBatchBuildInfo();
            if (activeBatchBuild != null) {
                System.out.println("  + Active batch build:");
                System.out.println("    + Hadoop Job ID: " + activeBatchBuild.getJobId());
                System.out.println("    + Submitted at: " + new DateTime(activeBatchBuild.getSubmitTime()).toString());
                System.out.println("    + Tracking URL: " + activeBatchBuild.getTrackingUrl());
                if (this.printBatchConfiguration) {
                    System.out.println("    + Batch build config : " +
                            prettyPrintJson(activeBatchBuild.getBatchIndexConfiguration(), 6));
                }
            }

            BatchBuildInfo lastBatchBuild = index.getLastBatchBuildInfo();
            if (lastBatchBuild != null) {
                System.out.println("  + Last batch build:");
                System.out.println("    + Hadoop Job ID: " + lastBatchBuild.getJobId());
                System.out.println("    + Submitted at: " + new DateTime(lastBatchBuild.getSubmitTime()).toString());
                System.out.println("    + Success: " + successMessage(lastBatchBuild));
                System.out.println("    + Job state: " + lastBatchBuild.getJobState());
                System.out.println("    + Tracking URL: " + lastBatchBuild.getTrackingUrl());
                Map<String, Long> counters = lastBatchBuild.getCounters();
                System.out.println("    + Map input records: " + counters.get(COUNTER_MAP_INPUT_RECORDS));
                System.out.println("    + Launched map tasks: " + counters.get(COUNTER_TOTAL_LAUNCHED_MAPS));
                System.out.println("    + Failed map tasks: " + counters.get(COUNTER_NUM_FAILED_MAPS));
                System.out.println("    + Index failures: " + counters.get(COUNTER_NUM_FAILED_RECORDS));
                if (this.printBatchConfiguration) {
                    System.out.println("    + Batch build config : " +
                            prettyPrintJson(lastBatchBuild.getBatchIndexConfiguration(), 6));
                }
            }
            if (index.getBatchTables() != null) {
                System.out.println("  + Batch build tables: " + index.getBatchTables());
            }
            if (index.getDefaultBatchTables() != null) {
                System.out.println("  + Default batch build tables: " + index.getDefaultBatchTables());
            }
        }

        return 0;
    }

    private String successMessage(BatchBuildInfo buildInfo) {
        StringBuilder result = new StringBuilder();
        result.append(buildInfo.getSuccess());

        Long failedRecords = buildInfo.getCounters().get(COUNTER_NUM_FAILED_RECORDS);
        if (failedRecords != null && failedRecords > 0) {
            result.append(", ").append(buildInfo.getSuccess() ? "but " : "").append(failedRecords)
                    .append(" index failures");
        }

        return result.toString();
    }

    private String prettyPrintJson(byte[] conf, int extraIndent) throws Exception {
        if (conf == null) {
            return "null";
        }
        JsonNode node = JsonFormat.deserializeNonStd(conf);
        char[] padding = new char[extraIndent];
        Arrays.fill(padding, ' ');
        StringBuilder output = new StringBuilder();
        output.append("\n");
        output.append(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(node));

        return output.toString().replaceAll("\n", "\n" + new String(padding));
    }

    private static final String COUNTER_MAP_INPUT_RECORDS = "org.apache.hadoop.mapred.Task$Counter:MAP_INPUT_RECORDS";
    private static final String COUNTER_TOTAL_LAUNCHED_MAPS =
            "org.apache.hadoop.mapred.JobInProgress$Counter:TOTAL_LAUNCHED_MAPS";
    private static final String COUNTER_NUM_FAILED_MAPS =
            "org.apache.hadoop.mapred.JobInProgress$Counter:NUM_FAILED_MAPS";
    private static final String COUNTER_NUM_FAILED_RECORDS =
            "org.lilyproject.indexer.batchbuild.IndexBatchBuildCounters:NUM_FAILED_RECORDS";
}