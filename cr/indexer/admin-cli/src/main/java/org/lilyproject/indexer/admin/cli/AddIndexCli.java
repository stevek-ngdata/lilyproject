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

import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.lilyproject.indexer.model.api.IndexDefinition;

public class AddIndexCli extends BaseIndexerAdminCli {
    @Override
    protected String getCmdName() {
        return "lily-add-index";
    }

    public static void main(String[] args) {
        new AddIndexCli().start(args);
    }

    @Override
    public List<Option> getOptions() {
        List<Option> options = super.getOptions();

        options.add(nameOption);
        options.add(solrShardsOption);
        options.add(shardingConfigurationOption);
        options.add(configurationOption);
        options.add(generalStateOption);
        options.add(updateStateOption);
        options.add(buildStateOption);
        options.add(forceOption);
        options.add(defaultBatchIndexConfigurationOption);
        options.add(solrCollectionOption);
        options.add(solrZkOption);
        options.add(solrModeOption);
        options.add(enableDerefMapOption);

        return options;
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        int result = super.run(cmd);
        if (result != 0)
            return result;

        if (indexName == null) {
            System.out.println("Specify index name with -" + nameOption.getOpt());
            return 1;
        }

        if (solrMode == null) {
            System.out.println("You must specify the solr mode (cloud or classic) with " + solrModeOption.getLongOpt());
            return 1;
        }

        if (indexerConfiguration == null) {
            System.out.println("Specify indexer configuration with -" + configurationOption.getOpt());
        }

        if (validateSolrOptions(null, solrMode)) return 1;

        IndexDefinition index = model.newIndex(indexName);
        if (solrMode == SolrMode.CLASSIC) {
            index.setSolrShards(solrShards);

            if (shardingConfiguration != null)
                index.setShardingConfiguration(shardingConfiguration);

        } else if (solrMode == SolrMode.CLOUD) {
            index.setZkConnectionString(solrZk != null ? solrZk : (this.zkConnectionString + "/solr"));

            if (solrCollection != null) {
                index.setSolrCollection(this.solrCollection);
            }
        }

        index.setConfiguration(indexerConfiguration);

        if (generalState != null)
            index.setGeneralState(generalState);

        if (updateState != null)
            index.setUpdateState(updateState);

        if (buildState != null)
            index.setBatchBuildState(buildState);

        if (defaultBatchIndexConfiguration != null)
            index.setDefaultBatchIndexConfiguration(defaultBatchIndexConfiguration);

        if (batchIndexConfiguration != null)
            index.setBatchIndexConfiguration(batchIndexConfiguration);

        if (enableDerefMap != null)
            index.setEnableDerefMap(enableDerefMap);
        else
            index.setEnableDerefMap(true); // default true

        model.addIndex(index);

        System.out.println("Index created: " + indexName);

        return 0;
    }

}
