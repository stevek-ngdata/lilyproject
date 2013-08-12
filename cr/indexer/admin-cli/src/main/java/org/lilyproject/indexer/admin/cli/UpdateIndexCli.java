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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.lilyproject.indexer.model.api.IndexDefinition;
import org.lilyproject.indexer.model.api.IndexGeneralState;
import org.lilyproject.util.ObjectUtils;

public class UpdateIndexCli extends BaseIndexerAdminCli {
    @Override
    protected String getCmdName() {
        return "lily-update-index";
    }

    public static void main(String[] args) {
        new UpdateIndexCli().start(args);
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
        options.add(batchIndexConfigurationOption);
        options.add(defaultBatchIndexTablesOption);
        options.add(batchIndexTablesOption);
        options.add(solrCollectionOption);
        options.add(solrZkOption);
        options.add(solrModeOption);
        options.add(enableDerefMapOption);
        options.add(batchIndexTablesOption);
        options.add(repositoryNameOption);

        return options;
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        int result = super.run(cmd);
        if (result != 0) {
            return result;
        }

        if (indexName == null) {
            System.out.println("Specify index name with -" + nameOption.getOpt());
            return 1;
        }

        if (!model.hasIndex(indexName)) {
            System.out.println("Index does not exist: " + indexName);
            return 1;
        }

        String lock = model.lockIndex(indexName);
        try {
            IndexDefinition index = model.getMutableIndex(indexName);

            boolean changes = false;

            SolrMode oldSolrMode =  SolrMode.CLOUD;
            if (index.getSolrShards() != null && !index.getSolrShards().isEmpty()) {
                oldSolrMode = SolrMode.CLASSIC;
            }
            if (solrMode == null) {
                solrMode = oldSolrMode;
            }

            if (validateSolrOptions(oldSolrMode, solrMode)) {
                return 1;
            }

            if (solrMode != oldSolrMode) {
                changes = true;

                if (solrMode == SolrMode.CLASSIC) {
                    // clear solr cloud settings
                    index.setZkConnectionString(null);
                    index.setSolrCollection(null);

                    // shards will be set later
                } else {
                    // clear solr classic settings
                    index.setSolrShards(Collections.<String, String>emptyMap());
                    index.setShardingConfiguration(null);

                    // set the required parameter for solr mode
                    index.setZkConnectionString(solrZk != null ? solrZk : (this.zkConnectionString + "/solr"));
                }
            }

            if (solrShards != null && !solrShards.isEmpty() && !solrShards.equals(index.getSolrShards())) {
                index.setSolrShards(solrShards);
                changes = true;
            }

            if (index.getSolrShards() == null || index.getSolrShards().isEmpty()) {
                // cloud mode => only respond to solr cloud specific options
                if (solrZk != null && !this.solrZk.equals(index.getZkConnectionString())) {
                    index.setZkConnectionString(this.solrZk);
                    changes = true;
                }

                if (this.solrCollection != null && !this.solrCollection.equals(index.getSolrCollection())) {
                    index.setSolrCollection(this.solrCollection);
                    changes = true;
                }
            } else {
                // classic mode => only respond to solr classic specific options
                if (setShardingConfiguration(shardingConfiguration, index)) {
                    changes = true;
                }
            }

            if (indexerConfiguration != null && !Arrays.equals(indexerConfiguration, index.getConfiguration())) {
                index.setConfiguration(indexerConfiguration);
                changes = true;
            }

            if (generalState != null && generalState != index.getGeneralState()) {
                index.setGeneralState(generalState);
                changes = true;
            }

            if (updateState != null && updateState != index.getUpdateState()) {
                index.setUpdateState(updateState);
                changes = true;
            }

            if (buildState != null && buildState != index.getBatchBuildState()) {
                index.setBatchBuildState(buildState);
                changes = true;
            }

            if (defaultBatchIndexConfiguration != null && !ObjectUtils.safeEquals(defaultBatchIndexConfiguration, index.getDefaultBatchIndexConfiguration())) {
                if (defaultBatchIndexConfiguration.length == 0) {
                    index.setDefaultBatchIndexConfiguration(null);
                } else {
                    index.setDefaultBatchIndexConfiguration(defaultBatchIndexConfiguration);
                }

                changes = true;
            }

            if (batchIndexConfiguration != null) {
                index.setBatchIndexConfiguration(batchIndexConfiguration);
                changes = true;
            }

            if (batchIndexTables != null && !batchIndexTables.equals(index.getBatchTables())) {
                index.setBatchTables(batchIndexTables);
                changes = true;
            }

            if (defaultBatchIndexTables != null && !defaultBatchIndexTables.equals(index.getDefaultBatchTables())) {
                index.setDefaultBatchTables(defaultBatchIndexTables);
                changes = true;
            }

            if (enableDerefMap != null && enableDerefMap != index.isEnableDerefMap()) {
                index.setEnableDerefMap(enableDerefMap);
                changes = true;
            }

            if (repositoryName != null && !repositoryName.equals(index.getRepositoryName())){
                index.setRepositoryName(repositoryName);
                changes = true;
            }

            if (changes) {
                model.updateIndex(index, lock);
                System.out.println("Index updated: " + indexName);
            } else {
                System.out.println("Index already matches the specified settings, did not update it.");
            }


        } finally {
            // In case we requested deletion of an index, it might be that the lock is already removed
            // by the time we get here as part of the index deletion.
            boolean ignoreMissing = generalState != null && generalState == IndexGeneralState.DELETE_REQUESTED;
            model.unlockIndex(lock, ignoreMissing);
        }

        return 0;
    }

    /**
     * Sets the sharding configuration, returning true if any actual changes were made
     * @param conf
     * @param index
     */
    private boolean setShardingConfiguration(byte[] conf, IndexDefinition index) {
        if (conf != null) {
            if (index.getShardingConfiguration() == null) {
                if (!ObjectUtils.safeEquals(conf, index.getShardingConfiguration())) {
                    index.setShardingConfiguration(conf);
                    return true;
                }
            } else {
                if (conf.length == 0) {
                    index.setShardingConfiguration(null);
                    return true;
                }
            }
        }
        return false;
    }

}
