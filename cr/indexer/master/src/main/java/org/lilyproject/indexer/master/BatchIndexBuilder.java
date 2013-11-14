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
package org.lilyproject.indexer.master;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Map;

import net.iharder.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.codehaus.jackson.JsonNode;
import org.lilyproject.hbaseindex.IndexNotFoundException;
import org.lilyproject.indexer.batchbuild.IndexingMapper;
import org.lilyproject.indexer.derefmap.DerefMapHbaseImpl;
import org.lilyproject.indexer.engine.SolrClientConfig;
import org.lilyproject.indexer.model.api.IndexDefinition;
import org.lilyproject.indexer.model.indexerconf.IndexerConf;
import org.lilyproject.indexer.model.indexerconf.IndexerConfBuilder;
import org.lilyproject.mapreduce.LilyMapReduceUtil;
import org.lilyproject.repository.api.LRepository;
import org.lilyproject.repository.api.RecordScan;
import org.lilyproject.repository.api.ReturnFields;
import org.lilyproject.tools.import_.json.RecordScanReader;
import org.lilyproject.util.hbase.HBaseTableFactory;
import org.lilyproject.util.json.JsonFormat;

public class BatchIndexBuilder {
    private BatchIndexBuilder() {
    }

    /**
     * @return the ID of the started job
     */
    public static Job startBatchBuildJob(IndexDefinition index, Configuration mapReduceConf, Configuration hbaseConf,
                                         LRepository repository, String zkConnectString, int zkSessionTimeout,
                                         SolrClientConfig solrConfig,
                                         byte[] batchIndexConfiguration, boolean enableLocking,
                                         List<String> tableList, HBaseTableFactory tableFactory) throws Exception {

        Configuration conf = new Configuration(mapReduceConf);
        Job job = new Job(conf, "BatchIndexBuild Job");

        //
        // Find and set the MapReduce job jar.
        //
        job.setJarByClass(IndexingMapper.class);
        job.setMapperClass(IndexingMapper.class);

        //
        // Pass information about the index to be built
        //
        job.getConfiguration().set("org.lilyproject.indexer.batchbuild.indexname", index.getName());

        if (! StringUtils.isEmpty(index.getRepositoryName()))
            job.getConfiguration().set("org.lilyproject.indexer.batchbuild.repoName", index.getRepositoryName());

        String indexerConfString = Base64.encodeBytes(index.getConfiguration(), Base64.GZIP);
        job.getConfiguration().set("org.lilyproject.indexer.batchbuild.indexerconf", indexerConfString);

        if (index.getShardingConfiguration() != null) {
            String shardingConfString = Base64.encodeBytes(index.getShardingConfiguration(), Base64.GZIP);
            job.getConfiguration().set("org.lilyproject.indexer.batchbuild.shardingconf", shardingConfString);
        }


        int i = 0;
        for (Map.Entry<String, String> shard : index.getSolrShards().entrySet()) {
            i++;
            job.getConfiguration().set("org.lilyproject.indexer.batchbuild.solrshard.name." + i, shard.getKey());
            job.getConfiguration().set("org.lilyproject.indexer.batchbuild.solrshard.address." + i, shard.getValue());
        }

        if (index.getZkConnectionString() != null) {
            job.getConfiguration()
                    .set("org.lilyproject.indexer.batchbuild.solr.zkConnectionString", index.getZkConnectionString());
        }
        if (index.getSolrCollection() != null) {
            job.getConfiguration().set("org.lilyproject.indexer.batchbuild.solr.collection", index.getSolrCollection());
        }

        job.setNumReduceTasks(0);
        job.setOutputFormatClass(NullOutputFormat.class);

        JsonNode batchConfigurationNode =
                JsonFormat.deserializeNonStd(new ByteArrayInputStream(batchIndexConfiguration));
        RecordScan recordScan = RecordScanReader.INSTANCE.fromJson(batchConfigurationNode.get("scan"), repository);
        recordScan.setReturnFields(ReturnFields.ALL);
        recordScan.setCacheBlocks(false);
        recordScan.setCaching(1024);

        if (batchConfigurationNode.has("clearIndex") && batchConfigurationNode.get("clearIndex").getBooleanValue()) {
            if (index.getSolrShards() == null) {
                CloudSolrServer server = new CloudSolrServer(index.getZkConnectionString());
                server.setDefaultCollection(index.getSolrCollection());
                server.deleteByQuery("*:*");
                server.commit();
            } else {
                // do a delete *:* on all shards
                for (String shard: index.getSolrShards().values()) {
                    HttpSolrServer server = new HttpSolrServer(shard);
                    server.deleteByQuery("*:*");
                    server.commit();
                }
            }
        }

        if (batchConfigurationNode.has("clearDerefMap") &&
                batchConfigurationNode.get("clearDerefMap").asBoolean(false)) {
            try {
                DerefMapHbaseImpl.delete(index.getName(), hbaseConf);
            } catch (IndexNotFoundException e) {
                // If there is no index to delete, keep calm and carry on.
            }
        }

        // Create derefmap table, but only if there are deref expressions.
        // We create the derefmap table already here, because here we have knowledge of the table
        // creation preferences (otherwise would need to serialize that config towards the mappers).
        // This also requires to parse the indexerconf, to know if we actually need a derefmap.
        IndexerConf indexerConf = IndexerConfBuilder.build(new ByteArrayInputStream(index.getConfiguration()),
                repository);
        if (indexerConf.containsDerefExpressions()) {
            DerefMapHbaseImpl.create(repository.getRepositoryName(), index.getName(), hbaseConf,
                    tableFactory, repository.getIdGenerator());
        }

        job.getConfiguration().set("hbase.zookeeper.quorum", hbaseConf.get("hbase.zookeeper.quorum"));
        job.getConfiguration().set("hbase.zookeeper.property.clientPort",
                hbaseConf.get("hbase.zookeeper.property.clientPort"));

        LilyMapReduceUtil.initMapperJob(recordScan, true, zkConnectString, repository, job, tableList);

        //
        // Provide Lily ZooKeeper props
        //
        job.getConfiguration().set("org.lilyproject.indexer.batchbuild.zooKeeperConnectString", zkConnectString);
        job.getConfiguration().set("org.lilyproject.indexer.batchbuild.zooKeeperSessionTimeout",
                String.valueOf(zkSessionTimeout));

        //
        // Solr options
        //
        if (solrConfig.getRequestWriter() != null) {
            job.getConfiguration().set("org.lilyproject.indexer.batchbuild.requestwriter",
                    solrConfig.getRequestWriter());
        }
        if (solrConfig.getResponseParser() != null) {
            job.getConfiguration().set("org.lilyproject.indexer.batchbuild.responseparser",
                    solrConfig.getResponseParser());
        }

        //
        // Other props
        //
        job.getConfiguration().set("org.lilyproject.indexer.batchbuild.enableLocking", String.valueOf(enableLocking));

        job.submit();

        return job;
    }
}
