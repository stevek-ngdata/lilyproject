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
import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;
import java.util.Map;

import net.iharder.Base64;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.codehaus.jackson.JsonNode;
import org.lilyproject.hbaseindex.IndexNotFoundException;
import org.lilyproject.indexer.batchbuild.IndexingMapper;
import org.lilyproject.indexer.derefmap.DerefMapHbaseImpl;
import org.lilyproject.indexer.engine.SolrClientConfig;
import org.lilyproject.indexer.model.api.IndexDefinition;
import org.lilyproject.mapreduce.LilyMapReduceUtil;
import org.lilyproject.repository.api.RecordScan;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.ReturnFields;
import org.lilyproject.tools.import_.json.RecordScanReader;
import org.lilyproject.util.json.JsonFormat;

public class BatchIndexBuilder {
    /**
<<<<<<< HEAD
     *
=======
>>>>>>> e0932a933af399648d5c66c1c976a9ec1716163f
     * @return the ID of the started job
     */
    public static Job startBatchBuildJob(IndexDefinition index, Configuration mapReduceConf, Configuration hbaseConf,
                                         Repository repository, String zkConnectString, int zkSessionTimeout,
                                         SolrClientConfig solrConfig,
                                         byte[] batchIndexConfiguration, boolean enableLocking) throws Exception {

        Configuration conf = new Configuration(mapReduceConf);
        Job job = new Job(conf, "BatchIndexBuild Job");

        //
        // Find and set the MapReduce job jar.
        //
        Class mapperClass = IndexingMapper.class;
        String jobJar = findContainingJar(mapperClass);
        if (jobJar == null) {
            // TODO
            throw new RuntimeException("Job jar not found for class " + mapperClass);
        }

        job.getConfiguration().set("mapred.jar", jobJar);
        job.setMapperClass(mapperClass);

        //
        // Pass information about the index to be built
        //
        job.getConfiguration().set("org.lilyproject.indexer.batchbuild.indexname", index.getName());
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

        job.setNumReduceTasks(0);
        job.setOutputFormatClass(NullOutputFormat.class);


        JsonNode batchConfigurationNode =
                JsonFormat.deserializeNonStd(new ByteArrayInputStream(batchIndexConfiguration));
        RecordScan recordScan = RecordScanReader.INSTANCE.fromJson(batchConfigurationNode.get("scan"), repository);
        recordScan.setReturnFields(ReturnFields.ALL);
        recordScan.setCacheBlocks(false);
        recordScan.setCaching(1024);

        if (batchConfigurationNode.has("clearDerefMap") &&
                batchConfigurationNode.get("clearDerefMap").asBoolean(false)) {
            try {
                DerefMapHbaseImpl.delete(index.getName(), hbaseConf);
            } catch (IndexNotFoundException e) {
                // If there is no index to delete, keep calm and carry on.
            }
        }

        job.getConfiguration().set("hbase.zookeeper.quorum", hbaseConf.get("hbase.zookeeper.quorum"));
        job.getConfiguration().set("hbase.zookeeper.property.clientPort",
                hbaseConf.get("hbase.zookeeper.property.clientPort"));

        LilyMapReduceUtil.initMapperJob(recordScan, true, zkConnectString, repository, job);

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

    /**
     * This method was copied from Hadoop JobConf (Apache License).
     */
    private static String findContainingJar(Class my_class) {
        ClassLoader loader = my_class.getClassLoader();
        String class_file = my_class.getName().replaceAll("\\.", "/") + ".class";
        try {
            for (Enumeration itr = loader.getResources(class_file); itr.hasMoreElements(); ) {
                URL url = (URL) itr.nextElement();
                if ("jar".equals(url.getProtocol())) {
                    String toReturn = url.getPath();
                    if (toReturn.startsWith("file:")) {
                        toReturn = toReturn.substring("file:".length());
                    }
                    toReturn = URLDecoder.decode(toReturn, "UTF-8");
                    return toReturn.replaceAll("!.*$", "");
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }
}
