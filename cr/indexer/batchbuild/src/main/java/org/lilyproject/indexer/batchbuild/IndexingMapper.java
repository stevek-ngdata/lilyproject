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
package org.lilyproject.indexer.batchbuild;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import net.iharder.Base64;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.solr.client.solrj.SolrServerException;
import org.lilyproject.client.LilyClient;
import org.lilyproject.indexer.engine.DerefMap;
import org.lilyproject.indexer.engine.DerefMapHbaseImpl;
import org.lilyproject.indexer.engine.IndexLocker;
import org.lilyproject.indexer.engine.Indexer;
import org.lilyproject.indexer.engine.IndexerMetrics;
import org.lilyproject.indexer.engine.SolrClientConfig;
import org.lilyproject.indexer.engine.SolrShardManager;
import org.lilyproject.indexer.engine.SolrShardManagerImpl;
import org.lilyproject.indexer.model.indexerconf.IndexerConf;
import org.lilyproject.indexer.model.indexerconf.IndexerConfBuilder;
import org.lilyproject.indexer.model.sharding.DefaultShardSelectorBuilder;
import org.lilyproject.indexer.model.sharding.JsonShardSelectorBuilder;
import org.lilyproject.indexer.model.sharding.ShardSelector;
import org.lilyproject.mapreduce.IdRecordMapper;
import org.lilyproject.mapreduce.IdRecordWritable;
import org.lilyproject.mapreduce.LilyMapReduceUtil;
import org.lilyproject.mapreduce.RecordIdWritable;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.IdRecord;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.zookeeper.ZkUtil;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

public class IndexingMapper extends IdRecordMapper<ImmutableBytesWritable, Result> {
    private Indexer indexer;
    private MultiThreadedHttpConnectionManager connectionManager;
    private IndexLocker indexLocker;
    private ZooKeeperItf zk;
    private Repository repository;
    private ThreadPoolExecutor executor;
    private Log log = LogFactory.getLog(getClass());

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        try {
            Configuration jobConf = context.getConfiguration();
            
            LilyClient lilyClient = LilyMapReduceUtil.getLilyClient(jobConf);            
            repository = lilyClient.getRepository();  
            
            String zkConnectString = jobConf.get("org.lilyproject.indexer.batchbuild.zooKeeperConnectString");
            int zkSessionTimeout =
                    getIntProp("org.lilyproject.indexer.batchbuild.zooKeeperSessionTimeout", null, jobConf);
            zk = ZkUtil.connect(zkConnectString, zkSessionTimeout);

            byte[] indexerConfBytes = Base64.decode(jobConf.get("org.lilyproject.indexer.batchbuild.indexerconf"));
            IndexerConf indexerConf = IndexerConfBuilder.build(new ByteArrayInputStream(indexerConfBytes), repository);

            Map<String, String> solrShards = new HashMap<String, String>();
            for (int i = 1; true; i++) {
                String shardName = jobConf.get("org.lilyproject.indexer.batchbuild.solrshard.name." + i);
                String shardAddress = jobConf.get("org.lilyproject.indexer.batchbuild.solrshard.address." + i);
                if (shardName == null)
                    break;
                solrShards.put(shardName, shardAddress);
            }

            ShardSelector shardSelector;
            String shardingConf = jobConf.get("org.lilyproject.indexer.batchbuild.shardingconf");
            if (shardingConf != null) {
                byte[] shardingConfBytes = Base64.decode(shardingConf);
                shardSelector = JsonShardSelectorBuilder.build(shardingConfBytes);
            } else {
                shardSelector = DefaultShardSelectorBuilder.createDefaultSelector(solrShards);
            }

            connectionManager = new MultiThreadedHttpConnectionManager();
            connectionManager.getParams().setDefaultMaxConnectionsPerHost(5);
            connectionManager.getParams().setMaxTotalConnections(50);
            HttpClient httpClient = new HttpClient(connectionManager);

            SolrClientConfig solrConfig = new SolrClientConfig();
            solrConfig.setRequestWriter(jobConf.get("org.lilyproject.indexer.batchbuild.requestwriter", null));
            solrConfig.setResponseParser(jobConf.get("org.lilyproject.indexer.batchbuild.responseparser", null));

            String indexName = "batchjob"; // we should pass on the real index name.

            SolrShardManager solrShardMgr = new SolrShardManagerImpl(indexName, solrShards, shardSelector, httpClient,
                    solrConfig);

            boolean enableLocking =
                    Boolean.parseBoolean(jobConf.get("org.lilyproject.indexer.batchbuild.enableLocking"));

            indexLocker = new IndexLocker(zk, enableLocking);

            final DerefMap derefMap = new DerefMapHbaseImpl(indexName, LilyClient.getHBaseConfiguration(zk), repository.getIdGenerator());
            indexer = new Indexer(indexName, indexerConf, repository, solrShardMgr, indexLocker,
                    new IndexerMetrics(indexName), derefMap);

            int workers = getIntProp("org.lilyproject.indexer.batchbuild.threads", 5, jobConf);

            executor = new ThreadPoolExecutor(workers, workers, 10, TimeUnit.SECONDS,
                    new ArrayBlockingQueue<Runnable>(1000));
            executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());

        } catch (Exception e) {
            throw new IOException("Error in index build map task setup.", e);
        }
    }

    private int getIntProp(String name, Integer defaultValue, Configuration conf) {
        String value = conf.get(name);
        if (value == null) {
            if (defaultValue != null)
                return defaultValue;
            else
                throw new RuntimeException("Missing property in jobconf: " + name);
        }

        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new RuntimeException("Invalid integer value in jobconf property. Property '" + name + "', value: " +
                    value);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        executor.shutdown();
        boolean successfulFinish = executor.awaitTermination(5, TimeUnit.MINUTES);
        if (!successfulFinish) {
            log.error("Executor did not finish outstanding work within the foreseen timeout.");
        }

        Closer.close(connectionManager);
        Closer.close(repository);
        super.cleanup(context);
        Closer.close(zk);
    }

    @Override
    public void map(RecordIdWritable recordIdWritable, IdRecordWritable recordWritable, Context context)
            throws IOException, InterruptedException {
        executor.submit(new MappingTask(recordWritable.getRecord(), context));
    }

    public class MappingTask implements Runnable {
        private IdRecord idRecord;
        private Context context;

        private MappingTask(IdRecord idRecord, Context context) {
            this.idRecord = idRecord;
            this.context = context;
        }

        @Override
        public void run() {
            boolean locked = false;
            RecordId recordId = idRecord.getId();
            try {
                indexLocker.lock(recordId);
                locked = true;
                indexer.index(idRecord);
            } catch (Throwable t) {
                context.getCounter(IndexBatchBuildCounters.NUM_FAILED_RECORDS).increment(1);

                // Avoid printing a complete stack trace for common errors.
                if (t instanceof SolrServerException &&
                        t.getMessage().equals("java.net.ConnectException: Connection refused")) {
                    log.error("Failure indexing record " + recordId + ": Solr connection refused.");
                } else {
                    log.error("Failure indexing record " + recordId, t);
                }

            } finally {
                if (locked) {
                    indexLocker.unlockLogFailure(recordId);
                }
            }
        }
    }

}
