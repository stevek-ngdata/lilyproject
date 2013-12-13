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
package org.lilyproject.indexer.hbase.mapper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import com.google.common.base.Optional;
import com.google.common.io.ByteStreams;
import com.ngdata.hbaseindexer.conf.IndexerComponentFactory;
import com.ngdata.hbaseindexer.conf.IndexerConf;
import com.ngdata.hbaseindexer.conf.IndexerConfBuilder;
import com.ngdata.hbaseindexer.conf.IndexerConfException;
import com.ngdata.hbaseindexer.parse.ResultToSolrMapper;
import org.apache.zookeeper.KeeperException;
import org.lilyproject.client.LilyClient;
import org.lilyproject.indexer.model.api.LResultToSolrMapper;
import org.lilyproject.indexer.model.indexerconf.LilyIndexerConf;
import org.lilyproject.indexer.model.indexerconf.LilyIndexerConfBuilder;
import org.lilyproject.repository.api.LRepository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.util.hbase.LilyHBaseSchema;
import org.lilyproject.util.hbase.RepoAndTableUtil;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.zookeeper.ZkConnectException;
import org.lilyproject.util.zookeeper.ZooKeeperImpl;

public class LilyIndexerComponentFactory implements IndexerComponentFactory {

    private byte[] confData;
    private IndexerConf indexerConf;
    private Map<String, String> params;

    @Override
    public void configure(InputStream is, Map<String, String> params) throws IndexerConfException {
        this.params = params;
        read(is);
    }

    @Override
    public IndexerConf createIndexerConf() throws IndexerConfException {
        return indexerConf;
    }

    public void validate(InputStream is) throws IndexerConfException {
        try {
            LilyIndexerConfBuilder.validate(is);
        } catch (org.lilyproject.indexer.model.indexerconf.IndexerConfException e) {
            throw new IndexerConfException(e);
        }
    }

    @Override
    public ResultToSolrMapper createMapper(String indexName) throws IndexerConfException {
        String zookeeperConnectString = params.get(LResultToSolrMapper.ZOOKEEPER_KEY);
        String repositoryName = params.get(LResultToSolrMapper.REPO_KEY);
        ZooKeeperImpl zk = null;
        LilyClient lilyClient = null;
        LRepository lRepository;
        try {
            zk = new ZooKeeperImpl(zookeeperConnectString, 30000);
            lilyClient = new LilyClient(zk);
            if (repositoryName == null) {
                lRepository = lilyClient.getDefaultRepository();
            } else {
                lRepository = lilyClient.getRepository(repositoryName);
            }
        } catch (RepositoryException e) {
            Closer.close(lilyClient);
            Closer.close(zk);
            throw new AssertionError(e);
        } catch (InterruptedException e) {
            Closer.close(lilyClient);
            Closer.close(zk);
            throw new AssertionError(e);
        } catch (IOException e) {
            Closer.close(lilyClient);
            Closer.close(zk);
            throw new AssertionError(e);
        } catch (KeeperException e) {
            Closer.close(lilyClient);
            Closer.close(zk);
            throw new AssertionError(e);
        } catch (ZkConnectException e) {
            Closer.close(lilyClient);
            Closer.close(zk);
            throw new AssertionError(e);
        } finally {
        }

        try {
            LilyIndexerConf lilyIndexerConf = LilyIndexerConfBuilder.build(new ByteArrayInputStream(confData), lRepository);
            LilyResultToSolrMapper mapper = new LilyResultToSolrMapper(indexName, lilyIndexerConf, lilyClient, zk);
            mapper.configure(params);
            return mapper;
        } catch (org.lilyproject.indexer.model.indexerconf.IndexerConfException e) {
            Closer.close(lilyClient);
            Closer.close(zk);
            throw new IndexerConfException(e);
        }
    }

    public void read(InputStream is) throws IndexerConfException {
        try {
            this.confData = ByteStreams.toByteArray(is);

            validate(new ByteArrayInputStream(confData));
            IndexerConfBuilder builder = new IndexerConfBuilder();

            String zkParam = params.get(LResultToSolrMapper.ZOOKEEPER_KEY);
            if (zkParam == null) {
                throw new IndexerConfException("The required connection parameter " + LilyResultToSolrMapper.ZOOKEEPER_KEY + " is not set.");
            }
            String repoParam= Optional.fromNullable(params.get(LResultToSolrMapper.REPO_KEY)).or(RepoAndTableUtil.DEFAULT_REPOSITORY);
            String tableParam = Optional.fromNullable(params.get(LResultToSolrMapper.TABLE_KEY)).or(LilyHBaseSchema.Table.RECORD.name);
            String tableName;
            if (repoParam.equals("default")) {
                tableName = tableParam;
            } else {
                tableName = repoParam.concat("__").concat(tableParam);
            }
            builder.table(tableName);

            // TODO: Do any of these need to be configurable given the Lily context?
            builder.mappingType(null);
            builder.rowReadMode(null);
            builder.uniqueyKeyField("lily.key");
            builder.uniqueKeyFormatterClass(null);
            builder.rowField(null);
            builder.columnFamilyField(null);
            builder.tableNameField(null);

            indexerConf = builder.build();
        } catch (Exception e) {
            throw new IndexerConfException("Problems initializing the indexer components", e);
        }
    }

}
