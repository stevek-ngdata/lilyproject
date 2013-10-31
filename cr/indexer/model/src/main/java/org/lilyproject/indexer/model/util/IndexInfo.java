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
package org.lilyproject.indexer.model.util;

import com.ngdata.hbaseindexer.conf.IndexerConf;
import com.ngdata.hbaseindexer.model.api.IndexerDefinition;
import org.lilyproject.indexer.model.api.LResultToSolrMapper;
import org.lilyproject.indexer.model.indexerconf.IndexerConfBuilder;
import org.lilyproject.indexer.model.indexerconf.IndexerConfException;
import org.lilyproject.repository.api.LRepository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.RepositoryManager;

import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;

public class IndexInfo {
    IndexerDefinition indexDefinition;
    IndexerConf indexerConf;
    org.lilyproject.indexer.model.indexerconf.IndexerConf lilyIndexerConf;

    public IndexInfo(IndexerDefinition indexDefinition, IndexerConf indexerConf, RepositoryManager repositoryManager)
            throws IndexerConfException, RepositoryException, InterruptedException {
        this.indexDefinition = indexDefinition;
        this.indexerConf = indexerConf;

        LRepository repository = indexerConf.getGlobalParams().containsKey(LResultToSolrMapper.REPO_KEY) ?
                repositoryManager.getRepository(indexerConf.getGlobalParams().get(LResultToSolrMapper.REPO_KEY)) :
                repositoryManager.getDefaultRepository();
        try {
            ByteArrayInputStream is = new ByteArrayInputStream(
                    indexerConf.getGlobalParams().get(LResultToSolrMapper.INDEXERCONF_KEY).getBytes("UTF-8"));
            this.lilyIndexerConf = IndexerConfBuilder.build(is, repository);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public IndexerDefinition getIndexDefinition() {
        return indexDefinition;
    }

    public IndexerConf getIndexerConf() {
        return indexerConf;
    }

    public org.lilyproject.indexer.model.indexerconf.IndexerConf getLilyIndexerConf() {
        return lilyIndexerConf;
    }
}
