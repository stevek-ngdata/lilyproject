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
package org.lilyproject.indexer.engine;

import java.io.IOException;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.indexer.IndexerException;
import org.lilyproject.indexer.model.indexerconf.IndexCase;
import org.lilyproject.indexer.model.sharding.ShardSelectorException;
import org.lilyproject.repository.api.IdRecord;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.RepositoryManager;

/**
 *
 */
public class IndexerApiImpl implements org.lilyproject.indexer.Indexer {
    private RepositoryManager repositoryManager;

    private IndexerRegistry indexerRegistry;

    private Log log = LogFactory.getLog(getClass());

    public IndexerApiImpl(RepositoryManager repositoryManager, IndexerRegistry indexerRegistry) {
        this.repositoryManager = repositoryManager;
        this.indexerRegistry = indexerRegistry;
    }

    @Override
    public void index(String table, RecordId recordId) throws IndexerException, InterruptedException {
        final IdRecord idRecord = tryReadRecord(table, recordId);

        if (indexerRegistry.getAllIndexers().isEmpty()) {
            log.warn("cannot index record [" + recordId + "] because there are no known indexes");
        }

        boolean matched = false;
        for (Indexer indexer : indexerRegistry.getAllIndexers()) {
            final IndexCase indexCase = indexer.getConf().getRecordFilter().getIndexCase(table, idRecord);
            if (indexCase != null) {
                matched = true;
                tryIndex(indexer, table, idRecord, indexCase);
            }
        }

        if (!matched) {
            log.warn("cannot index record [" + recordId + "] because it didn't match the record filter of any index");
        }
    }

    @Override
    public void indexOn(String table, RecordId recordId, Set<String> indexes) throws IndexerException, InterruptedException {
        for (String indexName : indexes) {
            final org.lilyproject.indexer.engine.Indexer indexer = indexerRegistry.getIndexer(indexName);
            if (indexer == null) {
                throw new IndexerException("index " + indexName + " could not be found");
            } else {
                final IdRecord idRecord = tryReadRecord(table, recordId);
                final IndexCase indexCase = indexer.getConf().getRecordFilter().getIndexCase(table, idRecord);
                if (indexCase != null) // it matches -> index
                {
                    tryIndex(indexer, table, idRecord, indexCase);
                } else // it doesn't match -> explicitly delete
                {
                    tryDelete(indexer, recordId);
                }
            }
        }
    }

    private IdRecord tryReadRecord(String table, RecordId recordId) throws IndexerException, InterruptedException {
        try {
            return repositoryManager.getTable(table).readWithIds(recordId, null, null);
        } catch (RepositoryException e) {
            throw new IndexerException("failed to read from repository", e);
        }
    }

    private void tryIndex(Indexer indexer, String table, IdRecord idRecord, IndexCase indexCase)
            throws InterruptedException, IndexerException {
        try {
            indexer.index(table, idRecord, indexCase.getVersionTags());
        } catch (SolrClientException e) {
            throw new IndexerException("failed to index on solr", e);
        } catch (ShardSelectorException e) {
            throw new IndexerException("failed to select shard", e);
        } catch (IOException e) {
            throw new IndexerException(e);
        } catch (RepositoryException e) {
            throw new IndexerException("problem with repository", e);
        }
    }

    private void tryDelete(Indexer indexer, RecordId recordId)
            throws InterruptedException, IndexerException {
        try {
            indexer.delete(recordId);
        } catch (SolrClientException e) {
            throw new IndexerException("failed to delete on solr", e);
        } catch (ShardSelectorException e) {
            throw new IndexerException("failed to select shard", e);
        }
    }

}
