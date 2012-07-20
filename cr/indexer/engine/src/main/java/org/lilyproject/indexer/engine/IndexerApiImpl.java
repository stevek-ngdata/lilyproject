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
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;

/**
 *
 */
public class IndexerApiImpl implements org.lilyproject.indexer.Indexer {
    private Repository repository;

    private IndexerRegistry indexerRegistry;

    private Log log = LogFactory.getLog(getClass());

    public IndexerApiImpl(Repository repository, IndexerRegistry indexerRegistry) {
        this.repository = repository;
        this.indexerRegistry = indexerRegistry;
    }

    @Override
    public void index(RecordId recordId) throws IndexerException, InterruptedException {
        final IdRecord idRecord = tryReadRecord(recordId);

        if (indexerRegistry.getAllIndexers().isEmpty()) {
            log.warn("cannot index record [" + recordId + "] because there are no known indexes");
        }

        boolean matched = false;
        for (Indexer indexer : indexerRegistry.getAllIndexers()) {
            final IndexCase indexCase = indexer.getConf().getRecordFilter().getIndexCase(idRecord);
            if (indexCase != null) {
                matched = true;
                tryIndex(indexer, idRecord, indexCase);
            }
        }

        if (!matched) {
            log.warn("cannot index record [" + recordId + "] because it didn't match the record filter of any index");
        }
    }

    @Override
    public void indexOn(RecordId recordId, Set<String> indexes) throws IndexerException, InterruptedException {
        for (String indexName : indexes) {
            final org.lilyproject.indexer.engine.Indexer indexer = indexerRegistry.getIndexer(indexName);
            if (indexer == null) {
                throw new IndexerException("index " + indexName + " could not be found");
            } else {
                final IdRecord idRecord = tryReadRecord(recordId);
                final IndexCase indexCase = indexer.getConf().getRecordFilter().getIndexCase(idRecord);
                if (indexCase != null) // it matches -> index
                    tryIndex(indexer, idRecord, indexCase);
                else // it doesn't match -> explicitly delete
                    tryDelete(indexer, recordId);
            }
        }
    }

    private IdRecord tryReadRecord(RecordId recordId) throws IndexerException, InterruptedException {
        try {
            return repository.readWithIds(recordId, null, null);
        } catch (RepositoryException e) {
            throw new IndexerException("failed to read from repository", e);
        }
    }

    private void tryIndex(Indexer indexer, IdRecord idRecord, IndexCase indexCase)
            throws InterruptedException, IndexerException {
        try {
            indexer.index(idRecord, indexCase.getVersionTags());
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
