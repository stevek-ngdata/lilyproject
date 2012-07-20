package org.lilyproject.indexer;

import java.util.Set;

import org.lilyproject.repository.api.RecordId;

/**
 * The indexer provides access to the indexing functionality of Lily. It allows things like explicitely indexing a
 * record.
 *
 *
 */
public interface Indexer {

    /**
     * Synchronously trigger the indexing of the record identified by the given {@link
     * org.lilyproject.repository.api.RecordId}. The indexing will happen on all indexes with a configuration that
     * matches the current state of the record. If indexing fails on one of the indexes, the implementation
     * will fail fast (throws an exception) and will not continue with other matching indexes. Note that clients can
     * retry the whole operation.
     *
     * @param recordId identification of the record to index
     */
    void index(RecordId recordId) throws IndexerException, InterruptedException;

    /**
     * Synchronously trigger the indexing of the record identified by the given {@link
     * org.lilyproject.repository.api.RecordId} on the requested indexes. The indexing will happen on all requested
     * indexes. If indexing fails on one of the indexes, or if one of the indexes doesn't exist, the implementation
     * will fail fast (throws an exception) and will not continue with other requested indexes. Note that clients can
     * retry the whole operation.
     *
     * @param recordId identification of the record to index
     * @param indexes  names of the indexes on which to trigger indexing
     */
    void indexOn(RecordId recordId, Set<String> indexes) throws IndexerException, InterruptedException;

}
