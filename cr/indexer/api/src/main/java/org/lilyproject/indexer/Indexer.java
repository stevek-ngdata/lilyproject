package org.lilyproject.indexer;

import java.util.Set;

import org.lilyproject.repository.api.RecordId;

/**
 * The indexer provides access to the indexing functionality of Lily. It allows things like explicitly indexing a
 * record.
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
     * <p>Synchronous indexing only indexes the specified record: it does not update information denormalized
     * into the index entries of other records.</p>
     *
     * @param recordId identification of the record to index
     */
    void index(RecordId recordId) throws IndexerException, InterruptedException;

    /**
     * Synchronously trigger the indexing of the record identified by the given {@link
     * org.lilyproject.repository.api.RecordId} on the requested indexes. The indexing will happen on all requested
     * indexes, even if the record filter does not match (in which case a delete will be triggered on the index).
     * If indexing fails on one of the indexes, or if one of the indexes doesn't exist, the implementation
     * will fail fast (throws an exception) and will not continue with other requested indexes. Note that clients can
     * retry the whole operation.
     *
     * @param recordId identification of the record to index
     * @param indexes  names of the indexes on which to trigger indexing
     */
    void indexOn(RecordId recordId, Set<String> indexes) throws IndexerException, InterruptedException;

}
