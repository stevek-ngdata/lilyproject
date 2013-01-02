package org.lilyproject.indexer.derefmap;

import java.io.IOException;

import org.lilyproject.hbaseindex.QueryResult;
import org.lilyproject.repository.api.RecordId;

/**
 * Implementation of {@link org.lilyproject.indexer.derefmap.DependantRecordIdsIterator}.
 */
final class DependantRecordIdsIteratorImpl implements DependantRecordIdsIterator {
    private final QueryResult queryResult;
    private final DerefMapSerializationUtil serializationUtil;

    DependantRecordIdsIteratorImpl(QueryResult queryResult, DerefMapSerializationUtil serializationUtil) {
        this.queryResult = queryResult;
        this.serializationUtil = serializationUtil;
    }

    @Override
    public void close() throws IOException {
        queryResult.close();
    }

    RecordId next = null;

    private RecordId getNextFromQueryResult() throws IOException {
        // the identifier is the record id of the record that depends on the queried record

        final byte[] nextIdentifier = queryResult.next();
        if (nextIdentifier == null) {
            return null;
        } else {
            return serializationUtil.deserializeRecordId(nextIdentifier);
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        synchronized (this) { // to protect setting/resetting the next value from race conditions
            if (next != null) {
                // the next was already set, but not yet used
                return true;
            } else {
                // try setting a next value
                next = getNextFromQueryResult();
                return next != null;
            }
        }
    }

    @Override
    public RecordId next() throws IOException {
        synchronized (this) { // to protect setting/resetting the next value from race conditions
            if (next != null) {
                // the next was already set, but not yet used
                RecordId nextToReturn = next;
                next = null;
                return nextToReturn;
            } else {
                // try setting a next value
                next = getNextFromQueryResult();
                return next;
            }
        }
    }
}
