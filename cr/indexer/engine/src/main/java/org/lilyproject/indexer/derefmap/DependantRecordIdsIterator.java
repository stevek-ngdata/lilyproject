package org.lilyproject.indexer.derefmap;

import java.io.Closeable;
import java.io.IOException;

import org.lilyproject.repository.api.AbsoluteRecordId;

/**
 * Iterator used to iterate over results of a query on the dereference map.
 *
 *
 */
public interface DependantRecordIdsIterator extends Closeable {
    
    boolean hasNext() throws IOException;

    AbsoluteRecordId next() throws IOException;
}
