package org.lilyproject.repository.api;

import java.io.Closeable;
import java.io.IOException;

/**
 * A RecordScanner allows to sequentially run over all or part of the records
 * stored in the Lily repository.
 * 
 * <p>A scanner can be obtained from {@link Repository#getScanner(RecordScan)}</p>.
 * 
 * <p>RecordScanner's implement Iterable, so you can use a for-each loop
 * directly on them.</p>
 * 
 * <p>When done with a scanner, be sure to call {@link #close()} on them to release
 * the server-side resources.</p>
 */
public interface RecordScanner extends Closeable, Iterable<Record> {
    /**
     * Returns the next record, or null if there are none left.
     */
    Record next() throws RepositoryException, InterruptedException;

    /**
     * Closes this scanner, releasing its server-side resources.
     */
    void close();
}
