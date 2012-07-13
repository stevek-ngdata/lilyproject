package org.lilyproject.indexer.derefmap;

import java.io.Closeable;
import java.io.IOException;

import org.lilyproject.repository.api.RecordId;

/**
* @author Jan Van Besien
*/
public interface DependantRecordIdsIterator extends Closeable {
    boolean hasNext() throws IOException;

    RecordId next() throws IOException;
}
