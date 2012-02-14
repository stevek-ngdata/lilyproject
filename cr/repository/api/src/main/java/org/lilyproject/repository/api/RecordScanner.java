package org.lilyproject.repository.api;

import java.io.Closeable;
import java.io.IOException;

public interface RecordScanner extends Closeable, Iterable<Record> {
    Record next() throws RepositoryException, InterruptedException;

    void close();
}
