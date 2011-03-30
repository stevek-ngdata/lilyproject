package org.lilyproject.rowlock;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;

public interface RowLocker {

    /**
     * Locks the specified row. The lock is only obtained if the returned RowLock is not null.
     *
     * <p>If the row is already locked, this method does not block but returns immediately. It is
     * up to the caller to retry if necessary.
     */
    RowLock lockRow(byte[] rowKey) throws IOException;

    /**
     * Locks the specified row. The lock is only obtained if the returned RowLock is not null.
     *
     * <p>If the row is already locked, this method will retry until the specified timeout has
     * passed. If after that the lock is still not obtained, null is returned.
     */
    RowLock lockRow(byte[] rowKey, long timeout) throws IOException, InterruptedException;

    void unlockRow(RowLock lock) throws IOException;

    boolean isLocked(byte[] rowKey) throws IOException;

    boolean put(Put put, RowLock lock) throws IOException;

    boolean delete(Delete delete, RowLock lock) throws IOException;
}
