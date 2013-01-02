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
package org.lilyproject.rowlock;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;

public interface RowLocker {

    /**
     * Locks the specified row. The lock is only obtained if the returned RowLock is not null.
     * <p/>
     * <p>If the row is already locked, this method does not block but returns immediately. It is
     * up to the caller to retry if necessary.
     */
    RowLock lockRow(byte[] rowKey) throws IOException;

    /**
     * Locks the specified row. The lock is only obtained if the returned RowLock is not null.
     * <p/>
     * <p>If the row is already locked, this method will retry until the specified timeout has
     * passed. If after that the lock is still not obtained, null is returned.
     */
    RowLock lockRow(byte[] rowKey, long timeout) throws IOException, InterruptedException;

    boolean unlockRow(RowLock lock) throws IOException;

    boolean isLocked(byte[] rowKey) throws IOException;

    boolean put(Put put, RowLock lock) throws IOException;

    boolean delete(Delete delete, RowLock lock) throws IOException;
}
