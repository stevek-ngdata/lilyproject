/*
 * Copyright 2010 Outerthought bvba
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

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseRowLocker implements RowLocker {

    private final HTableInterface table;
    private final byte[] family;
    private final byte[] qualifier;
    private final long timeout;
    private final RowLockerMetrics metrics;

    public HBaseRowLocker(HTableInterface table, byte[] family, byte[] qualifier, long timeout) {
        this(table, family, qualifier, timeout, null);
    }

    public HBaseRowLocker(HTableInterface table, String family, String qualifier, long timeout, RowLockerMetrics metrics) {
        this(table, Bytes.toBytes(family), Bytes.toBytes(qualifier), timeout, metrics);
    }

    public HBaseRowLocker(HTableInterface table, byte[] family, byte[] qualifier, long timeout, RowLockerMetrics metrics) {
        this.table = table;
        this.family = family;
        this.qualifier = qualifier;
        this.timeout = timeout;
        this.metrics = metrics;
    }
    
    public RowLock lockRow(byte[] rowKey) throws IOException {
        long now = System.currentTimeMillis();
        Get get = new Get(rowKey);
        get.addColumn(family, qualifier);
        
        Result result = table.get(get);
        byte[] previousPermit = null;
        long previousTimestamp = -1L;
        if (result != null && !result.isEmpty()) {
            previousPermit = result.getValue(family, qualifier);
            if (previousPermit != null) {
                RowLock previousRowLock = new RowLock(rowKey, previousPermit);
                previousTimestamp = previousRowLock.getTimestamp();
            }
        }
        if ((previousTimestamp == -1) || (previousTimestamp + timeout  < now)) {
            Put put = new Put(rowKey);
            RowLock rowLock = RowLock.createRowLock(rowKey);
            put.add(family, qualifier, 1L, rowLock.getPermit());
            if (table.checkAndPut(rowKey, family, qualifier, previousPermit, put)) {
                return rowLock;
            }
        }

        if (metrics != null) {
            metrics.contentions.inc();
        }

        return null;
    }

    public RowLock lockRow(byte[] rowKey, long timeout) throws IOException, InterruptedException {
        long tryUntil = System.currentTimeMillis() + timeout;
        while (System.currentTimeMillis() < tryUntil) {
            RowLock rowLock = lockRow(rowKey);
            if (rowLock != null) {
                return rowLock;
            }
            Thread.sleep(200);
        }
        return null;
    }

    public boolean unlockRow(RowLock lock) throws IOException {
        byte[] rowKey = lock.getRowKey();
        Put put = new Put(rowKey);
        put.add(family, qualifier, 1L, Bytes.toBytes(-1L));
        return table.checkAndPut(rowKey, family, qualifier, lock.getPermit(), put); // If it fails, we already lost the lock
    }
    
    public boolean isLocked(byte[] rowKey) throws IOException {
        long now = System.currentTimeMillis();
        Get get = new Get(rowKey);
        get.addColumn(family, qualifier);
        Result result = table.get(get);

        if (result.isEmpty()) return false;
        
        byte[] previousPermit = result.getValue(family, qualifier);
        if (previousPermit == null) return false;
        
        RowLock previousRowLock = new RowLock(rowKey, previousPermit);
        long previousTimestamp = previousRowLock.getTimestamp();
        if (previousTimestamp + timeout < now) return false;
        
        return true;
    }
    
    public boolean put(Put put, RowLock lock) throws IOException {
        if (!Bytes.equals(put.getRow(), lock.getRowKey()))
                return false;
        return table.checkAndPut(lock.getRowKey(), family, qualifier, lock.getPermit(), put);
    }
    
    public boolean delete(Delete delete, RowLock lock) throws IOException {
        if (!Bytes.equals(delete.getRow(), lock.getRowKey()))
                return false;
        return table.checkAndDelete(lock.getRowKey(), family, qualifier, lock.getPermit(), delete);
    }
}
