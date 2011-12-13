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
package org.lilyproject.util.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This is a threadsafe solution for the non-threadsafe HTable.
 *
 * <p>The problem with HTable this tries to solve is that HTable is not threadsafe, and
 * on the other hand it is best not to instantiate a new HTable for each use for
 * performance reasons.
 *
 * <p>HTable is, unlike e.g. a file handle or a JDBC connection, not a scarce
 * resource which needs to be closed. The actual connection handling (which
 * consists of connections to a variety of region servers) it handled at other
 * places. We only need to avoid the cost of creating new copies of HTable all the time.
 *
 * <p>The first implementation was based on caching HTable instances in threadlocal
 * variables, now it is based on HTablePool. The reason for changing this was that
 * HTable now contains an ExecutorService instance, which is better exploited if we
 * reduce the number of HTable instances.
 *
 * <p>Be careful/considerate when using autoflush.
 *
 */
public class LocalHTable implements HTableInterface {
    private Configuration conf;
    private byte[] tableName;
    private Log log = LogFactory.getLog(getClass());
    private static Map<Configuration, HTablePool> HTABLE_POOLS = new HashMap<Configuration, HTablePool>();
    private HTablePool pool;

    public LocalHTable(Configuration conf, byte[] tableName) throws IOException {
        this.conf = conf;
        this.tableName = tableName;
        this.pool = getHTablePool(conf);

        // Test the table is accessible
        runNoIE(new TableRunnable<Object>() {
            @Override
            public Object run(HTableInterface table) throws IOException {
                return null;
            }
        });
    }

    public LocalHTable(Configuration conf, String tableName) throws IOException {
        this(conf, Bytes.toBytes(tableName));
    }

    public static HTablePool getHTablePool(Configuration conf) {
        HTablePool pool;
        synchronized (HTABLE_POOLS) {
            pool = HTABLE_POOLS.get(conf);
            if (pool == null) {
                pool = new HTablePool(conf, 20);
                HTABLE_POOLS.put(conf, pool);
                Log log = LogFactory.getLog(LocalHTable.class);
                if (log.isDebugEnabled()) {
                    log.debug("Created a new HTablePool instance for configuration " + conf);
                }
            }
        }
        return pool;
    }

    @Override
    public byte[] getTableName() {
        return runNoExc(new TableRunnable<byte[]>() {
            @Override
            public byte[] run(HTableInterface table) throws IOException, InterruptedException {
                return table.getTableName();
            }
        });
    }

    @Override
    public Configuration getConfiguration() {
        return runNoExc(new TableRunnable<Configuration>() {
            @Override
            public Configuration run(HTableInterface table) throws IOException, InterruptedException {
                return table.getConfiguration();
            }
        });
    }

    @Override
    public HTableDescriptor getTableDescriptor() throws IOException {
        return runNoExc(new TableRunnable<HTableDescriptor>() {
            @Override
            public HTableDescriptor run(HTableInterface table) throws IOException, InterruptedException {
                return table.getTableDescriptor();
            }
        });
    }

    @Override
    public boolean exists(final Get get) throws IOException {
        return runNoIE(new TableRunnable<Boolean>() {
            @Override
            public Boolean run(HTableInterface table) throws IOException, InterruptedException {
                return table.exists(get);
            }
        });
    }

    @Override
    public Result get(final Get get) throws IOException {
        return runNoIE(new TableRunnable<Result>() {
            @Override
            public Result run(HTableInterface table) throws IOException, InterruptedException {
                return table.get(get);
            }
        });
    }

    @Override
    public Result getRowOrBefore(final byte[] row, final byte[] family) throws IOException {
        return runNoIE(new TableRunnable<Result>() {
            @Override
            public Result run(HTableInterface table) throws IOException, InterruptedException {
                return table.getRowOrBefore(row, family);
            }
        });
    }

    @Override
    public ResultScanner getScanner(final Scan scan) throws IOException {
        return runNoIE(new TableRunnable<ResultScanner>() {
            @Override
            public ResultScanner run(HTableInterface table) throws IOException, InterruptedException {
                return table.getScanner(scan);
            }
        });
    }

    @Override
    public ResultScanner getScanner(final byte[] family) throws IOException {
        return runNoIE(new TableRunnable<ResultScanner>() {
            @Override
            public ResultScanner run(HTableInterface table) throws IOException, InterruptedException {
                return table.getScanner(family);
            }
        });
    }

    @Override
    public ResultScanner getScanner(final byte[] family, final byte[] qualifier) throws IOException {
        return runNoIE(new TableRunnable<ResultScanner>() {
            @Override
            public ResultScanner run(HTableInterface table) throws IOException, InterruptedException {
                return table.getScanner(family, qualifier);
            }
        });
    }

    @Override
    public void put(final Put put) throws IOException {
        runNoIE(new TableRunnable<Void>() {
            @Override
            public Void run(HTableInterface table) throws IOException, InterruptedException {
                table.put(put);
                return null;
            }
        });
    }

    @Override
    public void put(final List<Put> puts) throws IOException {
        runNoIE(new TableRunnable<Void>() {
            @Override
            public Void run(HTableInterface table) throws IOException, InterruptedException {
                table.put(puts);
                return null;
            }
        });
    }

    @Override
    public boolean checkAndPut(final byte[] row, final byte[] family, final byte[] qualifier, final byte[] value,
            final Put put) throws IOException {
        return runNoIE(new TableRunnable<Boolean>() {
            @Override
            public Boolean run(HTableInterface table) throws IOException, InterruptedException {
                return table.checkAndPut(row, family, qualifier, value, put);
            }
        });
    }

    @Override
    public void delete(final Delete delete) throws IOException {
        runNoIE(new TableRunnable<Void>() {
            @Override
            public Void run(HTableInterface table) throws IOException, InterruptedException {
                table.delete(delete);
                return null;
            }
        });
    }

    @Override
    public void delete(final List<Delete> deletes) throws IOException {
        runNoIE(new TableRunnable<Void>() {
            @Override
            public Void run(HTableInterface table) throws IOException, InterruptedException {
                table.delete(deletes);
                return null;
            }
        });
    }

    @Override
    public boolean checkAndDelete(final byte[] row, final byte[] family, final byte[] qualifier, final byte[] value,
            final Delete delete) throws IOException {
        return runNoIE(new TableRunnable<Boolean>() {
            @Override
            public Boolean run(HTableInterface table) throws IOException, InterruptedException {
                return table.checkAndDelete(row, family, qualifier, value, delete);
            }
        });
    }

    @Override
    public long incrementColumnValue(final byte[] row, final byte[] family, final byte[] qualifier, final long amount)
            throws IOException {
        return runNoIE(new TableRunnable<Long>() {
            @Override
            public Long run(HTableInterface table) throws IOException, InterruptedException {
                return table.incrementColumnValue(row, family, qualifier, amount);
            }
        });
    }

    @Override
    public long incrementColumnValue(final byte[] row, final byte[] family, final byte[] qualifier, final long amount,
            final boolean writeToWAL) throws IOException {
        return runNoIE(new TableRunnable<Long>() {
            @Override
            public Long run(HTableInterface table) throws IOException, InterruptedException {
                return table.incrementColumnValue(row, family, qualifier, amount, writeToWAL);
            }
        });
    }

    @Override
    public boolean isAutoFlush() {
        return runNoExc(new TableRunnable<Boolean>() {
            @Override
            public Boolean run(HTableInterface table) throws IOException, InterruptedException {
                return table.isAutoFlush();
            }
        });
    }

    @Override
    public void flushCommits() throws IOException {
        runNoIE(new TableRunnable<Object>() {
            @Override
            public Object run(HTableInterface table) throws IOException, InterruptedException {
                table.flushCommits();
                return null;
            }
        });
    }

    @Override
    public void close() throws IOException {
        runNoIE(new TableRunnable<Void>() {
            @Override
            public Void run(HTableInterface table) throws IOException, InterruptedException {
                table.close();
                return null;
            }
        });
    }

    @Override
    public RowLock lockRow(final byte[] row) throws IOException {
        return runNoIE(new TableRunnable<RowLock>() {
            @Override
            public RowLock run(HTableInterface table) throws IOException, InterruptedException {
                return table.lockRow(row);
            }
        });
    }

    @Override
    public void unlockRow(final RowLock rl) throws IOException {
        runNoIE(new TableRunnable<Void>() {
            @Override
            public Void run(HTableInterface table) throws IOException, InterruptedException {
                table.unlockRow(rl);
                return null;
            }
        });
    }

    @Override
    public void batch(final List<Row> actions, final Object[] results) throws IOException, InterruptedException {
        run(new TableRunnable<Void>() {
            @Override
            public Void run(HTableInterface table) throws IOException, InterruptedException {
                table.batch(actions, results);
                return null;
            }
        });
    }

    @Override
    public Object[] batch(final List<Row> actions) throws IOException, InterruptedException {
        return run(new TableRunnable<Object[]>() {
            @Override
            public Object[] run(HTableInterface table) throws IOException, InterruptedException {
                return table.batch(actions);
            }
        });
    }

    @Override
    public Result[] get(final List<Get> gets) throws IOException {
        return runNoIE(new TableRunnable<Result[]>() {
            @Override
            public Result[] run(HTableInterface table) throws IOException, InterruptedException {
                return table.get(gets);
            }
        });
    }

    @Override
    public Result increment(final Increment increment) throws IOException {
        return runNoIE(new TableRunnable<Result>() {
            @Override
            public Result run(HTableInterface table) throws IOException {
                return table.increment(increment);
            }
        });
    }
    
    private <T> T run(TableRunnable<T> runnable) throws IOException, InterruptedException {
        HTableInterface table = pool.getTable(tableName);
        try {
            return runnable.run(table);
        } finally {
            pool.putTable(table);
        }
    }

    private <T> T runNoIE(TableRunnable<T> runnable) throws IOException {
        HTableInterface table = pool.getTable(tableName);
        try {
            return runnable.run(table);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            pool.putTable(table);
        }
    }

    private <T> T runNoExc(TableRunnable<T> runnable) {
        HTableInterface table = pool.getTable(tableName);
        try {
            return runnable.run(table);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            pool.putTable(table);
        }
    }

    private static interface TableRunnable<T> {
        public T run(HTableInterface table) throws IOException, InterruptedException;
    }
}
