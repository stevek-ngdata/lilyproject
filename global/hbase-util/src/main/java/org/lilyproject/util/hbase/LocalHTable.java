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
import org.lilyproject.util.Pair;
import org.lilyproject.util.concurrent.NamedThreadFactory;
import org.lilyproject.util.concurrent.WaitPolicy;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.*;

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
 * reduce the number of HTable instances (and now this implementation even changes
 * it by a shared ExecutorService instance, since otherwise threads still very
 * many very short-lived threads were created).
 *
 * <p>Be careful/considerate when using autoflush.
 *
 */
public class LocalHTable implements HTableInterface {
    private Configuration conf;
    private String tableNameString;
    private byte[] tableName;
    private Log log = LogFactory.getLog(getClass());
    private static Map<Configuration, HTablePool> HTABLE_POOLS = new HashMap<Configuration, HTablePool>();
    private static ExecutorService EXECUTOR_SERVICE;
    private static ExecutorService EXECUTOR_SERVICE_SHUTDOWN_PROTECTED;
    private static Field POOL_FIELD;
    private HTablePool pool;

    public LocalHTable(Configuration conf, byte[] tableName) throws IOException {
        this.conf = conf;
        this.tableName = tableName;
        this.tableNameString = Bytes.toString(tableName);
        this.pool = getHTablePool(conf);

        // HTable internally has an ExecutorService. I have noticed that many of the HBase operations that Lily
        // performs don't make use of this ES, since they are not plain put or batch operations. Thus, for the
        // operations that do make use of it (still enough, e.g. all the puts on the rowlog shards), they use
        // the ExecutorServices of many different HTable instances, leading to very little thread re-use
        // and many very short-lived threads. Therefore, we switch the ExecutorService instance in HBase by
        // a shared one, which requires modifying a private variable. (seems like this is improved in HBase trunk)

        synchronized (this) {
            if (EXECUTOR_SERVICE == null) {
                int maxThreads = Integer.MAX_VALUE;
                log.debug("Creating ExecutorService for HTable with max threads = " + maxThreads);

                EXECUTOR_SERVICE = new ThreadPoolExecutor(1, maxThreads,
                        60, TimeUnit.SECONDS,
                        new SynchronousQueue<Runnable>(),
                        new NamedThreadFactory("hbase-batch", null, true),
                        new WaitPolicy());
                EXECUTOR_SERVICE_SHUTDOWN_PROTECTED = new ShutdownProtectedExecutor(EXECUTOR_SERVICE);

                try {
                    POOL_FIELD = HTable.class.getDeclaredField("pool");
                    POOL_FIELD.setAccessible(true);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }

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

    public HTablePool getHTablePool(Configuration conf) {
        HTablePool pool;
        synchronized (HTABLE_POOLS) {
            pool = HTABLE_POOLS.get(conf);
            if (pool == null) {
                pool = new HTablePool(conf, 20, new HTableFactory());
                HTABLE_POOLS.put(conf, pool);
                Log log = LogFactory.getLog(LocalHTable.class);
                if (log.isDebugEnabled()) {
                    log.debug("Created a new HTablePool instance for conf " + System.identityHashCode(conf));
                }
            }
        }
        return pool;
    }
    
    public class HTableFactory implements HTableInterfaceFactory {
        @Override
        public HTableInterface createHTableInterface(Configuration config, byte[] tableName) {
            try {
                HTable table = new HTable(config, tableName);
                // Override HTable's private ExecutorService with our shared one, so that
                // there is better reuse of threads.
                try {
                    POOL_FIELD.set(table, EXECUTOR_SERVICE_SHUTDOWN_PROTECTED);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return table;
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }

        @Override
        public void releaseHTableInterface(HTableInterface table) {
            try {
                table.close();
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }
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
        // Not allowed, since pooled.
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
        // passing tableNameString, since otherwise pool.getTable converts it to string anyway
        HTableInterface table = pool.getTable(tableNameString);
        try {
            return runnable.run(table);
        } finally {
            pool.putTable(table);
        }
    }

    private <T> T runNoIE(TableRunnable<T> runnable) throws IOException {
        HTableInterface table = pool.getTable(tableNameString);
        try {
            return runnable.run(table);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            pool.putTable(table);
        }
    }

    private <T> T runNoExc(TableRunnable<T> runnable) {
        HTableInterface table = pool.getTable(tableNameString);
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

    public static class ShutdownProtectedExecutor implements ExecutorService {
        private ExecutorService executorService;

        public ShutdownProtectedExecutor(ExecutorService executorService) {
            this.executorService = executorService;
        }

        @Override
        public void shutdown() {
            // do nothing
        }

        @Override
        public List<Runnable> shutdownNow() {
            throw new RuntimeException("Don't call this");
        }

        @Override
        public boolean isShutdown() {
            return executorService.isShutdown();
        }

        @Override
        public boolean isTerminated() {
            return executorService.isTerminated();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return executorService.awaitTermination(timeout, unit);
        }

        @Override
        public <T> Future<T> submit(Callable<T> task) {
            return executorService.submit(task);
        }

        @Override
        public <T> Future<T> submit(Runnable task, T result) {
            return executorService.submit(task, result);
        }

        @Override
        public Future<?> submit(Runnable task) {
            return executorService.submit(task);
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
            return executorService.invokeAll(tasks);
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
            return executorService.invokeAll(tasks, timeout, unit);
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
            return executorService.invokeAny(tasks);
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return executorService.invokeAny(tasks, timeout, unit);
        }

        @Override
        public void execute(Runnable command) {
            executorService.execute(command);
        }
    }
}
