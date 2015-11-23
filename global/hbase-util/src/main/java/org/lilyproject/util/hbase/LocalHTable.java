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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilyproject.util.concurrent.CustomThreadFactory;
import org.lilyproject.util.concurrent.WaitPolicy;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.*;

//import org.apache.hadoop.hbase.client.RowLock;
//import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

/**
 * This is a threadsafe solution for the non-threadsafe HTable.
 *
 * <p>Be careful/considerate when using autoflush.
 *
 * <p>The problems with HTable this tries to solve is that HTable:
 *
 * <ul>
 *     <li>HTable is not threadsafe</li>
 *     <li>It needs to be closed when you're done with it (cfr. HConnection refcounting). And closing as soon
 *     as you're done might actually not be efficient because when the refcount reaches zero, the connection
 *     is closed but maybe a second later you'll need it again.</li>
 *     <li>it is best not to instantiate a new HTable for each use for performance reasons
 *     (though according to HBASE-4805, if you pass an executorservice & hconnection yourself, this isn't true).</li>
 * </ul>
 *
 * <p>Note that HTable doesn't represent a connection, but uses a HConnection managed by the
 * HConnectionManager.</p>
 *
 * <p>The first implementation was based on caching HTable instances in threadlocal
 * variables, now it is based on HTablePool. The reason for changing this was that
 * HTable now contains an ExecutorService instance, which is better exploited if we
 * reduce the number of HTable instances. And now this implementation even changes
 * it by a shared ExecutorService instance, since otherwise still very
 * many very short-lived threads were created. So in fact we could return to the
 * thread-local approach, but maybe that's less efficient when threads are not reused
 * (which would actually be an efficiency problem by itself, so this is maybe not
 * an argument).
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
        // operations that do make use of it, they use the ExecutorServices of many different HTable instances,
        // leading to very little thread re-use and many very short-lived threads. Therefore, we switch the
        // ExecutorService instance in HBase by a shared one, which requires modifying a private variable.
        // (seems like this is improved in HBase trunk)

        synchronized (this) {
            if (EXECUTOR_SERVICE == null) {
                int maxThreads = Integer.MAX_VALUE;
                log.debug("Creating ExecutorService for HTable with max threads = " + maxThreads);

                EXECUTOR_SERVICE = new ThreadPoolExecutor(1, maxThreads,
                        60, TimeUnit.SECONDS,
                        new SynchronousQueue<Runnable>(),
                        new CustomThreadFactory("hbase-batch", null, true),
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

    public static void closePool(Configuration configuration) throws IOException{
        synchronized (HTABLE_POOLS) {
            HTABLE_POOLS.remove(configuration).close();
        }
    }

    public static void closeAllPools() throws IOException{
        synchronized (HTABLE_POOLS) {
            Iterator<Map.Entry<Configuration, HTablePool>> it = HTABLE_POOLS.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<Configuration, HTablePool> entry = it.next();
                // closing the table pool will close the connection for every table.
                entry.getValue().close();
            }
        }
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
        throw new UnsupportedOperationException("isAutoFlush is not supported on LocalHTables");
    }

    @Override
    public void flushCommits() throws IOException {
        throw new UnsupportedOperationException("flushCommits is not supported on LocalHTables");
    }

    @Override
    public void close() throws IOException {
        // Not allowed, since pooled.
    }

    /*@Override
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
    }*/

    @Override
    public void batch(final List<? extends Row> actions, final Object[] results) throws IOException, InterruptedException {
        run(new TableRunnable<Void>() {
            @Override
            public Void run(HTableInterface table) throws IOException, InterruptedException {
                table.batch(actions, results);
                return null;
            }
        });
    }

    @Override
    public Object[] batch(final List<? extends Row> actions) throws IOException, InterruptedException {
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

    /*@Override
    public <T extends CoprocessorProtocol> T coprocessorProxy(final Class<T> protocol, final byte[] row) {
        return runNoExc(new TableRunnable<T>() {
            @Override
            public T run(HTableInterface table) throws IOException, InterruptedException {
                return table.coprocessorProxy(protocol, row);
            }
        });
    }

    @Override
    public <T extends CoprocessorProtocol, R> Map<byte[], R> coprocessorExec(Class<T> protocol,
            byte[] startKey, byte[] endKey, Batch.Call<T,R> callable) throws IOException, Throwable {
        HTableInterface table = pool.getTable(tableNameString);
        try {
            return table.coprocessorExec(protocol, startKey, endKey, callable);
        } finally {
            table.close();
        }
    }

    @Override
    public <T extends CoprocessorProtocol, R> void coprocessorExec(Class<T> protocol, byte[] startKey, byte[] endKey,
            Batch.Call<T,R> callable, Batch.Callback<R> callback) throws IOException, Throwable {
        HTableInterface table = pool.getTable(tableNameString);
        try {
            table.coprocessorExec(protocol, startKey, endKey, callable, callback);
        } finally {
            table.close();
        }
    }*/

    @Override
    public void mutateRow(final RowMutations rm) throws IOException {
        runNoIE(new TableRunnable<Object>() {
            @Override
            public Object run(HTableInterface table) throws IOException, InterruptedException {
                table.mutateRow(rm);
                return null;
            }
        });
    }

    @Override
    public Result append(final Append append) throws IOException {
        return runNoIE(new TableRunnable<Result>() {
            @Override
            public Result run(HTableInterface table) throws IOException, InterruptedException {
                return table.append(append);
            }
        });
    }

    @Override
    public void setAutoFlush(boolean autoFlush) {
        throw new UnsupportedOperationException("setAutoFlush is not supported on LocalHTables");
    }

    @Override
    public void setAutoFlush(boolean autoFlush, boolean clearBufferOnFail) {
        throw new UnsupportedOperationException("setAutoFlush is not supported on LocalHTables");
    }

    @Override
    public long getWriteBufferSize() {
        throw new UnsupportedOperationException("getWriteBufferSize is not supported on LocalHTables");
    }

    @Override
    public void setWriteBufferSize(long writeBufferSize) throws IOException {
        throw new UnsupportedOperationException("setWriteBufferSize is not supported on LocalHTables");
    }

    //added for HBase .98

    @Override
    public CoprocessorRpcChannel coprocessorService(byte[] row) {
        HTableInterface table = pool.getTable(tableNameString);
        try {
            return table.coprocessorService(row);
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    @Override
    public <T extends com.google.protobuf.Service,R> Map<byte[],R> coprocessorService(Class<T> service, byte[] startKey, byte[] endKey, Batch.Call<T,R> callable)
            throws Throwable {
        HTableInterface table = pool.getTable(tableNameString);
        try {
            return table.coprocessorService(service, startKey, endKey, callable);
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public <T extends com.google.protobuf.Service,R> void coprocessorService(Class<T> service, byte[] startKey, byte[] endKey, Batch.Call<T,R> callable,
                                                                             Batch.Callback<R> callback)
            throws Throwable {
        HTableInterface table = pool.getTable(tableNameString);
        try {
            table.coprocessorService(service, startKey, endKey, callable);
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public <R extends com.google.protobuf.Message> Map<byte[],R> batchCoprocessorService(com.google.protobuf.Descriptors.MethodDescriptor methodDescriptor,
                                                                                         com.google.protobuf.Message request, byte[] startKey, byte[] endKey, R responsePrototype)
            throws com.google.protobuf.ServiceException,
            Throwable {
        HTableInterface table = pool.getTable(tableNameString);
        try {
            return table.batchCoprocessorService(methodDescriptor, request, startKey, endKey, responsePrototype);
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public <R extends com.google.protobuf.Message> void batchCoprocessorService(com.google.protobuf.Descriptors.MethodDescriptor methodDescriptor,
                                                                                com.google.protobuf.Message request,
                                                                                byte[] startKey, byte[] endKey, R responsePrototype, Batch.Callback<R> callback)
            throws Throwable {
        HTableInterface table = pool.getTable(tableNameString);
        try {
            table.batchCoprocessorService(methodDescriptor, request, startKey, endKey, responsePrototype);
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public <R> Object[] batchCallback(List<? extends Row> actions,
                                      Batch.Callback<R> callback)
            throws IOException,
            InterruptedException {
        HTableInterface table = pool.getTable(tableNameString);
        try {
            return table.batchCallback(actions, callback);
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public <R> void batchCallback(List<? extends Row> actions,
                                  Object[] results,
                                  Batch.Callback<R> callback)
            throws IOException,
            InterruptedException {
        HTableInterface table = pool.getTable(tableNameString);
        try {
            table.batchCallback(actions, callback);
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public TableName getName() {
        HTableInterface table = pool.getTable(tableNameString);
        return table.getName();
    }

    @Override
    public void setAutoFlushTo(boolean autoFlush) {
        throw new UnsupportedOperationException("setAutoFlushTo is not supported on LocalHTables");
    }

    @Override
    public Boolean[] exists(final List<Get> get) throws IOException {
        return runNoIE(new TableRunnable<Boolean[]>() {
            @Override
            public Boolean[] run(HTableInterface table) throws IOException, InterruptedException {
                return table.exists(get);
            }
        });
    }

    public long incrementColumnValue(final byte[] row, final byte[] family, final byte[] qualifier, final long amount, final Durability durability)
            throws IOException {
        return runNoIE(new TableRunnable<Long>() {
            @Override
            public Long run(HTableInterface table) throws IOException {
                return table.incrementColumnValue(row, family, qualifier, amount, durability);
            }
        });
    }

    private <T> T run(TableRunnable<T> runnable) throws IOException, InterruptedException {
        // passing tableNameString, since otherwise pool.getTable converts it to string anyway
        HTableInterface table = pool.getTable(tableNameString);
        try {
            return runnable.run(table);
        } finally {
            table.close();
        }
    }

    private <T> T runNoIE(TableRunnable<T> runnable) throws IOException {
        HTableInterface table = pool.getTable(tableNameString);
        try {
            return runnable.run(table);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            table.close();
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
            try {
                table.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
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
