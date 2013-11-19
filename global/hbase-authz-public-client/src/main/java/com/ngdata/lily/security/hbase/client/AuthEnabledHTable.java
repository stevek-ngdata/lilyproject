package com.ngdata.lily.security.hbase.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.util.Bytes;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An implementation of, and wrapper around, HTableInterface that automatically adds
 * the authentication context onto requests. This is just helper code to assure correct
 * communication of the user context on all requests.
 *
 * <p>Some methods don't support passing attributes, and for those we can't add authentication
 * info. These methods throw an exception. It is of course possible to bypass the authentication
 * by using the wrapped HTable instance, but then no authorization filtering will be applied!</p>
 *
 * <p>This method will modify the operation objects (the Get, Put, etc.) by adding an attribute
 * to them (cfr. HBase's OperationWithAttributes). This means it is not a good idea to have, say,
 * a Get object shared between threads.</p>
 */
public class AuthEnabledHTable implements HTableInterface {
    private HTableInterface delegate;
    private AuthorizationContextProvider authzCtxProvider;
    private byte[] extraPermissions;
    private byte[] appName;
    private boolean failWhenNotAuthenticated;

    private final String ERROR_MSG = "Method not supported with authentication";

    /**
     *
     * @param failWhenNotAuthenticated throw an exception if there is no authorization context (= authenticated
     *                                 user) available, i.o.w. refuse to do requests that will not be subject to
     *                                 authorization filtering.
     * @param appName application name, identifies the permissions that should be active for this application.
     *                Permissions start with "appName:...".
     * @param extraPermissions extra permissions which will be passed upon each request and which extend the
     *                         permissions of the user. See {@link HBaseAuthzUtil#EXTRA_PERMISSION_ATT}. This overwrites
     *                         any per-request permissions which might already have been set.
     */
    public AuthEnabledHTable(AuthorizationContextProvider authzCtxProvider, boolean failWhenNotAuthenticated,
            String appName, @Nullable Set<String> extraPermissions, HTableInterface delegate) {
        this.authzCtxProvider = authzCtxProvider;
        this.appName = Bytes.toBytes(appName);
        this.extraPermissions = extraPermissions != null ? HBaseAuthzUtil.serialize(extraPermissions) : null;
        this.failWhenNotAuthenticated = failWhenNotAuthenticated;
        this.delegate = delegate;
    }

    private void addAuthInfo(OperationWithAttributes op) {
        addAuthInfo(Arrays.<OperationWithAttributes>asList(op));
    }

    private void addAuthInfo(Iterable<? extends OperationWithAttributes> ops) {
        // For multi-ops: since the list of operations might be split to sent to different servers, we
        // need to add the authentication context to all of them.

        AuthorizationContext authzCtx = authzCtxProvider.getAuthorizationContext();
        if (authzCtx == null && failWhenNotAuthenticated) {
            throw new RuntimeException("No authenticated user available.");
        } else if (authzCtx == null) {
            return;
        }

        byte[] userAsBytes = authzCtx.serialize();

        for (OperationWithAttributes op : ops) {
            op.setAttribute(AuthorizationContext.OPERATION_ATTRIBUTE, userAsBytes);

            op.setAttribute(HBaseAuthzUtil.APP_NAME_ATT, appName);

            if (extraPermissions != null) {
                op.setAttribute(HBaseAuthzUtil.EXTRA_PERMISSION_ATT, extraPermissions);
            }
        }
    }

    @Override
    public byte[] getTableName() {
        return delegate.getTableName();
    }

    @Override
    public Configuration getConfiguration() {
        return delegate.getConfiguration();
    }

    @Override
    public HTableDescriptor getTableDescriptor() throws IOException {
        return delegate.getTableDescriptor();
    }

    @Override
    public boolean exists(Get get) throws IOException {
        addAuthInfo(get);
        return delegate.exists(get);
    }

    @Override
    public void batch(List<? extends Row> actions, Object[] results) throws IOException, InterruptedException {
        throw new RuntimeException(ERROR_MSG);
    }

    @Override
    public Object[] batch(List<? extends Row> actions) throws IOException, InterruptedException {
        throw new RuntimeException(ERROR_MSG);
    }

    @Override
    public Result get(Get get) throws IOException {
        addAuthInfo(get);
        return delegate.get(get);
    }

    @Override
    public Result[] get(List<Get> gets) throws IOException {
        addAuthInfo(gets);
        return delegate.get(gets);
    }

    @Override
    public Result getRowOrBefore(byte[] row, byte[] family) throws IOException {
        throw new RuntimeException(ERROR_MSG);
    }

    @Override
    public ResultScanner getScanner(Scan scan) throws IOException {
        addAuthInfo(scan);
        return delegate.getScanner(scan);
    }

    @Override
    public ResultScanner getScanner(byte[] family) throws IOException {
        throw new RuntimeException(ERROR_MSG);
    }

    @Override
    public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
        throw new RuntimeException(ERROR_MSG);
    }

    @Override
    public void put(Put put) throws IOException {
        addAuthInfo(put);
        delegate.put(put);
    }

    @Override
    public void put(List<Put> puts) throws IOException {
        addAuthInfo(puts);
        delegate.put(puts);
    }

    @Override
    public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put) throws IOException {
        addAuthInfo(put);
        return delegate.checkAndPut(row, family, qualifier, value, put);
    }

    @Override
    public void delete(Delete delete) throws IOException {
        addAuthInfo(delete);
        delegate.delete(delete);
    }

    @Override
    public void delete(List<Delete> deletes) throws IOException {
        addAuthInfo(deletes);
        delegate.delete(deletes);
    }

    @Override
    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value, Delete delete)
            throws IOException {
        addAuthInfo(delete);
        return delegate.checkAndDelete(row, family, qualifier, value, delete);
    }

    @Override
    public void mutateRow(RowMutations rm) throws IOException {
        throw new RuntimeException(ERROR_MSG);
    }

    @Override
    public Result append(Append append) throws IOException {
        addAuthInfo(append);
        return delegate.append(append);
    }

    @Override
    public Result increment(Increment increment) throws IOException {
        throw new RuntimeException(ERROR_MSG);
    }

    @Override
    public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount) throws IOException {
        throw new RuntimeException(ERROR_MSG);
    }

    @Override
    public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount, boolean writeToWAL)
            throws IOException {
        throw new RuntimeException(ERROR_MSG);
    }

    @Override
    public boolean isAutoFlush() {
        return delegate.isAutoFlush();
    }

    @Override
    public void flushCommits() throws IOException {
        delegate.flushCommits();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public RowLock lockRow(byte[] row) throws IOException {
        throw new RuntimeException(ERROR_MSG);
    }

    @Override
    public void unlockRow(RowLock rl) throws IOException {
        throw new RuntimeException(ERROR_MSG);
    }

    @Override
    public <T extends CoprocessorProtocol> T coprocessorProxy(Class<T> protocol, byte[] row) {
        return delegate.coprocessorProxy(protocol, row);
    }

    @Override
    public <T extends CoprocessorProtocol, R> Map<byte[], R> coprocessorExec(Class<T> protocol, byte[] startKey,
            byte[] endKey, Batch.Call<T, R> callable) throws IOException, Throwable {
        return delegate.coprocessorExec(protocol, startKey, endKey, callable);
    }

    @Override
    public <T extends CoprocessorProtocol, R> void coprocessorExec(Class<T> protocol, byte[] startKey, byte[] endKey,
            Batch.Call<T, R> callable, Batch.Callback<R> callback) throws IOException, Throwable {
        delegate.coprocessorExec(protocol, startKey, endKey, callable, callback);
    }

    @Override
    public void setAutoFlush(boolean autoFlush) {
        delegate.setAutoFlush(autoFlush);
    }

    @Override
    public void setAutoFlush(boolean autoFlush, boolean clearBufferOnFail) {
        delegate.setAutoFlush(autoFlush, clearBufferOnFail);
    }

    @Override
    public long getWriteBufferSize() {
        return delegate.getWriteBufferSize();
    }

    @Override
    public void setWriteBufferSize(long writeBufferSize) throws IOException {
        delegate.setWriteBufferSize(writeBufferSize);
    }

}
