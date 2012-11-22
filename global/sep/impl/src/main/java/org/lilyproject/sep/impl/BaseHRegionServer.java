package org.lilyproject.sep.impl;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.MultiAction;
import org.apache.hadoop.hbase.client.MultiResponse;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Exec;
import org.apache.hadoop.hbase.client.coprocessor.ExecResult;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.io.hfile.BlockCacheColumnFamilySummary;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.ipc.ProtocolSignature;
import org.apache.hadoop.hbase.regionserver.RegionOpeningState;
import org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.net.ConnectException;
import java.util.List;

/**
 * Empty implementation of HRegionInterface, except for {@link #getProtocolVersion} and
 * {@link #getProtocolVersion}.
 */
public class BaseHRegionServer implements HRegionInterface {
    public long getProtocolVersion(final String protocol, final long clientVersion) throws IOException {
        if (protocol.equals(HRegionInterface.class.getName())) {
          return HRegionInterface.VERSION;
        }
        throw new IOException("Unknown protocol: " + protocol);
    }

    public ProtocolSignature getProtocolSignature(String protocol, long version, int clientMethodsHashCode)
            throws IOException {
        if (protocol.equals(HRegionInterface.class.getName())) {
          return new ProtocolSignature(HRegionInterface.VERSION, null);
        }
        throw new IOException("Unknown protocol: " + protocol);
    }

    @Override
    public HRegionInfo getRegionInfo(byte[] regionName) throws NotServingRegionException, ConnectException,
            IOException {
        return null;
    }

    @Override
    public Result getClosestRowBefore(byte[] regionName, byte[] row, byte[] family) throws IOException {
        return null;
    }

    @Override
    public Result get(byte[] regionName, Get get) throws IOException {
        return null;
    }

    @Override
    public boolean exists(byte[] regionName, Get get) throws IOException {
        return false;
    }

    @Override
    public void put(byte[] regionName, Put put) throws IOException {
    }

    @Override
    public int put(byte[] regionName, List<Put> puts) throws IOException {
        return 0;
    }

    @Override
    public void delete(byte[] regionName, Delete delete) throws IOException {
    }

    @Override
    public int delete(byte[] regionName, List<Delete> deletes) throws IOException {
        return 0;
    }

    @Override
    public boolean checkAndPut(byte[] regionName, byte[] row, byte[] family, byte[] qualifier, byte[] value,
            Put put) throws IOException {
        return false;
    }

    @Override
    public boolean checkAndDelete(byte[] regionName, byte[] row, byte[] family, byte[] qualifier, byte[] value,
            Delete delete) throws IOException {
        return false;
    }

    @Override
    public long incrementColumnValue(byte[] regionName, byte[] row, byte[] family, byte[] qualifier, long amount,
            boolean writeToWAL) throws IOException {
        return 0;
    }

    @Override
    public Result increment(byte[] regionName, Increment increment) throws IOException {
        return null;
    }

    @Override
    public long openScanner(byte[] regionName, Scan scan) throws IOException {
        return 0;
    }

    @Override
    public Result next(long scannerId) throws IOException {
        return null;
    }

    @Override
    public Result[] next(long scannerId, int numberOfRows) throws IOException {
        return new Result[0];
    }

    @Override
    public void close(long scannerId) throws IOException {
    }

    @Override
    public long lockRow(byte[] regionName, byte[] row) throws IOException {
        return 0;
    }

    @Override
    public void unlockRow(byte[] regionName, long lockId) throws IOException {
    }

    @Override
    public List<HRegionInfo> getOnlineRegions() throws IOException {
        return null;
    }

    @Override
    public HServerInfo getHServerInfo() throws IOException {
        return null;
    }

    @Override
    public <R> MultiResponse multi(MultiAction<R> multi) throws IOException {
        return null;
    }

    @Override
    public boolean bulkLoadHFiles(List<Pair<byte[], String>> familyPaths, byte[] regionName) throws IOException {
        return false;
    }

    @Override
    public RegionOpeningState openRegion(HRegionInfo region) throws IOException {
        return null;
    }

    @Override
    public RegionOpeningState openRegion(HRegionInfo region, int versionOfOfflineNode) throws IOException {
        return null;
    }

    @Override
    public void openRegions(List<HRegionInfo> regions) throws IOException {
    }

    @Override
    public boolean closeRegion(HRegionInfo region) throws IOException {
        return false;
    }

    @Override
    public boolean closeRegion(HRegionInfo region, int versionOfClosingNode) throws IOException {
        return false;
    }

    @Override
    public boolean closeRegion(HRegionInfo region, boolean zk) throws IOException {
        return false;
    }

    @Override
    public boolean closeRegion(byte[] encodedRegionName, boolean zk) throws IOException {
        return false;
    }

    @Override
    public void flushRegion(HRegionInfo regionInfo) throws NotServingRegionException, IOException {
    }

    @Override
    public void splitRegion(HRegionInfo regionInfo) throws NotServingRegionException, IOException {
    }

    @Override
    public void splitRegion(HRegionInfo regionInfo, byte[] splitPoint) throws NotServingRegionException, IOException {
    }

    @Override
    public void compactRegion(HRegionInfo regionInfo, boolean major) throws NotServingRegionException, IOException {
    }

    @Override
    public void replicateLogEntries(HLog.Entry[] entries) throws IOException {
    }

    @Override
    public ExecResult execCoprocessor(byte[] regionName, Exec call) throws IOException {
        return null;
    }

    @Override
    public boolean checkAndPut(byte[] regionName, byte[] row, byte[] family, byte[] qualifier,
            CompareFilter.CompareOp compareOp, WritableByteArrayComparable comparator, Put put) throws IOException {
        return false;
    }

    @Override
    public boolean checkAndDelete(byte[] regionName, byte[] row, byte[] family, byte[] qualifier,
            CompareFilter.CompareOp compareOp, WritableByteArrayComparable comparator, Delete delete)
            throws IOException {
        return false;
    }

    @Override
    public List<BlockCacheColumnFamilySummary> getBlockCacheColumnFamilySummaries() throws IOException {
        return null;
    }

    @Override
    public byte[][] rollHLogWriter() throws IOException, FailedLogCloseException {
        return new byte[0][];
    }

    @Override
    public void stop(String why) {
    }

    @Override
    public void abort(String why, Throwable e) {
    }

    @Override
    public boolean isAborted() {
        return false;
    }

    @Override
    public boolean isStopped() {
        return false;
    }
}
