package org.lilyproject.hadooptestfw;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.net.URI;
import java.util.*;

import static org.apache.zookeeper.ZooKeeper.States.CONNECTED;

public class CleanupUtil {
    private Configuration conf;

    private String zkConnectString;

    private static Set<String> RETAIN_TABLES = new HashSet<String>();
    static {
    }

    private static Map<String, byte[]> DEFAULT_TIMESTAMP_REUSING_TABLES = new HashMap<String, byte[]>();
    static {
        DEFAULT_TIMESTAMP_REUSING_TABLES.put("record", Bytes.toBytes("data"));
        DEFAULT_TIMESTAMP_REUSING_TABLES.put("type", Bytes.toBytes("fieldtype-entry"));
    }

    public CleanupUtil(Configuration conf, String zkConnectString) {
        this.conf = conf;
        this.zkConnectString = zkConnectString;
    }

    public Map<String, byte[]> getDefaultTimestampReusingTables() {
        return Collections.unmodifiableMap(DEFAULT_TIMESTAMP_REUSING_TABLES);
    }

    public void cleanZooKeeper() throws Exception {
        int sessionTimeout = 10000;

        ZooKeeper zk = new ZooKeeper(zkConnectString, sessionTimeout, new Watcher() {
            public void process(WatchedEvent event) {
                if (event.getState() == Watcher.Event.KeeperState.Disconnected) {
                    System.err.println("ZooKeeper Disconnected.");
                } else if (event.getState() == Event.KeeperState.Expired) {
                    System.err.println("ZooKeeper session expired.");
                }
            }
        });

        long waitUntil = System.currentTimeMillis() + sessionTimeout;
        while (zk.getState() != CONNECTED && waitUntil > System.currentTimeMillis()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                break;
            }
        }

        if (zk.getState() != CONNECTED) {
            throw new RuntimeException("Failed to connect to ZK within " + sessionTimeout + "ms.");
        }

        if (zk.exists("/lily", false) != null) {
            System.out.println("----------------- Clearing '/lily' node in ZooKeeper -------------------");
            deleteChildren("/lily", zk);
            zk.delete("/lily", -1);
            System.out.println("------------------------------------------------------------------------");
        }

        zk.close();
    }

    private void deleteChildren(String path, ZooKeeper zk) throws InterruptedException, KeeperException {
        List<String> children = zk.getChildren(path, false);
        for (String child : children) {
            String childPath = path + "/" + child;
            deleteChildren(childPath, zk);
            System.out.println("Deleting " + path);
            zk.delete(childPath, -1);
        }
    }

    public void cleanTables() throws Exception {
        Map<String, byte[]> timestampReusingTables = new HashMap<String, byte[]>();
        timestampReusingTables.putAll(DEFAULT_TIMESTAMP_REUSING_TABLES);
        cleanTables(timestampReusingTables);
    }

    public void cleanTables(Map<String, byte[]> timestampReusingTables) throws Exception {
        System.out.println("------------------------ Resetting HBase tables ------------------------");

        StringBuilder truncateReport = new StringBuilder();
        StringBuilder retainReport = new StringBuilder();

        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor[] tables = admin.listTables();
        System.out.println("Found tables: " + tables.length);

        Set<String> exploitTimestampTables = new HashSet<String>();

        for (HTableDescriptor table : tables) {
            if (RETAIN_TABLES.contains(table.getNameAsString())) {
                if (retainReport.length() > 0)
                    retainReport.append(", ");
                retainReport.append(table.getNameAsString());
                continue;
            }

            HTable htable = new HTable(conf, table.getName());

            if (timestampReusingTables.containsKey(table.getNameAsString())) {
                insertTimestampTableTestRecord(table.getNameAsString(), htable,
                        timestampReusingTables.get(table.getNameAsString()));
                exploitTimestampTables.add(table.getNameAsString());
            }

            Scan scan = new Scan();
            ResultScanner scanner = htable.getScanner(scan);
            Result[] results;
            int totalCount = 0;

            while ((results = scanner.next(1000)).length > 0) {
                List<Delete> deletes = new ArrayList<Delete>(results.length);
                for (Result result : results) {
                    deletes.add(new Delete(result.getRow()));
                }
                totalCount += deletes.size();
                htable.delete(deletes);
            }

            if (truncateReport.length() > 0)
                truncateReport.append(", ");
            truncateReport.append(table.getNameAsString()).append(" (").append(totalCount).append(")");

            scanner.close();
            htable.close();

            if (timestampReusingTables.containsKey(table.getNameAsString())) {
                flush(admin, table.getNameAsString());
                admin.majorCompact(table.getName());
            }
        }

        truncateReport.insert(0, "Truncated the following tables: ");
        retainReport.insert(0, "Did not truncate the following tables: ");

        System.out.println(truncateReport);
        System.out.println(retainReport);

        waitForTimestampTables(exploitTimestampTables, timestampReusingTables);

        System.out.println("------------------------------------------------------------------------");

    }

    private void insertTimestampTableTestRecord(String tableName, HTable htable, byte[] family) throws IOException {
        byte[] tmpRowKey = Bytes.toBytes("HBaseProxyDummyRow");
        byte[] COL = Bytes.toBytes("DummyColumn");
        Put put = new Put(tmpRowKey);
        // put a value with a fixed timestamp
        put.add(family, COL, 1, new byte[] { 0 });

        htable.put(put);
    }

    private void waitForTimestampTables(Set<String> tables, Map<String, byte[]> timestampReusingTables)
            throws IOException, InterruptedException {
        for (String tableName : tables) {

            HTable htable = new HTable(conf, tableName);

            byte[] CF = timestampReusingTables.get(tableName);
            byte[] tmpRowKey = waitForCompact(tableName, CF);

            // Delete our dummy row again
            htable.delete(new Delete(tmpRowKey));
        }
    }

    private byte[] waitForCompact(String tableName, byte[] CF) throws IOException, InterruptedException {
        byte[] tmpRowKey = Bytes.toBytes("HBaseProxyDummyRow");
        byte[] COL = Bytes.toBytes("DummyColumn");
        HTable htable = new HTable(conf, tableName);

        byte[] value = null;
        while (value == null) {
            Put put = new Put(tmpRowKey);
            put.add(CF, COL, 1, new byte[] { 0 });
            htable.put(put);

            Get get = new Get(tmpRowKey);
            Result result = htable.get(get);
            value = result.getValue(CF, COL);
            if (value == null) {
                // If the value is null, it is because the delete marker has not yet been flushed/compacted away
                System.out.println("Waiting for flush/compact of " + tableName + " to complete");
                Thread.sleep(100);
            }
        }
        return tmpRowKey;
    }

    /** Force a major compaction and wait for it to finish.
     *  This method can be used in a test to avoid issue HBASE-2256 after performing a delete operation
     *  Uses same principle as {@link #cleanTables}
     */
    public void majorCompact(String tableName, String[] columnFamilies) throws Exception {
        byte[] tmpRowKey = Bytes.toBytes("HBaseProxyDummyRow");
        byte[] COL = Bytes.toBytes("DummyColumn");
        HBaseAdmin admin = new HBaseAdmin(conf);
        HTable htable = new HTable(conf, tableName);

        // Write a dummy row
        for (String columnFamily : columnFamilies) {
            byte[] CF = Bytes.toBytes(columnFamily);
            Put put = new Put(tmpRowKey);
            put.add(CF, COL, 1, new byte[] { 0 });
            htable.put(put);
            // Delete the value again
            Delete delete = new Delete(tmpRowKey);
            delete.deleteColumn(CF, COL);
            htable.delete(delete);
        }

        // Perform major compaction
        flush(admin, tableName);
        admin.majorCompact(tableName);

        // Wait for compact to finish
        for (String columnFamily : columnFamilies) {
            byte[] CF = Bytes.toBytes(columnFamily);
            waitForCompact(tableName, CF);
        }
    }
    
    public void cleanBlobStore(URI dfsUri) throws Exception {
        FileSystem fs = FileSystem.get(new URI(dfsUri.getScheme() + "://" + dfsUri.getAuthority()), conf);
        Path blobRootPath = new Path(dfsUri.getPath());
        fs.delete(blobRootPath, true);
    }

    /**
     * This method was copied from HBase's HBaseAdmin source file in order to fix a connection leak:
     * the CatalogTracker was not closed, this would among other things leak a ZooKeeper client connection
     * on each flush (especially important when this is used in the LilyLauncher).
     *
     * <p></p>This bug was present in chd3u, but already fixed in HBase trunk, so this method can be
     * removed after upgrade.
     */
    private void flush(HBaseAdmin hbaseAdmin, String tableName) throws IOException, InterruptedException {
        CatalogTracker ct = getCatalogTracker();
        try {
            List<Pair<HRegionInfo, HServerAddress>> pairs =
                    MetaReader.getTableRegionsAndLocations(ct, tableName);
            for (Pair<HRegionInfo, HServerAddress> pair: pairs) {
                if (pair.getSecond() == null) continue;
                flush(hbaseAdmin, pair.getSecond(), pair.getFirst());
            }
        } finally {
            cleanupCatalogTracker(ct);
        }
    }

    /** Copied from HBase source to support flush fix. */
    private void flush(HBaseAdmin hbaseAdmin, final HServerAddress hsa, final HRegionInfo hri)
            throws IOException {
        HRegionInterface rs = hbaseAdmin.getConnection().getHRegionConnection(hsa);
        rs.flushRegion(hri);
    }

    /** Copied from HBase source to support flush fix. */
    private synchronized CatalogTracker getCatalogTracker()
            throws ZooKeeperConnectionException, IOException {
        CatalogTracker ct = null;
        try {
            HConnection connection =
                    HConnectionManager.getConnection(new Configuration(this.conf));
            ct = new CatalogTracker(connection);
            ct.start();
        } catch (InterruptedException e) {
            // Let it out as an IOE for now until we redo all so tolerate IEs
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted", e);
        }
        return ct;
    }

    /** Copied from HBase source to support flush fix. */
    private void cleanupCatalogTracker(final CatalogTracker ct) {
        ct.stop();
        HConnectionManager.deleteConnection(ct.getConnection().getConfiguration(), true);
    }
}
