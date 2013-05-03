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
package org.lilyproject.hadooptestfw;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest.CompactionState;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.lilyproject.util.hbase.LilyHBaseSchema;
import org.lilyproject.util.io.Closer;

import static org.apache.zookeeper.ZooKeeper.States.CONNECTED;

public class CleanupUtil {
    private Configuration conf;

    private String zkConnectString;

    private static Set<String> RETAIN_TABLES = new HashSet<String>();
    static {
    }

    private static final Map<String, byte[]> DEFAULT_TIMESTAMP_REUSING_TABLES = new HashMap<String, byte[]>();
    static {
        DEFAULT_TIMESTAMP_REUSING_TABLES.put("record", Bytes.toBytes("data"));
        DEFAULT_TIMESTAMP_REUSING_TABLES.put("type", Bytes.toBytes("fieldtype-entry"));
    }

    public CleanupUtil(Configuration conf, String zkConnectString) {
        this.conf = conf;
        this.zkConnectString = zkConnectString;
    }

    public Map<String, byte[]> getDefaultTimestampReusingTables() {
        Map<String, byte[]> defaultTables = Maps.newHashMap(DEFAULT_TIMESTAMP_REUSING_TABLES);
        try {
            HBaseAdmin hbaseAdmin = new HBaseAdmin(conf);
            HTableDescriptor[] descriptors = hbaseAdmin.listTables();
            hbaseAdmin.close();
            if (descriptors != null) {
                for (HTableDescriptor descriptor : descriptors) {
                    if (LilyHBaseSchema.isRecordTableDescriptor(descriptor)) {
                        defaultTables.put(descriptor.getNameAsString(), Bytes.toBytes("data"));
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Error listing repository tables", e);
        }
        return Collections.unmodifiableMap(defaultTables);
    }

    public void cleanZooKeeper() throws Exception {
        int sessionTimeout = 10000;

        ZooKeeper zk = new ZooKeeper(zkConnectString, sessionTimeout, new Watcher() {
            @Override
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
                Thread.sleep(20);
            } catch (InterruptedException e) {
                break;
            }
        }

        if (zk.getState() != CONNECTED) {
            throw new RuntimeException("Failed to connect to ZK within " + sessionTimeout + "ms.");
        }

        if (zk.exists("/lily", false) != null) {
            System.out.println("----------------- Clearing '/lily' node in ZooKeeper -------------------");

            List<String> paths = new ArrayList<String>();
            collectChildren("/lily", zk, paths);
            paths.add("/lily");

            for (String path : paths) {
                zk.delete(path, -1, null, null);
            }

            long startWait = System.currentTimeMillis();
            while (zk.exists("/lily", null) != null) {
                Thread.sleep(5);

                if (System.currentTimeMillis() - startWait > 120000) {
                    throw new RuntimeException("State was not cleared in ZK within the expected timeout");
                }
            }

            System.out.println("Deleted " + paths.size() + " paths from ZooKeeper");
            System.out.println("------------------------------------------------------------------------");
        }

        zk.close();
    }

    private void collectChildren(String path, ZooKeeper zk, List<String> paths) throws InterruptedException, KeeperException {
        List<String> children = zk.getChildren(path, false);
        for (String child : children) {
            String childPath = path + "/" + child;
            collectChildren(childPath, zk, paths);
            paths.add(childPath);
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
        try {
            HTableDescriptor[] tables = admin.listTables();
            System.out.println("Found tables: " + (tables == null ? "null" : tables.length));
            tables = tables == null ? new HTableDescriptor[0] : tables;

            Set<String> exploitTimestampTables = new HashSet<String>();

            for (HTableDescriptor table : tables) {
                if (RETAIN_TABLES.contains(table.getNameAsString())) {
                    if (retainReport.length() > 0) {
                        retainReport.append(", ");
                    }
                    retainReport.append(table.getNameAsString());
                    continue;
                }

                if (Bytes.equals(table.getValue(LilyHBaseSchema.TABLE_TYPE_PROPERTY), LilyHBaseSchema.TABLE_TYPE_RECORD)
                        && !table.getNameAsString().equals(LilyHBaseSchema.Table.RECORD.name)) {
                    // Drop all record tables that are not the default table of the public tenant
                    admin.disableTable(table.getName());
                    admin.deleteTable(table.getName());
                } else {
                    HTable htable = new HTable(conf, table.getName());

                    if (timestampReusingTables.containsKey(table.getNameAsString())) {
                        insertTimestampTableTestRecord(table.getNameAsString(), htable,
                                timestampReusingTables.get(table.getNameAsString()));
                        exploitTimestampTables.add(table.getNameAsString());
                    }

                    int totalCount = clearTable(htable);

                    if (truncateReport.length() > 0) {
                        truncateReport.append(", ");
                    }
                    truncateReport.append(table.getNameAsString()).append(" (").append(totalCount).append(")");

                    htable.close();

                    if (timestampReusingTables.containsKey(table.getNameAsString())) {
                        admin.flush(table.getNameAsString());
                        admin.majorCompact(table.getName());
                    }
                }
            }

            truncateReport.insert(0, "Truncated the following tables: ");
            retainReport.insert(0, "Did not truncate the following tables: ");

            System.out.println(truncateReport);
            System.out.println(retainReport);

            waitForTimestampTables(exploitTimestampTables, timestampReusingTables);

            System.out.println("------------------------------------------------------------------------");
        } finally {
            Closer.close(admin);
        }
    }

    public static int clearTable(HTable htable) throws IOException {
        Scan scan = new Scan();
        scan.setCaching(1000);
        scan.setCacheBlocks(false);
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
        scanner.close();
        return totalCount;
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
            HTable htable = null;
            try {
                htable = new HTable(conf, tableName);

                byte[] CF = timestampReusingTables.get(tableName);
                byte[] tmpRowKey = waitForCompact(tableName, CF);

                // Delete our dummy row again
                htable.delete(new Delete(tmpRowKey));
            } finally {
                if (htable != null) {
                    htable.close();
                }
            }

        }
    }

    private byte[] waitForCompact(String tableName, byte[] CF) throws IOException, InterruptedException {
        byte[] tmpRowKey = Bytes.toBytes("HBaseProxyDummyRow");
        byte[] COL = Bytes.toBytes("DummyColumn");
        HTable htable = null;
        try {
            htable = new HTable(conf, tableName);
            System.out.println("Waiting for flush/compact of " + tableName + " table to complete");
            byte[] value = null;
            long waitStart = System.currentTimeMillis();
            while (value == null) {
                Put put = new Put(tmpRowKey);
                put.add(CF, COL, 1, new byte[] { 0 });
                htable.put(put);

                Get get = new Get(tmpRowKey);
                Result result = htable.get(get);
                value = result.getValue(CF, COL);
                if (value == null) {
                    // If the value is null, it is because the delete marker has not yet been flushed/compacted away
                    Thread.sleep(500);
                }

                long totalWait = System.currentTimeMillis() - waitStart;
                if (totalWait > 5000) {
                    HBaseAdmin admin = new HBaseAdmin(conf);
                    try {
                        CompactionState compactionState = admin.getCompactionState(tableName);
                        if (compactionState != CompactionState.MAJOR && compactionState != CompactionState.MAJOR_AND_MINOR) {
                            System.out.println("Re-requesting major compaction on " + tableName);
                            admin.majorCompact(tableName);
                        }
                    } finally {
                        Closer.close(admin);
                    }
                    waitStart = System.currentTimeMillis();
                }
            }
            return tmpRowKey;
        } finally {
            if (htable != null) {
                htable.close();
            }
        }
    }

    public void cleanBlobStore(URI dfsUri) throws Exception {
        FileSystem fs = FileSystem.get(new URI(dfsUri.getScheme() + "://" + dfsUri.getAuthority()), conf);
        Path blobRootPath = new Path(dfsUri.getPath());
        fs.delete(blobRootPath, true);
    }

    /**
     * Removes nay HBase replication peers. This method does not wait for the actual related processes to stop,
     * for this {@link ReplicationPeerUtil#waitOnReplicationPeerStopped(String)} can be used on the list of
     * returned peer id's.
     */
    public List<String> cleanHBaseReplicas() throws Exception {
        ReplicationAdmin repliAdmin = new ReplicationAdmin(conf);
        List<String> removedPeers = new ArrayList<String>();
        try {
            for (String peerId : repliAdmin.listPeers().keySet()) {
                repliAdmin.removePeer(peerId);
                removedPeers.add(peerId);
            }
        } finally {
            Closer.close(repliAdmin);
        }
        return removedPeers;
    }
}
