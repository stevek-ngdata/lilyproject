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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.lilyproject.util.hbase.HBaseAdminFactory;

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

        HBaseAdmin admin = HBaseAdminFactory.get(conf);
        HTableDescriptor[] tables = admin.listTables();
        System.out.println("Found tables: " + (tables == null ? "null" : tables.length));
        tables = tables == null ? new HTableDescriptor[0] : tables;

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

            int totalCount = clearTable(htable);

            if (truncateReport.length() > 0)
                truncateReport.append(", ");
            truncateReport.append(table.getNameAsString()).append(" (").append(totalCount).append(")");

            htable.close();

            if (timestampReusingTables.containsKey(table.getNameAsString())) {
                admin.flush(table.getNameAsString());
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
                if (htable != null) htable.close();
            }

        }
    }

    private byte[] waitForCompact(String tableName, byte[] CF) throws IOException, InterruptedException {
        byte[] tmpRowKey = Bytes.toBytes("HBaseProxyDummyRow");
        byte[] COL = Bytes.toBytes("DummyColumn");
        HTable htable = null;
        try {
            htable = new HTable(conf, tableName);

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
        } finally {
            if (htable != null) htable.close();
        }
    }

    /** Force a major compaction and wait for it to finish.
     *  This method can be used in a test to avoid issue HBASE-2256 after performing a delete operation
     *  Uses same principle as {@link #cleanTables}
     */
    public void majorCompact(String tableName, String[] columnFamilies) throws Exception {
        byte[] tmpRowKey = Bytes.toBytes("HBaseProxyDummyRow");
        byte[] COL = Bytes.toBytes("DummyColumn");
        HBaseAdmin admin = HBaseAdminFactory.get(conf);
        HTable htable = null;
        try {
            htable = new HTable(conf, tableName);

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
            admin.flush(tableName);
            admin.majorCompact(tableName);

            // Wait for compact to finish
            for (String columnFamily : columnFamilies) {
                byte[] CF = Bytes.toBytes(columnFamily);
                waitForCompact(tableName, CF);
            }
        } finally {
            htable.close();
        }
    }

    public void cleanBlobStore(URI dfsUri) throws Exception {
        FileSystem fs = FileSystem.get(new URI(dfsUri.getScheme() + "://" + dfsUri.getAuthority()), conf);
        Path blobRootPath = new Path(dfsUri.getPath());
        fs.delete(blobRootPath, true);
    }

    public void cleanHBaseReplicas() throws Exception {
        ReplicationAdmin repliAdmin = new ReplicationAdmin(conf);
        for (String peerId : repliAdmin.listPeers().keySet()) {
            repliAdmin.removePeer(peerId);
        }
    }
}
