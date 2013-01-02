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
package org.lilyproject.clientmetrics;

import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeDataSupport;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

/**
 * Various utility methods to pull interesting data from HBase.
 */
public class HBaseMetrics {
    private HBaseAdmin hbaseAdmin;
    private JmxConnections jmxConnections = new JmxConnections();
    private static final String HBASE_JMX_PORT = "10102";

    private ObjectName regionServerStats = new ObjectName("hadoop:service=RegionServer,name=RegionServerStatistics");
    ;
    private ObjectName operationSystem = new ObjectName("java.lang:type=OperatingSystem");
    private ObjectName memory = new ObjectName("java.lang:type=Memory");
    private ObjectName threading = new ObjectName("java.lang:type=Threading");
    private boolean gcMetricsInitialized = false;
    private Map<String, ObjectName> garbageCollectionNames = new HashMap<String, ObjectName>();

    public HBaseMetrics(HBaseAdmin hbaseAdmin) throws MasterNotRunningException, MalformedObjectNameException {
        this.hbaseAdmin = hbaseAdmin;
    }

    public void close() {
        jmxConnections.close();
    }

    public void reportMetrics(Metrics metrics) throws Exception {
        ClusterStatus clusterStatus = hbaseAdmin.getClusterStatus();
        for (ServerName serverName : clusterStatus.getServers()) {
            int hbaseLoad = clusterStatus.getLoad(serverName).getLoad();

            MBeanServerConnection connection = jmxConnections.getConnector(serverName.getHostname(), HBASE_JMX_PORT)
                    .getMBeanServerConnection();
            initGarbageCollMetrics(connection);

            Integer blockCacheHitRatio = (Integer)connection.getAttribute(regionServerStats, "blockCacheHitRatio");
            Double sysLoadAvg = (Double)connection.getAttribute(operationSystem, "SystemLoadAverage");
            Integer threadCount = (Integer)connection.getAttribute(threading, "ThreadCount");

            CompositeDataSupport heapMemUsage = (CompositeDataSupport)connection.getAttribute(memory, "HeapMemoryUsage");
            double usedHeapMB = ((double)(Long)heapMemUsage.get("used")) / 1024d / 1024d;

            // The dash at the beginning of the name is a hint towards the MetricsReportTool that the avg/min/max
            // is the same (there is only one value per interval)
            // The usage of the @ symbol is also a hint for MetricsReportTool: it will group all values with the
            // same text before the @ symbol.
            metrics.increment("-hbaseLoad@" + serverName, hbaseLoad);
            metrics.increment("-blockCacheHitRatio@" + serverName, blockCacheHitRatio);
            metrics.increment("-sysLoadAvg@" + serverName, sysLoadAvg);
            metrics.increment("-hbaseUsedHeap@" + serverName, usedHeapMB);
            metrics.increment("-hbaseThreadCount@" + serverName, threadCount);

            for (Map.Entry<String, ObjectName> entry : garbageCollectionNames.entrySet()) {
                long collectionCount = (Long)connection.getAttribute(entry.getValue(), "CollectionCount");
                long collectionTime = (Long)connection.getAttribute(entry.getValue(), "CollectionTime");

                metrics.increment("-" + entry.getKey() + "CollCount@" + serverName, collectionCount);
                metrics.increment("-" + entry.getKey() + "CollTime@" + serverName, collectionTime);
            }
        }
    }

    private void initGarbageCollMetrics(MBeanServerConnection connection) throws Exception {
        if (!gcMetricsInitialized) {
            gcMetricsInitialized = true;
            Set<ObjectInstance> mbeans = connection.queryMBeans(new ObjectName("java.lang:type=GarbageCollector,name=*"), null);
            for (ObjectInstance instance : mbeans) {
                garbageCollectionNames.put(instance.getObjectName().getKeyProperty("name"), instance.getObjectName());
            }
        }
    }

    public void reportRequestCountMetric(Metrics metrics) throws Exception {
        ClusterStatus clusterStatus = hbaseAdmin.getClusterStatus();
        for (ServerName serverName : clusterStatus.getServers()) {
            int requestCount = clusterStatus.getLoad(serverName).getNumberOfRequests();
            metrics.increment("-hbaseRequestCount@" + serverName.getHostname(), requestCount);
        }
    }

    public Collection<TableInfo> getHBaseTableInfo(ClusterStatus clusterStatus) throws IOException {
        SortedMap<String, TableInfo> tableInfos = new TreeMap<String, TableInfo>();

        for (ServerName serverName : clusterStatus.getServers()) {
            for (HServerLoad.RegionLoad regionLoad : clusterStatus.getLoad(serverName).getRegionsLoad().values()) {
                String regionName = regionLoad.getNameAsString();
                String tableName = getTableNameFromRegionName(regionName);

                TableInfo table = tableInfos.get(tableName);
                if (table == null) {
                    table = new TableInfo();
                    table.name = tableName;
                    tableInfos.put(tableName, table);
                }

                table.stores = Math.max(table.stores, regionLoad.getStores());
                table.regions++;
                table.storefiles += regionLoad.getStorefiles();
                table.storefilesMB += regionLoad.getStorefileSizeMB();
                table.memStoreMB += regionLoad.getMemStoreSizeMB();
            }
        }

        for (Map.Entry<String, TableInfo> entry : tableInfos.entrySet()) {
            HTableInterface table = new HTable(hbaseAdmin.getConfiguration(), entry.getKey());
            Scan scan = new Scan();
            scan.setCaching(100);
            scan.setCacheBlocks(false);
            scan.setFilter(new FirstKeyOnlyFilter());
            ResultScanner scanner = table.getScanner(scan);
            int rowCount = 0;
            while (scanner.next() != null && rowCount <= 5000) {
                rowCount++;
            }

            entry.getValue().rows = rowCount;
            scanner.close();
            table.close();
        }

        return tableInfos.values();
    }

    private String getTableNameFromRegionName(String regionName) {
        int commaPos = regionName.indexOf(',');
        String tableName = regionName.substring(0, commaPos);
        return tableName;
    }

    public static class TableInfo {
        String name;
        int stores;
        int regions;
        int storefiles;
        int storefilesMB;
        int memStoreMB;
        int rows;
    }

    public void outputHBaseState(PrintStream stream) throws IOException {
        ClusterStatus clusterStatus = hbaseAdmin.getClusterStatus();
        Collection<TableInfo> tableInfos = getHBaseTableInfo(clusterStatus);

        Table table = new Table(stream);
        table.addColumn(20, "Table name", "s");
        table.addColumn(-1, "# stores", "d");
        table.addColumn(-1, "# regions", "d");
        table.addColumn(-1, "# storesfiles", "d");
        table.addColumn(-1, "storefiles MB", "d");
        table.addColumn(-1, "memstore MB", "d");
        table.addColumn(-1, "# rows", "s", false, 0);
        table.finishDefinition();

        table.fullSepLine();
        table.crossColumn("HBase version: " + clusterStatus.getHBaseVersion());
        table.crossColumn("# regions in transition: " + clusterStatus.getRegionsInTransition().size());
        table.crossColumn("The information below is from summarizing HBaseAdmin.ClusterStatus (= only online regions)");
        table.columnSepLine();
        table.titles();
        table.columnSepLine();

        for (TableInfo tableInfo : tableInfos) {
            String rowCount = tableInfo.rows < 5000 ? String.valueOf(tableInfo.rows) : "> 5000";

            table.columns(tableInfo.name, tableInfo.stores, tableInfo.regions, tableInfo.storefiles,
                    tableInfo.storefilesMB, tableInfo.memStoreMB, rowCount);
        }

        table.columnSepLine();
    }

    public void outputRegionServersInfo(PrintStream ps) throws Exception {
        Table table = new Table(ps);
        table.addColumn(30, "Regionserver", "s");
        table.addColumn(6, "Arch", "s");
        table.addColumn(-1, "# CPU", "d");
        table.addColumn(15, "OS Version", "s");
        table.addColumn(-1, "Physical mem", "f");
        table.addColumn(-1, "JVM version", "s");
        table.addColumn(-1, "Max heap", "f");
        table.addColumn(-1, "Used heap", "f");
        table.finishDefinition();

        table.fullSepLine();
        table.crossColumn("Information on the HBase region servers");
        table.columnSepLine();
        table.titles();
        table.columnSepLine();

        ObjectName runtime = new ObjectName("java.lang:type=Runtime");

        ClusterStatus clusterStatus = hbaseAdmin.getClusterStatus();
        for (ServerName serverName : clusterStatus.getServers()) {
            String hostname = serverName.getHostname();
            MBeanServerConnection connection = jmxConnections.getConnector(hostname, HBASE_JMX_PORT).getMBeanServerConnection();

            String arch = (String)connection.getAttribute(operationSystem, "Arch");
            Integer processors = (Integer)connection.getAttribute(operationSystem, "AvailableProcessors");
            String osVersion = (String)connection.getAttribute(operationSystem, "Version");
            long physicalMem = (Long)connection.getAttribute(operationSystem, "TotalPhysicalMemorySize");
            double physicalMemMB = ((double)physicalMem) / 1024d / 1024d;

            String vmVersion = (String)connection.getAttribute(runtime, "VmVersion");

            CompositeDataSupport heapMemUsage = (CompositeDataSupport)connection.getAttribute(memory, "HeapMemoryUsage");
            double maxHeapMB = ((double)(Long)heapMemUsage.get("max")) / 1024d / 1024d;
            double usedHeapMB = ((double)(Long)heapMemUsage.get("used")) / 1024d / 1024d;

            table.columns(hostname, arch, processors, osVersion, physicalMemMB, vmVersion, maxHeapMB, usedHeapMB);
        }

        table.columnSepLine();
    }

    public void outputRegionCountByServer(PrintStream ps) throws IOException {
        Table table = new Table(ps);
        table.addColumn(30, "Regionserver", "s");
        table.addColumn(30, "Table name", "s");
        table.addColumn(-1, "Region count", "d");
        table.finishDefinition();

        table.fullSepLine();
        table.crossColumn("Number of regions for each table on each region server");
        table.columnSepLine();
        table.titles();
        table.columnSepLine();

        ClusterStatus clusterStatus = hbaseAdmin.getClusterStatus();
        for (ServerName serverName : clusterStatus.getServers()) {
            Map<String, Integer> regionCounts = new HashMap<String, Integer>();
            for (HServerLoad.RegionLoad regionLoad : clusterStatus.getLoad(serverName).getRegionsLoad().values()) {
                String tableName = getTableNameFromRegionName(regionLoad.getNameAsString());
                if (regionCounts.containsKey(tableName)) {
                    regionCounts.put(tableName, regionCounts.get(tableName) + 1);
                } else {
                    regionCounts.put(tableName, 1);
                }
            }

            String hostName = serverName.getHostname();

            for (Map.Entry<String, Integer> entry : regionCounts.entrySet()) {
                table.columns(hostName, entry.getKey(), entry.getValue());
            }
        }

        table.columnSepLine();
    }
}
