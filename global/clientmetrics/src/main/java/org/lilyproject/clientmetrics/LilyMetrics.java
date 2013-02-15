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

import org.joda.time.Period;
import org.joda.time.format.PeriodFormat;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeDataSupport;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Various utility methods to pull interesting data from Lily.
 */
public class LilyMetrics {
    private ZooKeeperItf zk;
    private JmxConnections jmxConnections = new JmxConnections();
    private static final String LILY_JMX_PORT = "10202";

    public LilyMetrics(ZooKeeperItf zk) {
        this.zk = zk;
    }

    public void close() {
        jmxConnections.close();
    }

    public void outputLilyServerInfo(PrintStream ps) throws Exception {
        Table table = new Table(ps);
        table.addColumn(30, "Lily server", "s");
        table.addColumn(10, "Version", "s");
        table.addColumn(-1, "IndexerMaster", "s");
        table.addColumn(-1, "Max heap", "f");
        table.addColumn(-1, "Used heap", "f");

        table.finishDefinition();

        List<String> lilyServers = zk.getChildren("/lily/repositoryNodes", false);

        table.fullSepLine();
        table.crossColumn("Information on the Lily servers");
        table.columnSepLine();
        table.titles();
        table.columnSepLine();

        ObjectName lily = new ObjectName("Lily:name=Info");
        ObjectName memory = new ObjectName("java.lang:type=Memory");

        // This data structure is used to collect for each index, and for each server, the timestamp of the
        // last WALEdit processed by the SEP
        Map<String, Map<String, Long>> sepTimestampByIndexAndServer = new HashMap<String, Map<String, Long>>() {
            @Override
            public Map<String, Long> get(Object key) {
                Map<String, Long> value = super.get(key);
                if (value == null) {
                    value = new HashMap<String, Long>();
                    put((String)key, value);
                }
                return value;
            }
        };

        for (String server : lilyServers) {
            int colonPos = server.indexOf(':');
            String address = server.substring(0, colonPos);

            MBeanServerConnection connection = jmxConnections.getConnector(address, LILY_JMX_PORT).getMBeanServerConnection();
            String version = (String)connection.getAttribute(lily, "Version");
            boolean indexerMaster = (Boolean)connection.getAttribute(lily, "IndexerMaster");

            CompositeDataSupport heapMemUsage = (CompositeDataSupport)connection.getAttribute(memory, "HeapMemoryUsage");
            double maxHeapMB = ((double)(Long)heapMemUsage.get("max")) / 1024d / 1024d;
            double usedHeapMB = ((double)(Long)heapMemUsage.get("used")) / 1024d / 1024d;

            table.columns(address, version, String.valueOf(indexerMaster), maxHeapMB, usedHeapMB);

            ObjectName indexUpdaterPattern = new ObjectName("Lily:service=SEP,name=IndexUpdater_*");
            Set<ObjectName> indexUpdaterNames = connection.queryNames(indexUpdaterPattern, null);
            for (ObjectName indexUpdater : indexUpdaterNames) {
                Long lastSepTimestamp = (Long)connection.getAttribute(indexUpdater, "lastSepTimestamp");
                String indexName = indexUpdater.getKeyProperty("name").substring("IndexUpdater_".length());
                sepTimestampByIndexAndServer.get(indexName).put(server, lastSepTimestamp);
            }
        }

        table.columnSepLine();

        if (sepTimestampByIndexAndServer.size() > 0) {
            long now = System.currentTimeMillis();
            ps.println();
            ps.println("SEP: how long ago was the timestamp of the last processed event:");
            for (Map.Entry<String, Map<String, Long>> indexEntry : sepTimestampByIndexAndServer.entrySet()) {
                ps.println("  Index " + indexEntry.getKey());
                for (Map.Entry<String, Long> serverEntry : indexEntry.getValue().entrySet()) {
                    Period duration = new Period(now - serverEntry.getValue());
                    ps.println("    Server " + serverEntry.getKey() + ": " + PeriodFormat.getDefault().print(duration));
                }
            }
        }
    }
}
