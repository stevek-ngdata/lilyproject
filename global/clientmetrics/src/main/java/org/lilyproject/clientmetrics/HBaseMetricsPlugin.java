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

import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.util.Collections;
import java.util.List;

public class HBaseMetricsPlugin implements MetricsPlugin {
    private HBaseMetrics hbaseMetrics;
    private HBaseAdmin hbaseAdmin;
    private boolean useJmx;
    private long lastRequestCountReport;

    public HBaseMetricsPlugin(HBaseMetrics hbaseMetrics, HBaseAdmin hbaseAdmin, boolean useJmx) throws MasterNotRunningException {
        this.hbaseAdmin = hbaseAdmin;
        this.hbaseMetrics = hbaseMetrics;
        this.useJmx = useJmx;
    }

    @Override
    public void beforeReport(Metrics metrics) {
        if (!useJmx)
            return;

        try {
            hbaseMetrics.reportMetrics(metrics);
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    @Override
    public void afterIncrement(Metrics metrics) {
        try {
            long now = System.currentTimeMillis();
            // the 3000 is the default value of hbase.regionserver.msginterval
            if (now - lastRequestCountReport > 3000) {
                lastRequestCountReport = now;
                hbaseMetrics.reportRequestCountMetric(metrics);
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    @Override
    public List<String> getExtraInfoLines() {
        try {
            ClusterStatus clusterStatus = hbaseAdmin.getClusterStatus();

            int deadServers = clusterStatus.getDeadServers();
            int liveServers = clusterStatus.getServers();
            int regionCount = clusterStatus.getRegionsCount();

            String line = String.format("HBase cluster status: dead servers: %1$d, live servers: %2$d, regions: %3$d",
                    deadServers, liveServers, regionCount);

            return Collections.singletonList(line);
        } catch (Throwable t) {
            t.printStackTrace();
        }
        return Collections.emptyList();
    }
}
