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

    public void beforeReport(Metrics metrics) {
        if (!useJmx)
            return;

        try {
            hbaseMetrics.reportMetrics(metrics);
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

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
