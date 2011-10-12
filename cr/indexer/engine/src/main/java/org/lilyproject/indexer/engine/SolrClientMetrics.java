package org.lilyproject.indexer.engine;

import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingLong;
import org.lilyproject.util.hbase.metrics.MBeanUtil;
import org.lilyproject.util.hbase.metrics.MetricsDynamicMBeanBase;

import javax.management.ObjectName;

public class SolrClientMetrics implements Updater {
    private final String recordName;
    private final MetricsRegistry registry = new MetricsRegistry();
    private final MetricsRecord metricsRecord;
    private final SolrClientMetricsMBean mbean;
    private final MetricsContext context;

    /** Counts number of times a Solr operation has been retried because of connection-related problems. */
    public MetricsTimeVaryingLong retries = new MetricsTimeVaryingLong("retries", registry);

    public SolrClientMetrics(String indexName, String shardName) {
        this.recordName = indexName + "_" + shardName;
        context = MetricsUtil.getContext("solrClient");
        metricsRecord = MetricsUtil.createRecord(context, recordName);
        context.registerUpdater(this);
        mbean = new SolrClientMetricsMBean(this.registry);
    }

    public void shutdown() {
        context.unregisterUpdater(this);
        mbean.shutdown();
    }

    @Override
    public void doUpdates(MetricsContext metricsContext) {
        synchronized (this) {
          for (MetricsBase m : registry.getMetricsList()) {
            m.pushMetric(metricsRecord);
          }
        }
        metricsRecord.update();
    }

    public class SolrClientMetricsMBean extends MetricsDynamicMBeanBase {
        private final ObjectName mbeanName;

        public SolrClientMetricsMBean(MetricsRegistry registry) {
            super(registry, "Solr client");

            mbeanName = MBeanUtil.registerMBean("SolrClient", recordName, this);
        }

        public void shutdown() {
            if (mbeanName != null)
                MBeanUtil.unregisterMBean(mbeanName);
        }
    }
}
