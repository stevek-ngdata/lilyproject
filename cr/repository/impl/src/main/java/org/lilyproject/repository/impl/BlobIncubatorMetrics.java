package org.lilyproject.repository.impl;

import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.util.*;
import org.lilyproject.util.hbase.metrics.MBeanUtil;
import org.lilyproject.util.hbase.metrics.MetricsDynamicMBeanBase;

import javax.management.ObjectName;

public class BlobIncubatorMetrics implements Updater {
    private final MetricsRegistry registry = new MetricsRegistry();
    private final MetricsRecord metricsRecord;
    private final BlobIncubatorMetricsMXBean mbean;
    private final MetricsContext context;

    public MetricsTimeVaryingRate runDuration = new MetricsTimeVaryingRate("run_duration", registry);

    public MetricsTimeVaryingRate checkDuration = new MetricsTimeVaryingRate("check_duration", registry);

    public MetricsTimeVaryingInt blobDeleteCount = new MetricsTimeVaryingInt("blob_delete_cnt", registry);
    public MetricsTimeVaryingInt refDeleteCount = new MetricsTimeVaryingInt("ref_delete_cnt", registry);
    
    public BlobIncubatorMetrics() {
        context = MetricsUtil.getContext("blobIncubator");
        metricsRecord = MetricsUtil.createRecord(context, "blobIncubator");
        context.registerUpdater(this);
        mbean = new BlobIncubatorMetricsMXBean(this.registry);
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

    public class BlobIncubatorMetricsMXBean extends MetricsDynamicMBeanBase {
        private final ObjectName mbeanName;

        public BlobIncubatorMetricsMXBean(MetricsRegistry registry) {
            super(registry, "Lily Blob Incubator");

            mbeanName = MBeanUtil.registerMBean("Blob Incubator", "blobIncubator", this);
        }

        public void shutdown() {
            if (mbeanName != null)
                MBeanUtil.unregisterMBean(mbeanName);
        }
    }
}
