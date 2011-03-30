package org.lilyproject.rowlock;

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

public class RowLockerMetrics implements Updater {
    private final MetricsRegistry registry = new MetricsRegistry();
    private final MetricsRecord metricsRecord;
    private final RowLockerMetricsMBean mbean;
    private final MetricsContext context;

    public MetricsTimeVaryingLong contentions = new MetricsTimeVaryingLong("contentions", registry);

    public RowLockerMetrics() {
        context = MetricsUtil.getContext("rowLocker");
        metricsRecord = MetricsUtil.createRecord(context, "rowLocker");
        context.registerUpdater(this);
        mbean = new RowLockerMetricsMBean(this.registry);
    }

    public void shutdown() {
        context.unregisterUpdater(this);
        mbean.shutdown();
    }

    public void doUpdates(MetricsContext metricsContext) {
        synchronized (this) {
          for (MetricsBase m : registry.getMetricsList()) {
            m.pushMetric(metricsRecord);
          }
        }
        metricsRecord.update();
    }

    public class RowLockerMetricsMBean extends MetricsDynamicMBeanBase {
        private final ObjectName mbeanName;

        public RowLockerMetricsMBean(MetricsRegistry registry) {
            super(registry, "Lily Indexer");

            mbeanName = MBeanUtil.registerMBean("Row Locker", "rowLocker", this);
        }

        public void shutdown() {
            if (mbeanName != null)
                MBeanUtil.unregisterMBean(mbeanName);
        }
    }
}

