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
package org.lilyproject.repository.impl;

import javax.management.ObjectName;

import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingInt;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;
import org.lilyproject.util.hbase.metrics.MBeanUtil;
import org.lilyproject.util.hbase.metrics.MetricsDynamicMBeanBase;

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
            if (mbeanName != null) {
                MBeanUtil.unregisterMBean(mbeanName);
            }
        }
    }
}
