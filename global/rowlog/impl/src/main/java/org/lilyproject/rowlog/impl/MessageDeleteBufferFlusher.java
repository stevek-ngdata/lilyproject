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
package org.lilyproject.rowlog.impl;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.rowlog.api.RowLog;
import org.lilyproject.rowlog.api.RowLogShard;

/**
 * Flushes the rowlog message delete buffers regularly.
 * <p/>
 * <p>This is only needed for the 'WAL' rowlog case.
 * <p/>
 * <p>In case of the MQ, all deletes are done in the JVM where the rowlog processor runs, and thus
 * there will only be delete buffers in that JVM. When the rowlog processor performs a scan, it
 * will first flush the delete buffer, so in case of the MQ all outstanding deletes will always
 * be flushed before a scan is done.
 * <p/>
 * <p>In contrast, for the WAL, the rowlog message processing is done as part of the record CRUD flow,
 * thus there are delete buffers in each JVM, and for each rowlog shard. E.g. if you have 6 nodes
 * and 12 shards, that gives 72 delete buffers in total across the cluster. These delete buffers
 * don't get flushed until they either get a certain size (100) or a certain time is passed (5 min),
 * and they don't get flushed at all if there is no activity.
 * <p/>
 * <p>Therefore, it would often occur that the WAL
 * processor will scan messages which are in fact already processed, but the delete is still
 * pending in the buffer. As long as the 'orphaned message delay' is not passed, the WAL processor
 * would re-scan the messages, and finally delete them itself if still there after the orphaned
 * message delay, thus leading to double deletes of the same row and possibly continues WAL
 * scan loops if there are more than a full batch of these messages.
 * <p/>
 * <p>To avoid all this, we let the delete buffers be flushed by a background thread, which runs
 * more frequent than the minimal process delay of the WAL, so that in general, there should
 * not turn up any (or few) already processed rows when the WAL processor scans the global queue.
 */
public class MessageDeleteBufferFlusher {
    private ScheduledExecutorService scheduledServices;
    private RowLog rowLog;
    private int interval;
    private Log log = LogFactory.getLog(getClass());

    public MessageDeleteBufferFlusher(RowLog rowLog, int interval) {
        this.rowLog = rowLog;
        this.interval = interval;
    }

    @PostConstruct
    public void start() {
        this.scheduledServices = Executors.newScheduledThreadPool(1);
        this.scheduledServices.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    // Since by definition each shard is a different region, it might make
                    // sense to do this multi-threaded. Or collect all the deletes and
                    // send them as one big delete to HBase, which would then they care of it.
                    for (RowLogShard shard : rowLog.getShards()) {
                        shard.flushMessageDeleteBuffer();
                    }
                } catch (Throwable t) {
                    log.error("Error flushing rowlog message delete buffer", t);
                }
            }
        }, interval, interval, TimeUnit.SECONDS);

    }

    @PreDestroy
    public void stop() {
        if (scheduledServices != null) {
            scheduledServices.shutdown();
        }
    }
}
