/*
 * Copyright 2010 Outerthought bvba
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
package org.lilyproject.server.modules.rowlog;

import java.io.IOException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.KeeperException;
import org.kauriproject.conf.Conf;
import org.lilyproject.rowlock.HBaseRowLocker;
import org.lilyproject.rowlock.RowLocker;
import org.lilyproject.rowlog.api.*;
import org.lilyproject.rowlog.impl.*;
import org.lilyproject.util.LilyInfo;
import org.lilyproject.util.hbase.HBaseTableFactory;
import org.lilyproject.util.hbase.LilyHBaseSchema;
import org.lilyproject.util.hbase.LilyHBaseSchema.RecordCf;
import org.lilyproject.util.hbase.LilyHBaseSchema.RecordColumn;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.zookeeper.LeaderElectionSetupException;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

public class RowLogSetup {
    private final RowLogConfigurationManager confMgr;
    private final Configuration hbaseConf;
    private final ZooKeeperItf zk;
    private RowLogImpl messageQueue;
    private WalRowLog writeAheadLog;
    private RowLogProcessorElection messageQueueProcessorLeader;
    private RowLogProcessorElection writeAheadLogProcessorLeader;
    private Thread walProcessorStartupThread;
    private final HBaseTableFactory hbaseTableFactory;
    private final Conf rowLogConf;
    private final LilyInfo lilyInfo;
    private final RowLocker rowLocker;
    private final Log log = LogFactory.getLog(getClass());

    public RowLogSetup(RowLogConfigurationManager confMgr, ZooKeeperItf zk, Configuration hbaseConf,
            HBaseTableFactory hbaseTableFactory, Conf rowLogConf, RowLocker rowLocker, LilyInfo lilyInfo) {
        this.confMgr = confMgr;
        this.zk = zk;
        this.hbaseConf = hbaseConf;
        this.hbaseTableFactory = hbaseTableFactory;
        this.rowLogConf = rowLogConf;
        this.rowLocker = rowLocker;
        this.lilyInfo = lilyInfo;
    }

    @PostConstruct
    public void start() throws InterruptedException, KeeperException, IOException, LeaderElectionSetupException, RowLogException {
        if (!confMgr.rowLogExists("wal")) {
            confMgr.addRowLog("wal", new RowLogConfig(10000L, true, false, 200L, 5000L, 5000L));
        }
        
        if (!confMgr.rowLogExists("mq")) {
            confMgr.addRowLog("mq", new RowLogConfig(10000L, true, true, 200L, 0L, 5000L));
        }
        
        boolean linkIdxEnabled = rowLogConf.getChild("linkIndexUpdater").getAttributeAsBoolean("enabled", true);
        if (linkIdxEnabled) {
            if (!confMgr.subscriptionExists("wal", "LinkIndexUpdater")) {
                // If the subscription already exists, this method will silently return
                confMgr.addSubscription("wal", "LinkIndexUpdater", RowLogSubscription.Type.VM, 10);
            }
        } else {
            log.info("LinkIndexUpdater is disabled.");
            if (confMgr.subscriptionExists("wal", "LinkIndexUpdater")) {
                confMgr.removeSubscription("wal", "LinkIndexUpdater");
            }
        }

        boolean mqFeederEnabled = rowLogConf.getChild("mqFeeder").getAttributeAsBoolean("enabled", true);
        if (mqFeederEnabled) {
            if (!confMgr.subscriptionExists("wal", "MQFeeder")) {
                confMgr.addSubscription("wal", "MQFeeder", RowLogSubscription.Type.VM, 20);
            }
        } else {
            log.info("MQFeeder is disabled.");
            if (confMgr.subscriptionExists("wal", "MQFeeder")) {
                confMgr.removeSubscription("wal", "MQFeeder");
            }
        }

        messageQueue = new RowLogImpl("mq", LilyHBaseSchema.getRecordTable(hbaseTableFactory), RecordCf.ROWLOG.bytes,
                RecordColumn.MQ_PREFIX, confMgr, null);
        RowLogShard mqShard = new RowLogShardImpl("shard1", hbaseConf, messageQueue, 100, hbaseTableFactory);
        messageQueue.registerShard(mqShard);

        writeAheadLog = new WalRowLog("wal", LilyHBaseSchema.getRecordTable(hbaseTableFactory), RecordCf.ROWLOG.bytes,
                RecordColumn.WAL_PREFIX, confMgr, rowLocker);
        RowLogShard walShard = new RowLogShardImpl("shard1", hbaseConf, writeAheadLog, 100, hbaseTableFactory);
        writeAheadLog.registerShard(walShard);

        RowLogMessageListenerMapping.INSTANCE.put("WAL", new WalListener(writeAheadLog));
        RowLogMessageListenerMapping.INSTANCE.put("MQFeeder", new MessageQueueFeeder(messageQueue));

        // Start the message queue processor
        boolean mqProcEnabled = rowLogConf.getChild("mqProcessor").getAttributeAsBoolean("enabled", true);
        if (mqProcEnabled) {
            messageQueueProcessorLeader = new RowLogProcessorElection(zk, new RowLogProcessorImpl(messageQueue, confMgr), lilyInfo);
            messageQueueProcessorLeader.start();
        } else {
            log.info("Not participating in MQ processor election.");
        }
        
        if (linkIdxEnabled) {
            confMgr.addListener("wal", "LinkIndexUpdater", "LinkIndexUpdaterListener");
        }

        if (mqFeederEnabled) {
            confMgr.addListener("wal", "MQFeeder", "MQFeederListener");
        }

        // Start the wal processor
        boolean walProcEnabled = rowLogConf.getChild("walProcessor").getAttributeAsBoolean("enabled", true);
        if (walProcEnabled) {
            writeAheadLogProcessorLeader = new RowLogProcessorElection(zk, new WalProcessor(writeAheadLog, confMgr), lilyInfo);
            // The WAL processor should only be started once the LinkIndexUpdater listener is available
            walProcessorStartupThread = new Thread(new DelayedWALProcessorStartup());
            walProcessorStartupThread.start();
        } else {
            log.info("Not participating in WAL processor election.");
        }
    }

    @PreDestroy
    public void stop() throws RowLogException, InterruptedException, KeeperException {
        Closer.close(messageQueueProcessorLeader);
        if (walProcessorStartupThread != null && walProcessorStartupThread.isAlive()) {
            walProcessorStartupThread.interrupt();
            walProcessorStartupThread.join();
        }
        Closer.close(writeAheadLogProcessorLeader);
        Closer.close(messageQueue);
        Closer.close(writeAheadLog);
        confMgr.removeListener("wal", "LinkIndexUpdater", "LinkIndexUpdaterListener");
        confMgr.removeListener("wal", "MQFeeder", "MQFeederListener");
        RowLogMessageListenerMapping.INSTANCE.remove("MQFeeder");        
    }

    public RowLog getMessageQueue() {
        return messageQueue;
    }

    public RowLog getWriteAheadLog() {
        return writeAheadLog;
    }

    private class DelayedWALProcessorStartup implements Runnable {
        public void run() {
            long timeOut = 5 * 60 * 1000; // 5 minutes
            long waitUntil = System.currentTimeMillis() + timeOut;
            while (RowLogMessageListenerMapping.INSTANCE.get("LinkIndexUpdater") == null) {
                if (System.currentTimeMillis() > waitUntil) {
                    log.error("IMPORTANT: LinkIndexUpdater did not appear in RowLogMessageListenerMapping after" +
                            " waiting for " + timeOut + "ms. Will not start up WAL processor.");
                    return;
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    return;
                }
            }

            try {
                writeAheadLogProcessorLeader.start();
            } catch (Throwable t) {
                log.error("Error starting up WAL processor", t);
            }
        }
    }
}
