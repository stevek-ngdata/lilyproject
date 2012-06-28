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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.KeeperException;
import org.kauriproject.conf.Conf;
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
    private final String hostName;

    public RowLogSetup(RowLogConfigurationManager confMgr, ZooKeeperItf zk, Configuration hbaseConf,
            HBaseTableFactory hbaseTableFactory, Conf rowLogConf, RowLocker rowLocker, LilyInfo lilyInfo,
            String hostName) {
        this.confMgr = confMgr;
        this.zk = zk;
        this.hbaseConf = hbaseConf;
        this.hbaseTableFactory = hbaseTableFactory;
        this.rowLogConf = rowLogConf;
        this.rowLocker = rowLocker;
        this.lilyInfo = lilyInfo;
        this.hostName = hostName;
    }
    
    private RowLogConfig createRowLogConfig(Conf initialConf) {
        boolean respectOrder = initialConf.getChild("respectOrder").getValueAsBoolean();
        boolean enableNotify = initialConf.getChild("enableNotify").getValueAsBoolean();
        long notifyDelay = initialConf.getChild("notifyDelay").getValueAsLong();
        long minimalProcessDelay = initialConf.getChild("minimalProcessDelay").getValueAsLong();
        long wakeupTimeout = initialConf.getChild("wakeupTimeout").getValueAsLong();
        long orphanedMessageDelay = initialConf.getChild("orphanedMessageDelay").getValueAsLong();
        int deleteBufferSize = initialConf.getChild("deleteBufferSize").getValueAsInteger();
        
        return new RowLogConfig(respectOrder, enableNotify, notifyDelay, minimalProcessDelay, wakeupTimeout,
                orphanedMessageDelay, deleteBufferSize);
    }

    @PostConstruct
    public void start() throws InterruptedException, KeeperException, IOException, LeaderElectionSetupException, RowLogException {
        if (!confMgr.rowLogExists("wal")) {
            confMgr.addRowLog("wal", createRowLogConfig(rowLogConf.getChild("mqConfig")));
        }
        
        if (!confMgr.rowLogExists("mq")) {
            confMgr.addRowLog("mq", createRowLogConfig(rowLogConf.getChild("walConfig")));
        } else {
            // Before Lily 1.0.4, the respectOrder parameter for the MQ was true. Change it if necessary.
            for (Map.Entry<String, RowLogConfig> entry : confMgr.getRowLogs().entrySet()) {
                if (entry.getKey().equals("mq") && entry.getValue().isRespectOrder()) {
                    log.warn("Changing MQ respect order to false.");
                    RowLogConfig config = entry.getValue();
                    config.setRespectOrder(false);
                    confMgr.updateRowLog("mq", config);
                }
            }
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

        int shardCount = rowLogConf.getChild("shardCount").getValueAsInteger();

        messageQueue = new RowLogImpl("mq", LilyHBaseSchema.getRecordTable(hbaseTableFactory), RecordCf.ROWLOG.bytes,
                RecordColumn.MQ_PREFIX, confMgr, null, new RowLogHashShardRouter());
        RowLogShardSetup.setupShards(shardCount, messageQueue, hbaseTableFactory);

        writeAheadLog = new WalRowLog("wal", LilyHBaseSchema.getRecordTable(hbaseTableFactory), RecordCf.ROWLOG.bytes,
                RecordColumn.WAL_PREFIX, confMgr, rowLocker, new RowLogHashShardRouter());
        RowLogShardSetup.setupShards(shardCount, writeAheadLog, hbaseTableFactory);

        RowLogMessageListenerMapping.INSTANCE.put(WalListener.ID, new WalListener(writeAheadLog, rowLocker));
        // Instead of using the default MQFeeder, a custom one is used to do selective feeding of indexer
        // related subscriptions, see IndexAwareMQFeeder.
        // RowLogMessageListenerMapping.INSTANCE.put("MQFeeder", new MessageQueueFeeder(messageQueue));

        // Start the message queue processor
        Conf mqProcessorConf = rowLogConf.getChild("mqProcessor");
        boolean mqProcEnabled = mqProcessorConf.getAttributeAsBoolean("enabled", true);
        if (mqProcEnabled) {
            List<String> mqProcessorNodes = Collections.EMPTY_LIST;
            Conf nodesConf = mqProcessorConf.getChild("nodes");
            if (nodesConf != null) {
                String nodes = nodesConf.getValue("");
                if (!nodes.isEmpty()) {
                    mqProcessorNodes = Arrays.asList(nodes.split(","));
                }
            }
            RowLogProcessorSettings settings = createProcessorSettings(mqProcessorConf);
            RowLogProcessor processor = new RowLogProcessorImpl(messageQueue, confMgr, hbaseConf, settings);
            messageQueueProcessorLeader = new RowLogProcessorElection(zk, processor, lilyInfo);
            if (mqProcessorNodes.isEmpty() || mqProcessorNodes.contains(hostName)) {
                messageQueueProcessorLeader.start();
            }
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
        Conf walProcessorConf = rowLogConf.getChild("walProcessor");
        boolean walProcEnabled = walProcessorConf.getAttributeAsBoolean("enabled", true);
        if (walProcEnabled) {
            List<String> walProcessorNodes = Collections.EMPTY_LIST;
            Conf nodesConf = walProcessorConf.getChild("nodes");
            if (nodesConf != null) {
                String nodes = nodesConf.getValue("");
                if (!nodes.isEmpty()) {
                    walProcessorNodes = Arrays.asList(nodes.split(","));
                }
            }
            RowLogProcessorSettings settings = createProcessorSettings(walProcessorConf);
            RowLogProcessor processor = new WalProcessor(writeAheadLog, confMgr, hbaseConf, settings);
            writeAheadLogProcessorLeader = new RowLogProcessorElection(zk, processor, lilyInfo);
            // The WAL processor should only be started once the LinkIndexUpdater listener is available
            walProcessorStartupThread = new Thread(new DelayedWALProcessorStartup());
            if (walProcessorNodes.isEmpty() || walProcessorNodes.contains(hostName)) {
                walProcessorStartupThread.start();
            }
        } else {
            log.info("Not participating in WAL processor election.");
        } 
    }

    private RowLogProcessorSettings createProcessorSettings(Conf conf) {
        RowLogProcessorSettings settings = new RowLogProcessorSettings();

        settings.setMsgTimestampMargin(
                conf.getChild("messageTimestampMargin")
                        .getValueAsInteger(RowLogProcessor.DEFAULT_MSG_TIMESTAMP_MARGIN));

        settings.setScanThreadCount(
                conf.getChild("scanThreadCount")
                        .getValueAsInteger(settings.getScanThreadCount()));

        settings.setScanBatchSize(
                conf.getChild("scanBatchSize")
                        .getValueAsInteger(settings.getScanBatchSize()));

        settings.setMessagesWorkQueueSize(
                conf.getChild("messagesWorkQueueSize")
                        .getValueAsInteger(settings.getMessagesWorkQueueSize()));

        return settings;
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
        @Override
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

            while (RowLogMessageListenerMapping.INSTANCE.get("LinkIndexUpdater") == null) {
                if (System.currentTimeMillis() > waitUntil) {
                    log.error("IMPORTANT: MQFeeder did not appear in RowLogMessageListenerMapping after" +
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
