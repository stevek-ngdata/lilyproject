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
package org.lilyproject.rowlog.impl;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.ComparisonChain;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilyproject.rowlog.api.ProcessorNotifyObserver;
import org.lilyproject.rowlog.api.RowLog;
import org.lilyproject.rowlog.api.RowLogConfig;
import org.lilyproject.rowlog.api.RowLogConfigurationManager;
import org.lilyproject.rowlog.api.RowLogException;
import org.lilyproject.rowlog.api.RowLogMessage;
import org.lilyproject.rowlog.api.RowLogObserver;
import org.lilyproject.rowlog.api.RowLogProcessor;
import org.lilyproject.rowlog.api.RowLogShard;
import org.lilyproject.rowlog.api.RowLogSubscription;
import org.lilyproject.rowlog.api.SubscriptionsObserver;
import org.lilyproject.util.Logs;
import org.lilyproject.util.concurrent.CustomThreadFactory;
import org.lilyproject.util.hbase.HBaseAdminFactory;
import org.lilyproject.util.io.Closer;

public class RowLogProcessorImpl implements RowLogProcessor, RowLogObserver, SubscriptionsObserver, ProcessorNotifyObserver {
    private volatile boolean stop = true;
    protected final RowLog rowLog;
    protected final Map<String, SubscriptionThread> subscriptionThreads = Collections.synchronizedMap(new HashMap<String, SubscriptionThread>());
    private RowLogConfigurationManager rowLogConfigurationManager;
    private Log log = LogFactory.getLog(RowLogProcessorImpl.class);
    private RowLogConfig rowLogConfig;
    private ThreadPoolExecutor globalQScanExecutor;
    private ScheduledExecutorService scheduledServices;
    private Configuration hbaseConf;
    private RowLogProcessorSettings settings;
    private Triggerable bufferedProcessorNotifier;

    private final AtomicBoolean initialRowLogConfigLoaded = new AtomicBoolean(false);
    
    public RowLogProcessorImpl(RowLog rowLog, RowLogConfigurationManager rowLogConfigurationManager,
            final Configuration hbaseConf) {
        this(rowLog, rowLogConfigurationManager, hbaseConf, new RowLogProcessorSettings());
    }

    public RowLogProcessorImpl(RowLog rowLog, RowLogConfigurationManager rowLogConfigurationManager,
            final Configuration hbaseConf, RowLogProcessorSettings settings) {
        this.rowLog = rowLog;
        this.rowLogConfigurationManager = rowLogConfigurationManager;
        this.hbaseConf = hbaseConf;
        this.settings = settings;
    }

    @Override
    public RowLog getRowLog() {
        return rowLog;
    }
    
    @Override
    public synchronized void start() throws InterruptedException, IOException {
        if (stop) {
            stop = false;

            // Init the executor service for the scan jobs
            int regionServerCnt = HBaseAdminFactory.get(hbaseConf).getClusterStatus().getServers();
            int threadCnt = getScanThreadCount(regionServerCnt);
            log.info("Maximum global queue scan threads set to " + threadCnt);
            this.globalQScanExecutor = new ThreadPoolExecutor(threadCnt, threadCnt,
                    30, TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>(),
                    new CustomThreadFactory("rowlog-scan", new ThreadGroup("RowLogScan")));

            // Start the service that will monitor the number of region servers and adjust number
            // of scan threads accordingly
            if (settings.getScanThreadCount() < 1) {
                this.scheduledServices = Executors.newScheduledThreadPool(1);
                this.scheduledServices.scheduleWithFixedDelay(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            int regionServerCnt = HBaseAdminFactory.get(hbaseConf).getClusterStatus().getServers();
                            int threadCnt = getScanThreadCount(regionServerCnt);
                            if (globalQScanExecutor.getMaximumPoolSize() != threadCnt) {
                                log.warn("Changing number of global queue scan threads to " + threadCnt
                                        + " (" + rowLog.getId() + ")");
                                globalQScanExecutor.setMaximumPoolSize(threadCnt);
                                globalQScanExecutor.setCorePoolSize(threadCnt);
                            }
                        } catch (Throwable t) {
                            log.error("Error determining or adjusting global queue scan thread pool size", t);
                        }
                    }
                }, 5, 10, TimeUnit.MINUTES);
            }

            initializeRowLogConfig();
            initializeSubscriptions();
            initializeNotifyObserver();

            bufferedProcessorNotifier = new BufferedTriggerable(new Triggerable() {
                @Override
                public void trigger() {
                    notifyProcessorNonDelayed();
                }
            }, rowLogConfig.getNotifyDelay());
        }
    }

    @Override
    public synchronized void stop() {
        stop = true;
        Closer.close(bufferedProcessorNotifier);
        if (scheduledServices != null)
            scheduledServices.shutdownNow();
        stopRowLogConfig();
        stopSubscriptions();
        stopNotifyObserver();
        stopSubscriptionThreads();
        if (globalQScanExecutor != null)
            globalQScanExecutor.shutdownNow();
    }

    private int getScanThreadCount(int regionServerCnt) {
        if (settings.getScanThreadCount() > 0) {
            return settings.getScanThreadCount();
        }

        // Use up to 2 requests per region server (on average).
        // Lily-specific note: keep in mind that there are 2 rowlog processors (one for MQ, one for the WAL), so
        // there could be up to twice this number of threads in a server! (though not necessarily, both may be
        // running on different servers)
        int threads = regionServerCnt * 2;

        // at least one, at most 30
        threads = threads > 30 ? 30 : threads < 1 ? 1 : threads;

        // don't need more threads than there are shards
        int shardCount = rowLog.getShards().size();
        threads = threads > shardCount ? shardCount : threads;

        return threads;
    }

    private void initializeNotifyObserver() {
        rowLogConfigurationManager.addProcessorNotifyObserver(rowLog.getId(), this);
    }

    protected void initializeSubscriptions() {
        rowLogConfigurationManager.addSubscriptionsObserver(rowLog.getId(), this);
    }

    private void initializeRowLogConfig() throws InterruptedException {
        rowLogConfigurationManager.addRowLogObserver(rowLog.getId(), this);
        synchronized (initialRowLogConfigLoaded) {
            while(!initialRowLogConfigLoaded.get()) {
                initialRowLogConfigLoaded.wait();
            }
        }
    }

    //  synchronized because we do not want to run this concurrently with the start/stop methods
    @Override
    public synchronized void subscriptionsChanged(List<RowLogSubscription> newSubscriptions) {
        synchronized (subscriptionThreads) {
            if (!stop) {
                List<String> newSubscriptionIds = new ArrayList<String>();
                for (RowLogSubscription newSubscription : newSubscriptions) {
                    String subscriptionId = newSubscription.getId();
                    newSubscriptionIds.add(subscriptionId);
                    SubscriptionThread existingSubscriptionThread = subscriptionThreads.get(subscriptionId);
                    if (existingSubscriptionThread == null) {
                        if (log.isDebugEnabled()) {
                            log.debug("Starting subscription thread for new subscription " + newSubscription.getId());
                        }
                        SubscriptionThread subscriptionThread = startSubscriptionThread(newSubscription);
                        subscriptionThreads.put(subscriptionId, subscriptionThread);
                    } else if (!existingSubscriptionThread.getSubscription().equals(newSubscription)) {
                        if (log.isDebugEnabled()) {
                            log.debug("Stopping existing and starting new subscription thread for subscription "
                                    + newSubscription.getId());
                        }
                        stopSubscriptionThread(subscriptionId);
                        SubscriptionThread subscriptionThread = startSubscriptionThread(newSubscription);
                        subscriptionThreads.put(subscriptionId, subscriptionThread);
                    }
                }
                Iterator<String> iterator = subscriptionThreads.keySet().iterator();
                while (iterator.hasNext()) {
                    String subscriptionId = iterator.next();
                    if (!newSubscriptionIds.contains(subscriptionId)) {
                        if (log.isDebugEnabled()) {
                            log.debug("Stopping subscription thread for subscription " + subscriptionId);
                        }
                        stopSubscriptionThread(subscriptionId);
                        iterator.remove();
                    }
                }
            }
        }
    }

    protected SubscriptionThread startSubscriptionThread(RowLogSubscription subscription) {
        SubscriptionThread subscriptionThread = new SubscriptionThread(subscription);
        subscriptionThread.start();
        return subscriptionThread;
    }
    
    private void stopSubscriptionThread(String subscriptionId) {
        SubscriptionThread subscriptionThread = subscriptionThreads.get(subscriptionId);
        subscriptionThread.shutdown();
        try {
            Logs.logThreadJoin(subscriptionThread);
            subscriptionThread.join();
        } catch (InterruptedException e) {
        }
    }

    private void stopSubscriptionThreads() {
        Collection<SubscriptionThread> threadsToStop;
        synchronized (subscriptionThreads) {
            threadsToStop = new ArrayList<SubscriptionThread>(subscriptionThreads.values());
            subscriptionThreads.clear();
        }
        for (SubscriptionThread thread : threadsToStop) {
            if (thread != null) {
                thread.shutdown();
            }
        }
        for (Thread thread : threadsToStop) {
            if (thread != null) {
                try {
                    if (thread.isAlive()) {
                        Logs.logThreadJoin(thread);
                        thread.join();
                    }
                } catch (InterruptedException e) {
                }
            }
        }
    }

    private void stopNotifyObserver() {
        rowLogConfigurationManager.removeProcessorNotifyObserver(rowLog.getId());
    }

    protected void stopSubscriptions() {
        rowLogConfigurationManager.removeSubscriptionsObserver(rowLog.getId(), this);
    }

    private void stopRowLogConfig() {
        rowLogConfigurationManager.removeRowLogObserver(rowLog.getId(), this);
        synchronized (initialRowLogConfigLoaded) {
            initialRowLogConfigLoaded.set(false);
        }
    }

    @Override
    public boolean isRunning(String subscriptionId) {
        return subscriptionThreads.get(subscriptionId) != null;
    }
            
    /**
     * Called when a message has been posted on the rowlog that needs to be processed by this RowLogProcessor.
     * The notification will only be taken into account when a delay has passed since the previous notification.
     */
    @Override
    public void notifyProcessor() {
        try {
            bufferedProcessorNotifier.trigger();
        } catch (InterruptedException e) {
            // we are asked to stop
            Thread.currentThread().interrupt();
        }
    }

    private synchronized void notifyProcessorNonDelayed() {
        Collection<SubscriptionThread> threadsToWakeup;
        synchronized (subscriptionThreads) {
            threadsToWakeup = new HashSet<SubscriptionThread>(subscriptionThreads.values());
        }
        for (SubscriptionThread subscriptionThread : threadsToWakeup) {
            subscriptionThread.wakeup();
        }
    }

    protected class SubscriptionThread extends Thread {
        private long lastWakeup;
        private ProcessorMetrics metrics;
        private volatile boolean stopRequested = false; // do not rely only on Thread.interrupt since some libraries eat interruptions
        private final MessagesWorkQueue messagesWorkQueue;
        private SubscriptionHandler subscriptionHandler;
        private final RowLogSubscription subscription;
        private boolean firstRun = true;
        private final int scanBatchPerShard;

        public SubscriptionThread(RowLogSubscription subscription) {
            super(new ThreadGroup("RowLogProcessor"), "Row log SubscriptionThread for " + subscription.getId());
            this.subscription = subscription;
            this.metrics = new ProcessorMetrics(rowLog.getId()+"_"+subscription.getId());

            int scanBatchPerShard = settings.getScanBatchSize() / rowLog.getShards().size();
            if (scanBatchPerShard < 1) {
                scanBatchPerShard = 1;
            }
            this.scanBatchPerShard = scanBatchPerShard;
            log.info("RowLog scan batch size (on each shard/split): " + scanBatchPerShard);

            messagesWorkQueue = new MessagesWorkQueue(settings.getMessagesWorkQueueSize());
            log.info("RowLog messages work queue size: " + settings.getMessagesWorkQueueSize());

            switch (subscription.getType()) {
                case VM:
                    subscriptionHandler =new LocalListenersSubscriptionHandler(subscription.getId(), messagesWorkQueue,
                            rowLog, rowLogConfigurationManager);
                    break;

                case Netty:
                    subscriptionHandler = new RemoteListenersSubscriptionHandler(subscription.getId(),
                            messagesWorkQueue, rowLog, rowLogConfigurationManager);
                    break;

                case WAL:
                    subscriptionHandler = new WalSubscriptionHandler(subscription.getId(), messagesWorkQueue, rowLog,
                            rowLogConfigurationManager);
                    break;

                default:
                    break;
            }
        }
        
        public RowLogSubscription getSubscription() {
            return subscription;
        }
        
        public synchronized void wakeup() {
            metrics.wakeups.inc();
            lastWakeup = System.currentTimeMillis();
            this.notify();
        }
        
        @Override
        public synchronized void start() {
            stopRequested = false;
            subscriptionHandler.start();
            super.start();
        }
        
        public void shutdown() {
            stopRequested = true;
            subscriptionHandler.shutdown();
            interrupt();
        }
                
        @Override
        public void run() {
            try {
                Long minimalTimestamp = null;
                // scanFirstMessageOnly: for the WAL use case, where there is a minimal process delay and messages
                // are normally processed directly and only in case of recovery by the RowLogProcessor, it does not
                // make sense to scan e.g. 200 messages just to see their minimalProcessDelay has not yet passed.
                // Therefore, this boolean indicates that just one message should be scanned. Note that this assumes
                // that the minimalProcessDelay parameter will only be used for WAL-type uses.
                boolean scanFirstMessageOnly = false;
                while (!isInterrupted() && !stopRequested) {
                    final String subscriptionId = subscription.getId();
                    try {
                        metrics.scans.inc();

                        long tsBeforeGetMessages = System.currentTimeMillis();

                        // Scan in parallel over the different regions
                        // Ideally, we would figure out on what servers what regions are deployed and then do the
                        // requests such that we touch the maximum number of different servers. For now, we keep
                        // it simple and assume the requests will be distributed enough by chance.
                        final int batchSize = scanFirstMessageOnly ? 1 : scanBatchPerShard;
                        List<RowLogMessage> messages = new ArrayList<RowLogMessage>();
                        int maxMessagesFromOneShard = 0;
                        List<Future<List<RowLogMessage>>> scanFutures = new ArrayList<Future<List<RowLogMessage>>>();
                        final Long currentMinimalTimestamp = minimalTimestamp;
                        for (final RowLogShard shard : rowLog.getShards()) {
                            try {
                                scanFutures.add(globalQScanExecutor.submit(new Callable<List<RowLogMessage>>() {
                                    @Override
                                    public List<RowLogMessage> call() throws Exception {
                                        return shard.next(subscriptionId, currentMinimalTimestamp, batchSize);
                                    }
                                }));
                            } catch (RejectedExecutionException e) {
                                // The only reason this could occur is because we're shutting down, since there
                                // is no limit on the size of the queue
                                log.info("Got RejectedExecutionException", e);
                                break;
                            }
                        }

                        for (Future<List<RowLogMessage>> future : scanFutures) {
                            List<RowLogMessage> shardMessages = future.get();
                            messages.addAll(shardMessages);
                            if (shardMessages.size() > maxMessagesFromOneShard) {
                                 maxMessagesFromOneShard = shardMessages.size();
                            }
                        }

                        // Sort the messages from the different shards by timestamp
                        // TODO this could be improved, knowing that the lists from shard.next() are already sorted
                        Collections.sort(messages, new Comparator<RowLogMessage>() {
                            @Override
                            public int compare(RowLogMessage o1, RowLogMessage o2) {
                                return ComparisonChain.start()
                                        .compare(o1.getTimestamp(), o2.getTimestamp())
                                        .compare(o1.getRowKey(), o2.getRowKey(), Bytes.BYTES_RAWCOMPARATOR)
                                        .compare(o1.getSeqNr(), o2.getSeqNr())
                                        .result();
                            }
                        });

                        metrics.scanDuration.inc(System.currentTimeMillis() - tsBeforeGetMessages);

                        if (log.isDebugEnabled()) {
                            log.debug(String.format("[%1$s - %2$s] Scanned with minimal timestamp of %3$s, got %4$s messages.",
                                    rowLog.getId(), subscriptionId, minimalTimestamp, messages.size()));
                        }

                        if (stopRequested) {
                            // Check if not stopped because HBase hides thread interruptions
                            return;
                        }

                        if (firstRun) {
                            firstRun = false;
                            if (messages.isEmpty()) {
                                // If on startup of this processor, we have no messages, we initialize the
                                // minimalTimestamp manually so that we would not always scan from the start
                                // of the table.
                                minimalTimestamp = tsBeforeGetMessages - settings.getMsgTimestampMargin();

                                if (log.isDebugEnabled()) {
                                    log.debug(String.format("[%1$s - %2$s] On initial scan, got no messages from HBase, " +
                                            "setting minimal timestamp to %3$s", rowLog.getId(), subscriptionId, minimalTimestamp));
                                }
                            }
                        }

                        metrics.messagesPerScan.inc(messages != null ? messages.size() : 0);
						if (messages != null && !messages.isEmpty()) {
						    minimalTimestamp = messages.get(0).getTimestamp() - settings.getMsgTimestampMargin();
                            for (RowLogMessage message : messages) {
                                if (stopRequested)
                                    return;

                                if (checkMinimalProcessDelay(message)) {
                                    scanFirstMessageOnly = true;
                                	break; // Rescan the messages since they might have been processed in the meanwhile
                                } else {
                                    scanFirstMessageOnly = false;
                                }

                                messagesWorkQueue.offer(message);
                            }
                        }

                        // If we had a full batch of messages, we will immediately request the next batch, without
                        // sleeping. If we got less, we sleep unless we received a wake-up signal after we started
                        // scanning for messages.
                        // Also: the minimalProcessDelay setting is not taken into account: as it currently is,
                        // this is only relevant for the WAL, which does not make use of the wake-up signal.
                        if (maxMessagesFromOneShard < scanBatchPerShard && lastWakeup < tsBeforeGetMessages) {
                            synchronized (this) {
                                // The timeout covers two cases:
                                //   (1) a safety fallback, in case a wake-up got lost or so
                                //   (2) the WAL, which does not make use of wake-ups
                                wait(rowLogConfig.getWakeupTimeout());
                            }
                        }

                        // It makes no sense to scan for new messages if the work-queue is still full. The messages
                        // which would be retrieved by the scan would be messages which are still in the work-queue
                        // anyway (minus those meanwhile consumed by the listeners). When no listeners are active,
                        // this can even lead to endless scan-loops (if work-queue-size >= batch-size and # messages
                        // in queue > work-queue-size). So we'll wait till the work-queue is almost empty.
                        messagesWorkQueue.waitOnRefillThreshold();

                    } catch (InterruptedException e) {
                        return;
                    } catch (Throwable t) {
                        if (Thread.currentThread().isInterrupted())
                            return;
                        log.error("Error in subscription thread for " + subscriptionId, t);
                    }
                }
            } finally {
                metrics.shutdown();
            }
        }

        /**
         * Check if the message is old enough to be processed. If not, wait
         * until it is. Any other messages that might be in the queue to be
         * processed should (will) be younger and don't have to be processed yet
         * either.
         * 
         * @return true if we waited
         * @throws InterruptedException
         */
        private boolean checkMinimalProcessDelay(RowLogMessage message) throws InterruptedException {
            long now = System.currentTimeMillis();
            long messageTimestamp = message.getTimestamp();
            long waitAtLeastUntil = messageTimestamp + rowLogConfig.getMinimalProcessDelay();
            if (now < waitAtLeastUntil) {
                synchronized (this) {
                    wait(waitAtLeastUntil - now);
                }
                return true;
            }
        	return false;
        }
    }
    
    @Override
    public void rowLogConfigChanged(RowLogConfig rowLogConfig) {
        this.rowLogConfig = rowLogConfig;
        if (!initialRowLogConfigLoaded.get()) {
            synchronized(initialRowLogConfigLoaded) {
                initialRowLogConfigLoaded.set(true);
                initialRowLogConfigLoaded.notifyAll();
            }
        }
    }
    
    protected boolean isMessageDone(RowLogMessage message, String subscriptionId) throws RowLogException {
        return rowLog.isMessageDone(message, subscriptionId);
    }
}
