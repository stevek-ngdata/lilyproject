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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.rowlog.api.*;
import org.lilyproject.util.Logs;

public abstract class AbstractSubscriptionHandler implements SubscriptionHandler {
    protected final RowLog rowLog;
    protected final String rowLogId;
    protected final String subscriptionId;
    protected final MessagesWorkQueue messagesWorkQueue;
    private Log log = LogFactory.getLog(AbstractSubscriptionHandler.class);
	private SubscriptionHandlerMetrics metrics;
    
    public AbstractSubscriptionHandler(String subscriptionId, MessagesWorkQueue messagesWorkQueue, RowLog rowLog) {
        this.rowLog = rowLog;
        this.rowLogId = rowLog.getId();
        this.subscriptionId = subscriptionId;
        this.messagesWorkQueue = messagesWorkQueue;
        this.metrics = new SubscriptionHandlerMetrics(rowLog.getId() + "_" + subscriptionId);
    }

    /**
     * Called once on the setup of a worker, thus the WorkerDelegate is suited for keeping any
     * per-worker state. Since a worker only processes one message at a time, this means per-message
     * (or from an IO point of view, per-request) state.
     */
    protected abstract WorkerDelegate createWorkerDelegate(String context);

    protected static interface WorkerDelegate {
        boolean processMessage(RowLogMessage message) throws RowLogException, InterruptedException;

        /**
         * Called when the worker is stopped.
         */
        void close();
    }
    
    protected class Worker implements Runnable {
        private WorkerDelegate delegate;
        private final String subscriptionId;
        private final String listener;
        private Thread thread;
        private volatile boolean stop; // do not rely only on Thread.interrupt since some libraries eat interruptions

        public Worker(String subscriptionId, String listener) {
            this.subscriptionId = subscriptionId;
            this.listener = listener;
            this.delegate = createWorkerDelegate(listener);
        }

        public void start() {
            thread = new Thread(new ThreadGroup("RowLogHandlers"), this,
                    "Handler: subscription " + subscriptionId + ", listener " + listener);
            thread.start();
        }

        public void stop() throws InterruptedException {
            stop = true;
            thread.interrupt();
            Logs.logThreadJoin(thread);
            thread.join();
            delegate.close();
        }

        @Override
        public void run() {
            while(!stop && !Thread.interrupted()) {
                RowLogMessage message;
                try {
                	metrics.queueSize.set(messagesWorkQueue.size());
                    message = messagesWorkQueue.take();
                    if (message != null) {
                        try {
                            // We removed taking the lock here
                            // A rowlock should be taken by the WalListener or the HBaseRepository methods so that they don't interfere
                            // Taking a lock in the execution state is not needed since :
                            //   1) there is currently only one rowlog processor
                            //   2) the messagesWorkQueue take() and done() calls make sure messages for the same row are not given to multiple listeners at the same time
                            if (rowLog.isMessageAvailable(message, subscriptionId)) {
                                boolean processMessageResult = false;
                                try {
                                    processMessageResult = delegate.processMessage(message);
                                } catch (RemoteListenerIOException e) {
                                    metrics.ioExceptionRate.inc();
                                    // Logging to info to avoid log-flooding in case of network connection problems
                                    if (log.isInfoEnabled()) {
                                        log.info(String.format("[%1$s - %2$s] RemoteListenerIOException occurred while processing message %3$s", rowLogId, subscriptionId, message), e);
                                    }
                                }
                                if (processMessageResult) {
                                	metrics.successRate.inc();
                                    rowLog.messageDone(message, subscriptionId);
                                } else {
                                	metrics.failureRate.inc();
                                }
                            } else {
                                if (log.isDebugEnabled()) {
                                    log.debug(String.format("[%1$s - %2$s] Message is not available: %3$s", rowLogId, subscriptionId, message));
                                }
                            }
                        } catch (InterruptedException e) {
                            break;
                        } catch (Throwable e) {
                            log.warn(String.format("[%1$s - %2$s] RowLogException occurred while processing message %3$s", rowLogId, subscriptionId, message), e);
                        } finally {
                            messagesWorkQueue.done(message);
                        }
                    }
                } catch (InterruptedException e) {
                    break;
                } catch (Throwable t) {
                    log.error(String.format("[%1$s - %2$s] Error in subscription handler thread", rowLogId, subscriptionId), t);
                }
            }
        }
    }
}
