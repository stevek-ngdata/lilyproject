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

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.lilyproject.rowlog.api.*;
import org.lilyproject.rowlog.api.RowLogSubscription.Type;

/**
 * The WalProcessor is an optimized version of the RowLogProcessor for the WAL use case.
 * 
 * <p>This WalProcessor should be used together with the {@link WalRowLog}, {@link WalListener} and {@link WalSubscriptionHandler} 
 *
 * <p>The WalProcessor is optimized to only look on the rowlog shard for messages of the meta subscription id 'WAL'.
 * <br>Instead of starting a SubscriptionThread for each subscription which in their turn would each scan themselves for their messages
 * on the wal, a SubscriptionThread is started for the meta 'WAL' subscription. This thread will request the {@link WalListener} to
 * process any found messages. This will call the WalRowLog.processMessage() call which in its turn will request each
 * 'normal' subscription to process the message.
 * <br>The meta message on the rowlog shard will only be removed once all subscriptions have processed the message.  
 */
public class WalProcessor extends RowLogProcessorImpl {
    public WalProcessor(WalRowLog rowLog, RowLogConfigurationManager rowLogConfigurationManager,
            Configuration hbaseConf) {
        super(rowLog, rowLogConfigurationManager, hbaseConf);
    }

    public WalProcessor(WalRowLog rowLog, RowLogConfigurationManager rowLogConfigurationManager,
            Configuration hbaseConf, RowLogProcessorSettings settings) {
        super(rowLog, rowLogConfigurationManager, hbaseConf, settings);
    }

    /**
     * Instead of creating SubscriptionThread for each subscription, only one SubscriptionThread is created for the
     * meta 'WAL' subscription.
     * Also no subscription observers should be registered with the RowLogConfigurationManager.
     */
    @Override
    protected void initializeSubscriptions() {
        RowLogSubscription walSubscription = new RowLogSubscription(rowLog.getId(), WalRowLog.WAL_SUBSCRIPTIONID, Type.WAL, 1);
        SubscriptionThread subscriptionThread = startSubscriptionThread(walSubscription);
        subscriptionThreads.put(walSubscription.getId(), subscriptionThread);
    }
    
    @Override
    public synchronized void subscriptionsChanged(List<RowLogSubscription> newSubscriptions) {
    }
    
    /**
     * There are no subscription observers to be unregistered from the RowLogConfigurationManager.
     */
    @Override
    protected void stopSubscriptions() {
    }
    
    @Override
    protected boolean isMessageDone(RowLogMessage message, String subscriptionId) throws RowLogException {
        return false;
    }
}
