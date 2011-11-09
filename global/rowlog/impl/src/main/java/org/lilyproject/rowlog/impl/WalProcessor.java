package org.lilyproject.rowlog.impl;

import java.util.List;

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
    public WalProcessor(WalRowLog rowLog, RowLogConfigurationManager rowLogConfigurationManager) {
        super(rowLog, rowLogConfigurationManager);
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
