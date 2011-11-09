package org.lilyproject.rowlog.impl;

import org.lilyproject.rowlog.api.RowLog;
import org.lilyproject.rowlog.api.RowLogConfigurationManager;

/**
 * The WalSubscriptionHandler will mark the {@link WalListener} as registered and unregistered itself.
 * It does not depend on the RowLogConfigurationManager to have it registered and unregistered.
 * 
 * <p>This WalSubscriptionHandler should be used together with the {@link WalProcessor}, {@link WalListener} and {@link WalRowLog} 
 *
 */
public class WalSubscriptionHandler extends LocalListenersSubscriptionHandler {
    public WalSubscriptionHandler(String subscriptionId, MessagesWorkQueue messagesWorkQueue, RowLog rowLog,
            RowLogConfigurationManager rowLogConfigurationManager) {
        super(subscriptionId, messagesWorkQueue, rowLog, rowLogConfigurationManager);
    } 
    
    public void start() {
        listenerRegistered(WalListener.ID);
    }

    public void shutdown() {
        stop = true;
        listenerUnregistered(WalListener.ID);
    }
}
