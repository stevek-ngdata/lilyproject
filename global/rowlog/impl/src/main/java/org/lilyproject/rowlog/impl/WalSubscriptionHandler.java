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
    
    @Override
    public void start() {
        listenerRegistered(WalListener.ID);
    }

    @Override
    public void shutdown() {
        stop = true;
        listenerUnregistered(WalListener.ID);
    }
}
