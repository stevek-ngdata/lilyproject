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

import com.google.common.base.Function;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.lilyproject.rowlog.api.RowLogConfigurationManager;
import org.lilyproject.util.io.Closer;

public class RowLogProcessorNotifier {
    private RowLogConfigurationManager rowLogConfMgr;
    private Log log = LogFactory.getLog(getClass());
    private LoadingCache<SubscriptionKey, Triggerable> triggerables;

    public RowLogProcessorNotifier(RowLogConfigurationManager rowLogConfigurationManager, final long delay) {
        this.rowLogConfMgr = rowLogConfigurationManager;

        this.triggerables = CacheBuilder.newBuilder().build(CacheLoader.from(new Function<SubscriptionKey, Triggerable>() {
            @Override
            public Triggerable apply(final SubscriptionKey subscriptionKey) {
                return new BufferedTriggerable(new Triggerable() {
                    @Override
                    public void trigger() throws InterruptedException {
                        sendNotification(subscriptionKey);
                    }
                }, delay);
            }
        }));
    }

    protected void notifyProcessor(String rowLogId, String subscriptionId) throws InterruptedException {
        triggerables.getUnchecked(new SubscriptionKey(rowLogId, subscriptionId)).trigger();
    }

    private void sendNotification(SubscriptionKey subscriptionKey) throws InterruptedException {
        try {
            rowLogConfMgr.notifyProcessor(subscriptionKey.getRowLogId(), subscriptionKey.getSubscriptionId());
        } catch (KeeperException e) {
            log.debug("Exception while notifying processor for rowlog subscription " + subscriptionKey, e);
        }
    }

    public void close() throws InterruptedException {
        for (Triggerable triggerable : triggerables.asMap().values()) {
            Closer.close(triggerable);
        }
    }
}
