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
package org.lilyproject.repotestfw;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Put;
import org.lilyproject.rowlock.RowLock;
import org.lilyproject.rowlog.api.RowLog;
import org.lilyproject.rowlog.api.RowLogConfig;
import org.lilyproject.rowlog.api.RowLogException;
import org.lilyproject.rowlog.api.RowLogMessage;
import org.lilyproject.rowlog.api.RowLogShard;
import org.lilyproject.rowlog.api.RowLogShardList;
import org.lilyproject.rowlog.api.RowLogSubscription;

/**
 * A RowLog implementation that delegates to another RowLog and allows to manually trigger the processing
 * of the messages put since the last manual trigger. For use in test cases.
 */
public class ManualProcessRowLog implements RowLog {
    private RowLog delegate;
    private List<RowLogMessage> unprocessedMessages = new ArrayList<RowLogMessage>();

    public ManualProcessRowLog(RowLog delegate) {
        this.delegate = delegate;
    }

    @Override
    public String getId() {
        return delegate.getId();
    }

    @Override
    public byte[] getPayload(RowLogMessage message) throws RowLogException {
        return delegate.getPayload(message);
    }

    @Override
    public RowLogMessage putMessage(byte[] rowKey, byte[] data, byte[] payload, Put put) throws RowLogException,
            InterruptedException {
        List<String> subscriptionIds = getSubscriptionIds();
        return putMessage(rowKey, data, payload, put, subscriptionIds);
    }

    @Override
    public RowLogMessage putMessage(byte[] rowKey, byte[] data, byte[] payload, Put put,
                                    List<String> subscriptionIds) throws RowLogException, InterruptedException {

        RowLogMessage msg = delegate.putMessage(rowKey, data, payload, put, subscriptionIds);
        unprocessedMessages.add(msg);

        return msg;
    }

    public void processMessages() throws RowLogException, InterruptedException {
        while (!unprocessedMessages.isEmpty()) {
            RowLogMessage msg = unprocessedMessages.remove(0);
            processMessage(msg, null);
        }
    }

    @Override
    public boolean processMessage(RowLogMessage message, RowLock lock) throws RowLogException, InterruptedException {
        return delegate.processMessage(message, lock);
    }

    @Override
    public boolean messageDone(RowLogMessage message, String subscriptionId) throws RowLogException, InterruptedException {
        return delegate.messageDone(message, subscriptionId);
    }

    @Override
    public boolean isMessageDone(RowLogMessage message, String subscriptionId) throws RowLogException {
        return delegate.isMessageDone(message, subscriptionId);
    }

    @Override
    public List<RowLogMessage> getMessages(byte[] rowKey, String... subscriptionId) throws RowLogException {
        return delegate.getMessages(rowKey, subscriptionId);
    }

    @Override
    public List<RowLogSubscription> getSubscriptions() {
        return delegate.getSubscriptions();
    }

    @Override
    public List<String> getSubscriptionIds() {
        return delegate.getSubscriptionIds();
    }

    @Override
    public List<RowLogShard> getShards() {
        return delegate.getShards();
    }

    @Override
    public RowLogShardList getShardList() {
        return delegate.getShardList();
    }

    @Override
    public boolean isMessageAvailable(RowLogMessage message, String subscriptionId) throws RowLogException {
        return delegate.isMessageAvailable(message, subscriptionId);
    }

    @Override
    public RowLogConfig getConfig() {
        return delegate.getConfig();
    }

}
