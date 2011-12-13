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
package org.lilyproject.indexer.batchbuild;

import org.apache.hadoop.hbase.client.Put;
import org.lilyproject.rowlock.RowLock;
import org.lilyproject.rowlog.api.*;

import java.util.List;

public class DummyRowLog implements RowLog {
    private final String failMessage;

    public DummyRowLog(String failMessage) {
        this.failMessage = failMessage;
    }

    @Override
    public String getId() {
        throw new RuntimeException(failMessage);
    }

    @Override
    public byte[] getPayload(RowLogMessage message) throws RowLogException {
        throw new RuntimeException(failMessage);
    }

    @Override
    public RowLogMessage putMessage(byte[] rowKey, byte[] data, byte[] payload, Put put) throws RowLogException {
        throw new RuntimeException(failMessage);
    }

    @Override
    public boolean processMessage(RowLogMessage message, RowLock rowLock) throws RowLogException {
        throw new RuntimeException(failMessage);
    }

    @Override
    public boolean messageDone(RowLogMessage message, String subscriptionId) throws RowLogException {
        throw new RuntimeException(failMessage);
    }

    @Override
    public boolean isMessageDone(RowLogMessage message, String subscriptionId) throws RowLogException {
        throw new RuntimeException(failMessage);
    }

    @Override
    public List<RowLogMessage> getMessages(byte[] rowKey, String... subscriptionId) throws RowLogException {
        throw new RuntimeException(failMessage);
    }

    @Override
    public List<RowLogSubscription> getSubscriptions() {
        throw new RuntimeException(failMessage);
    }

    @Override
    public List<RowLogShard> getShards() {
        throw new RuntimeException(failMessage);
    }

    @Override
    public RowLogShardList getShardList() {
        throw new RuntimeException(failMessage);
    }

    @Override
    public boolean isMessageAvailable(RowLogMessage message, String subscriptionId) throws RowLogException {
        throw new RuntimeException(failMessage);
    }
}
