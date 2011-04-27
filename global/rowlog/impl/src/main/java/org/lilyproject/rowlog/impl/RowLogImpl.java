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
import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilyproject.rowlock.RowLock;
import org.lilyproject.rowlock.RowLocker;
import org.lilyproject.rowlog.api.*;
import org.lilyproject.util.io.Closer;

/**
 * See {@link RowLog}
 */
public class RowLogImpl implements RowLog, SubscriptionsObserver, RowLogObserver {

    private static final byte PL_BYTE = (byte)1;
    private static final byte ES_BYTE = (byte)2;
    private static final byte[] SEQ_NR = Bytes.toBytes("SEQNR");
    protected RowLogShard shard; // TODO: We only work with one shard for now
    private final HTableInterface rowTable;
    private final byte[] rowLogColumnFamily;
    private RowLogConfig rowLogConfig;
    
    private final Map<String, RowLogSubscription> subscriptions = Collections.synchronizedMap(new HashMap<String, RowLogSubscription>());
    protected final String id;
    private RowLogProcessorNotifier processorNotifier = null;
    private Log log = LogFactory.getLog(getClass());
    private RowLogConfigurationManager rowLogConfigurationManager;

    private final AtomicBoolean initialSubscriptionsLoaded = new AtomicBoolean(false);
    private final AtomicBoolean initialRowLogConfigLoaded = new AtomicBoolean(false);
    private final RowLocker rowLocker;
    private byte[] payloadPrefix;
    private byte[] executionStatePrefix;
    private byte[] seqNrQualifier;

    /**
     * The RowLog should be instantiated with information about the table that contains the rows the messages are 
     * related to, and the column families it can use within this table to put the payload and execution state of the
     * messages on.
     * @param rowTable the HBase table containing the rows to which the messages are related
     * @param rowLogColumnFamily the column family in which the payload and execution state of the messages can be stored
     * @param rowLogId a byte uniquely identifying the rowLog amongst all rowLogs in the system
     * @param rowLocker if given, the rowlog will take locks at row level; if null, the locks will be taken at executionstate level
     * @throws RowLogException
     */
    public RowLogImpl(String id, HTableInterface rowTable, byte[] rowLogColumnFamily, byte rowLogId,
            RowLogConfigurationManager rowLogConfigurationManager, RowLocker rowLocker) throws InterruptedException {
        this.id = id;
        this.rowTable = rowTable;
        this.rowLogColumnFamily = rowLogColumnFamily;
        this.payloadPrefix = new byte[]{rowLogId, PL_BYTE};
        this.executionStatePrefix = new byte[]{rowLogId, ES_BYTE};
        this.seqNrQualifier = Bytes.add(new byte[]{rowLogId}, SEQ_NR);
        this.rowLogConfigurationManager = rowLogConfigurationManager;
        this.rowLocker = rowLocker;
        rowLogConfigurationManager.addRowLogObserver(id, this);
        synchronized (initialRowLogConfigLoaded) {
            while(!initialRowLogConfigLoaded.get()) {
                initialRowLogConfigLoaded.wait();
            }
        }
        this.processorNotifier = new RowLogProcessorNotifier(rowLogConfigurationManager, rowLogConfig.getNotifyDelay());
        rowLogConfigurationManager.addSubscriptionsObserver(id, this);
        synchronized (initialSubscriptionsLoaded) {
            while (!initialSubscriptionsLoaded.get()) {
                initialSubscriptionsLoaded.wait();
            }
        }
    }

    public void stop() {
        rowLogConfigurationManager.removeRowLogObserver(id, this);
        synchronized (initialRowLogConfigLoaded) {
            initialRowLogConfigLoaded.set(false);
        }
        rowLogConfigurationManager.removeSubscriptionsObserver(id, this);
        synchronized (initialSubscriptionsLoaded) {
            initialSubscriptionsLoaded.set(false);
        }
        Closer.close(processorNotifier);
    }
    
    @Override
    protected void finalize() throws Throwable {
        stop();
        super.finalize();
    }
    
    public String getId() {
        return id;
    }
    
    public void registerShard(RowLogShard shard) {
        this.shard = shard;
    }
    
    public void unRegisterShard(RowLogShard shard) {
        this.shard = null;
    }
    
    private void putPayload(long seqnr, byte[] payload, long timestamp, Put put) throws IOException {
        put.add(rowLogColumnFamily, payloadQualifier(seqnr, timestamp), payload);
    }

    public byte[] getPayload(RowLogMessage message) throws RowLogException {
        byte[] rowKey = message.getRowKey();
        byte[] qualifier = payloadQualifier(message.getSeqNr(), message.getTimestamp());
        Get get = new Get(rowKey);
        get.addColumn(rowLogColumnFamily, qualifier);
        Result result;
        try {
            result = rowTable.get(get);
        } catch (IOException e) {
            throw new RowLogException("Exception while getting payload from the rowTable", e);
        }
        return result.getValue(rowLogColumnFamily, qualifier);
    }

    public RowLogMessage putMessage(byte[] rowKey, byte[] data, byte[] payload, Put put) throws InterruptedException, RowLogException {
        try {
            // Take current snapshot of the subscriptions so that shard.putMessage and initializeSubscriptions
            // use the exact same set of subscriptions.
            List<RowLogSubscription> subscriptions = getSubscriptions();
            if (subscriptions.isEmpty()) 
                return null;

            // Get a sequence number for this new message
            long seqnr = rowTable.incrementColumnValue(rowKey, rowLogColumnFamily, seqNrQualifier, 1L);

            // Create Put object if not supplied
            boolean ownPut = false;
            if (put == null) {
                put = new Put(rowKey);
                ownPut = true;
            }

            long now = System.currentTimeMillis();

            putPayload(seqnr, payload, now, put);
                    
            RowLogMessage message = new RowLogMessageImpl(now, rowKey, seqnr, data, payload, this);

            putMessageOnShard(message, subscriptions);

            initializeSubscriptions(message, put, subscriptions);

            // If the Put was not supplied by the user, apply it now
            if (ownPut) {
                rowTable.put(put);
            }

            if (rowLogConfig.isEnableNotify()) {
                processorNotifier.notifyProcessor(id, getShard().getId());
            }
            return message;
        } catch (IOException e) {
            throw new RowLogException("Failed to put message on RowLog", e);
        }
    }

    protected void putMessageOnShard(RowLogMessage message, List<RowLogSubscription> subscriptions)
            throws RowLogException {
        List<String> subscriptionIds = new ArrayList<String>(subscriptions.size());
        for (RowLogSubscription subscription : subscriptions) {
            subscriptionIds.add(subscription.getId());
        }
        getShard().putMessage(message, subscriptionIds);
    }

    
    private void initializeSubscriptions(RowLogMessage message, Put put, List<RowLogSubscription> subscriptions)
            throws IOException {
        String[] subscriptionIds = new String[subscriptions.size()];
        for (int i = 0; i < subscriptions.size(); i++) {
            subscriptionIds[i] = subscriptions.get(i).getId();
        }
        SubscriptionExecutionState executionState = new SubscriptionExecutionState(message.getTimestamp(),
                subscriptionIds);
        byte[] qualifier = executionStateQualifier(message.getSeqNr(), message.getTimestamp());
        put.add(rowLogColumnFamily, qualifier, executionState.toBytes());
    }

    public boolean processMessage(RowLogMessage message, RowLock lock) throws RowLogException, InterruptedException {
        if (message == null)
            return true;
        byte[] rowKey = message.getRowKey();
        byte[] executionStateQualifier = executionStateQualifier(message.getSeqNr(), message.getTimestamp());
        Get get = new Get(rowKey);
        get.addColumn(rowLogColumnFamily, executionStateQualifier);
        try {
            Result result = rowTable.get(get);
            if (result.isEmpty()) {
                // No execution state was found indicating an orphan message on the global queue table
                // Treat this message as if it was processed
                return true;
            }
            byte[] previousValue = result.getValue(rowLogColumnFamily, executionStateQualifier);
            SubscriptionExecutionState executionState = SubscriptionExecutionState.fromBytes(previousValue);

            boolean allDone = processMessage(message, executionState);
            
            if (allDone) {
                return handleAllDone(message, rowKey, executionStateQualifier, previousValue, lock);
            } else {
                if (rowLocker != null) {
                    // TODO (bruno) return value is ignored, is this ok?
                    updateExecutionState(rowKey, executionStateQualifier, executionState, previousValue, lock);
                } else {
                    // TODO (bruno) return value is ignored, is this ok?
                    updateExecutionState(rowKey, executionStateQualifier, executionState, previousValue);
                }
                return false;
            }
        } catch (IOException e) {
            throw new RowLogException("Failed to process message", e);
        }
    }

    protected boolean handleAllDone(RowLogMessage message, byte[] rowKey, byte[] executionStateQualifier,
            byte[] previousValue, RowLock lock) throws RowLogException, IOException {

        if (lock != null) {
            return removeExecutionStateAndPayload(rowKey, executionStateQualifier, payloadQualifier(message.getSeqNr(),
                    message.getTimestamp()), lock);
        } else {
            removeExecutionStateAndPayload(rowKey, executionStateQualifier, payloadQualifier(message.getSeqNr(),
                    message.getTimestamp()));
            return true;
        }
    }

    private boolean processMessage(RowLogMessage message, SubscriptionExecutionState executionState) throws RowLogException, InterruptedException {
        boolean allDone = true;
        RowLogSubscription[] subscriptions = getSubscriptionsAsArray();
        if (rowLogConfig.isRespectOrder()) {
            Arrays.sort(subscriptions);
        }

        if (log.isDebugEnabled()) {
            log.debug("Processing msg '" + formatId(message) + "' nr of subscriptions: " + subscriptions.length);
        }

        for (RowLogSubscription subscription : subscriptions) {
            String subscriptionId = subscription.getId();

            if (log.isDebugEnabled()) {
                log.debug("Processing msg '" + formatId(message) + "', subscr '" + subscriptionId + "' state: " +
                        executionState.getState(subscriptionId));
            }

            if (!executionState.getState(subscriptionId)) {
                boolean done = false;
                RowLogMessageListener listener = RowLogMessageListenerMapping.INSTANCE.get(subscriptionId);
                if (listener != null) {
                    done = listener.processMessage(message);
                    if (log.isDebugEnabled()) {
                        log.debug("Processing msg '" + formatId(message) + "', subscr '" + subscriptionId +
                                "' processing result: " + done);
                    }
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("Processing msg '" + formatId(message) + "', subscr '" + subscriptionId +
                                "' no listener present.");
                    }
                }
                executionState.setState(subscriptionId, done);
                if (!done) {
                    allDone = false;
                    if (rowLogConfig.isRespectOrder()) {
                        break;
                    }
                } else {
                    removeMessageFromShard(message, subscriptionId);
                }
            }
        }
        return allDone;
    }

    private String formatId(RowLogMessage message) {
        return Bytes.toStringBinary(message.getRowKey()) + ":" + message.getSeqNr();
    }
    
    public List<RowLogSubscription> getSubscriptions() {
        synchronized (subscriptions) {
            return new ArrayList<RowLogSubscription>(subscriptions.values());
        }
    }
    
    public RowLogSubscription[] getSubscriptionsAsArray() {
        synchronized (subscriptions) {
            return (RowLogSubscription[])subscriptions.values().toArray();
        }
    }

    public boolean messageDone(RowLogMessage message, String subscriptionId) throws RowLogException, InterruptedException {
        if (rowLocker != null) { // If the rowLocker exists the lock should be a RowLock
            RowLock rowLock = null;
            try {
                rowLock = rowLocker.lockRow(message.getRowKey());
            } catch (IOException e) {
                log.debug("Exception occurred while trying to take lock, retrying", e);
                // retry
            }
            while (rowLock == null) {
                Thread.sleep(10);
                try {
                    rowLock = rowLocker.lockRow(message.getRowKey());
                } catch (IOException e) {
                    log.debug("Exception occurred while trying to take lock, retrying", e);
                    // retry
                }
            }
            return messageDoneRowLocked(message, subscriptionId, rowLock);
        }
        return messageDone(message, subscriptionId, 0); 
    }
    
    private boolean messageDoneRowLocked(RowLogMessage message, String subscriptionId, RowLock rowLock) throws RowLogException {
        byte[] rowKey = message.getRowKey();
        byte[] executionStateQualifier = executionStateQualifier(message.getSeqNr(), message.getTimestamp());
        Get get = new Get(rowKey);
        get.addColumn(rowLogColumnFamily, executionStateQualifier);
        try {
            Result result = rowTable.get(get);
            if (!result.isEmpty()) {
                byte[] previousValue = result.getValue(rowLogColumnFamily, executionStateQualifier);
                SubscriptionExecutionState executionState = SubscriptionExecutionState.fromBytes(previousValue);
                executionState.setState(subscriptionId, true);
                if (executionState.allDone()) {
                    handleAllDone(message, rowKey, executionStateQualifier, previousValue, rowLock);
                } else {
                    if (!updateExecutionState(rowKey, executionStateQualifier, executionState, previousValue, rowLock))
                        return false;
                }
            }
            removeMessageFromShard(message, subscriptionId);
            return true;
        } catch (IOException e) {
            throw new RowLogException("Failed to put message to done", e);
        }
    
    }
    
    private boolean messageDone(RowLogMessage message, String subscriptionId, int count) throws RowLogException {
        if (count >= 10) {
            return false;
        }
        byte[] rowKey = message.getRowKey();
        byte[] executionStateQualifier = executionStateQualifier(message.getSeqNr(), message.getTimestamp());
        Get get = new Get(rowKey);
        get.addColumn(rowLogColumnFamily, executionStateQualifier);
        try {
            Result result = rowTable.get(get);
            if (!result.isEmpty()) {
                byte[] previousValue = result.getValue(rowLogColumnFamily, executionStateQualifier);
                SubscriptionExecutionState executionState = SubscriptionExecutionState.fromBytes(previousValue);
                executionState.setState(subscriptionId, true);
                if (executionState.allDone()) {
                    handleAllDone(message, rowKey, executionStateQualifier, previousValue, null);
                } else {
                    if (!updateExecutionState(rowKey, executionStateQualifier, executionState, previousValue)) {
                        return messageDone(message, subscriptionId, count + 1); // Retry
                    }
                }
            }
            removeMessageFromShard(message, subscriptionId);
            return true;
        } catch (IOException e) {
            throw new RowLogException("Failed to put message to done", e);
        }
    }
 
    protected void removeMessageFromShard(RowLogMessage message, String subscriptionId) throws RowLogException {
        getShard().removeMessage(message, subscriptionId);
    }
    
    public boolean isMessageDone(RowLogMessage message, String subscriptionId) throws RowLogException {
        SubscriptionExecutionState executionState = getExecutionState(message);
        if (executionState == null) {
            checkOrphanMessage(message, subscriptionId);
            return true;
        }
        return executionState.getState(subscriptionId);
    }

    private SubscriptionExecutionState getExecutionState(RowLogMessage message) throws RowLogException {
        byte[] rowKey = message.getRowKey();
        byte[] executionStateQualifier = executionStateQualifier(message.getSeqNr(), message.getTimestamp());
        Get get = new Get(rowKey);
        get.addColumn(rowLogColumnFamily, executionStateQualifier);
        SubscriptionExecutionState executionState = null;
        try {
            Result result = rowTable.get(get);
            byte[] previousValue = result.getValue(rowLogColumnFamily, executionStateQualifier);
            if (previousValue != null)
                executionState = SubscriptionExecutionState.fromBytes(previousValue);
        } catch (IOException e) {
            throw new RowLogException("Failed to check if message is done", e);
        }
        return executionState;
    }

    public boolean isMessageAvailable(RowLogMessage message, String subscriptionId) throws RowLogException {
        SubscriptionExecutionState executionState = getExecutionState(message);
        if (executionState == null) {
            checkOrphanMessage(message, subscriptionId);
            return false;
        }
        if (rowLogConfig.isRespectOrder()) {
            RowLogSubscription[] subscriptions = getSubscriptionsAsArray();
            Arrays.sort(subscriptions);
            for (RowLogSubscription subscriptionContext : subscriptions) {
                if (subscriptionId.equals(subscriptionContext.getId()))
                    break;
                if (!executionState.getState(subscriptionContext.getId())) {
                    return false; // There is a previous subscription to be processed first
                }
            }
        }
        return !executionState.getState(subscriptionId);
    }
    
    private boolean updateExecutionState(byte[] rowKey, byte[] executionStateQualifier, SubscriptionExecutionState executionState, byte[] previousValue) throws IOException {
        Put put = new Put(rowKey);
        put.add(rowLogColumnFamily, executionStateQualifier, executionState.toBytes());
        return rowTable.checkAndPut(rowKey, rowLogColumnFamily, executionStateQualifier, previousValue, put);
    }
    
    private boolean updateExecutionState(byte[] rowKey, byte[] executionStateQualifier, SubscriptionExecutionState executionState, byte[] previousValue, RowLock rowLock) throws IOException {
        Put put = new Put(rowKey);
        put.add(rowLogColumnFamily, executionStateQualifier, executionState.toBytes());
        return rowLocker.put(put, rowLock);
    }

    private void removeExecutionStateAndPayload(byte[] rowKey, byte[] executionStateQualifier, byte[] payloadQualifier)
            throws IOException {
        Delete delete = new Delete(rowKey); 
        delete.deleteColumns(rowLogColumnFamily, executionStateQualifier);
        delete.deleteColumns(rowLogColumnFamily, payloadQualifier);
        rowTable.delete(delete);
    }

    private boolean removeExecutionStateAndPayload(byte[] rowKey, byte[] executionStateQualifier,
            byte[] payloadQualifier, RowLock rowLock) throws IOException {
        Delete delete = new Delete(rowKey); 
        delete.deleteColumns(rowLogColumnFamily, executionStateQualifier);
        delete.deleteColumns(rowLogColumnFamily, payloadQualifier);
        return rowLocker.delete(delete, rowLock);
    }
    
    // For now we work with only one shard
    protected RowLogShard getShard() throws RowLogException {
        if (shard == null) {
            throw new RowLogException("No shards registerd");
        }
        return shard;
    }
    
    public List<RowLogMessage> getMessages(byte[] rowKey, String ... subscriptionIds) throws RowLogException {
        List<RowLogMessage> messages = new ArrayList<RowLogMessage>();
        Get get = new Get(rowKey);
        get.addFamily(rowLogColumnFamily);
        get.setFilter(new ColumnPrefixFilter(executionStatePrefix));
        try {
            Result result = rowTable.get(get);
            if (!result.isEmpty()) {
                NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(rowLogColumnFamily);
                for (Entry<byte[], byte[]> entry : familyMap.entrySet()) {
                    SubscriptionExecutionState executionState = SubscriptionExecutionState.fromBytes(entry.getValue());
                    boolean add = false;
                    if (subscriptionIds.length == 0)
                        add = true;
                    else {
                        for (String subscriptionId : subscriptionIds) {
                            if (!executionState.getState(subscriptionId))
                                add = true;
                        }
                    }
                    if (add) {
                        ByteBuffer buffer = ByteBuffer.wrap(entry.getKey());
                        messages.add(new RowLogMessageImpl(executionState.getTimestamp(), rowKey, buffer.getLong(2), null, this));
                    }
                }
            }
        } catch (IOException e) {
            throw new RowLogException("Failed to get messages", e);
        }
        return messages;
    }
    
    /**
     * Checks if the message is orphaned, meaning there is a message on the global queue which has no representative on the row-local queue.
     * If the message is orphaned it is removed from the shard.
     * @param message the message to check
     * @param subscriptionId the subscription to check the message for
     */
    private void checkOrphanMessage(RowLogMessage message, String subscriptionId) throws RowLogException {
        Get get = new Get(message.getRowKey());
        byte[] executionStateQualifier = executionStateQualifier(message.getSeqNr(), message.getTimestamp());
        get.addColumn(rowLogColumnFamily, executionStateQualifier);
        Result result;
        try {
            result = rowTable.get(get);
            if (result.isEmpty()) {
                removeOrphanMessageFromShard(message, subscriptionId);
            }
        } catch (IOException e) {
            throw new RowLogException("Failed to check is message "+message+" is orphaned for subscription " + subscriptionId, e);
        }
    }
    
    protected void removeOrphanMessageFromShard(RowLogMessage message, String subscriptionId) throws RowLogException {
        removeMessageFromShard(message, subscriptionId);
    }
    
    public void subscriptionsChanged(List<RowLogSubscription> newSubscriptions) {
        synchronized (subscriptions) {
            for (RowLogSubscription subscription : newSubscriptions) {
                subscriptions.put(subscription.getId(), subscription);
            }
            Iterator<RowLogSubscription> iterator = subscriptions.values().iterator();
            while (iterator.hasNext()) {
                RowLogSubscription subscription = iterator.next();
                if (!newSubscriptions.contains(subscription))
                    iterator.remove();
            }
        }
        if (!initialSubscriptionsLoaded.get()) {
            synchronized (initialSubscriptionsLoaded) {
                initialSubscriptionsLoaded.set(true);
                initialSubscriptionsLoaded.notifyAll();
            }
        }
    }

    public List<RowLogShard> getShards() {
        List<RowLogShard> shards = new ArrayList<RowLogShard>();
        shards.add(shard);
        return shards;
    }
    
    public void rowLogConfigChanged(RowLogConfig rowLogConfig) {
        this.rowLogConfig = rowLogConfig;
        if (!initialRowLogConfigLoaded.get()) {
            synchronized(initialRowLogConfigLoaded) {
                initialRowLogConfigLoaded.set(true);
                initialRowLogConfigLoaded.notifyAll();
            }
        }
    }

    private byte[] payloadQualifier(long seqnr, long timestamp) {
        ByteBuffer buffer = ByteBuffer.allocate(2 + 8 + 8); // payload-prefix + seqnr + timestamp
        buffer.put(payloadPrefix);
        buffer.putLong(seqnr);
        buffer.putLong(timestamp);
        return buffer.array();
    }
    
    private byte[] executionStateQualifier(long seqnr, long timestamp) {
        ByteBuffer buffer = ByteBuffer.allocate(2 + 8 + 8); // executionState-prefix + seqnr + timestamp
        buffer.put(executionStatePrefix);
        buffer.putLong(seqnr);
        buffer.putLong(timestamp);
        return buffer.array();
    }
 }
