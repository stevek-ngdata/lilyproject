package org.lilyproject.rowlog.impl;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.lilyproject.rowlock.RowLocker;
import org.lilyproject.rowlog.api.*;

/**
 * The WalRowLog is an optimized version of the RowLog for the WAL use case. 
 *
 * <p>This WalRowLog should be used together with the {@link WalProcessor} and {@link WalListener} 
 *
 * <p>It optimizes by putting only one message on the RowLogShard which represents all subscriptions together.
 * And this message is only removed once the message has been processed for all registered subscriptions.
 * <br>The message is put on the rowlog shard with the meta subscription id 'WAL' and can be handled by the {@link WalListener}. 
 * <br>The {@link WalProcessor} will pick up this message and request the {@link WalListener} to process it, which 
 * will request the {@link WalRowLog} to process it. This will in its turn request the listeners of each registered
 * (normal) subscription to process the message. (For the WAL use case this processing will be done in-order.)
 * <br>Only when the message has been processed for each subscription it will be removed from the shard.
 * 
 */
public class WalRowLog extends RowLogImpl {

    public static final String WAL_SUBSCRIPTIONID = "WAL";

    public WalRowLog(String id, HTableInterface rowTable, byte[] rowLogColumnFamily, byte rowLogId,
            RowLogConfigurationManager rowLogConfigurationManager, RowLocker rowLocker) throws InterruptedException {
        super(id, rowTable, rowLogColumnFamily, rowLogId, rowLogConfigurationManager, rowLocker);
    }
    
    /**
     * When the RowLogMessage needs to be put on the rowlog shard, we only put it there once with the 'meta' subscription id "WAL".
     */
    @Override
    protected void putMessageOnShard(RowLogMessage message, List<RowLogSubscription> subscriptions) throws RowLogException {
        // Ignore subscriptions and put a message for the 'meta' wal subscription
        getShard().putMessage(message, Arrays.asList(new String[]{WAL_SUBSCRIPTIONID}));
    }
    
    /**
     * Requests to remove the message from the shard for individual subscriptions are ignored.
     * The 'meta' message will only be removed when it has been processed by all subscriptions.
     */
    @Override
    protected void removeMessageFromShard(RowLogMessage message, String subscriptionId) throws RowLogException {
        // Ignore, don't remove message for individual subscriptions.
        // Instead remove the 'meta' message when all messages are done
    }
    
    /**
     * When the message has been processed for all subscriptions (and only then), we can remove the 'meta' message from the rowlog shard.
     */
    @Override
    protected boolean handleAllDone(RowLogMessage message, byte[] rowKey, byte[] executionStateQualifier,
            byte[] previousValue, Object lock) throws IOException, RowLogException {
        // Remove the 'meta' message
        getShard().removeMessage(message, WAL_SUBSCRIPTIONID);
        // Also make sure the execution state and payload are removed from the row-local queue
        return super.handleAllDone(message, rowKey, executionStateQualifier, previousValue, lock);
    }
    
    /**
     * For orphan messages the single 'meta' message can be safely removed.
     */
    @Override
    protected void removeOrphanMessageFromShard(RowLogMessage message, String subscriptionId) throws RowLogException {
        getShard().removeMessage(message, WAL_SUBSCRIPTIONID);
    }
}
