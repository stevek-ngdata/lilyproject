package org.lilyproject.repotestfw;

import org.apache.hadoop.hbase.client.Put;
import org.lilyproject.rowlock.RowLock;
import org.lilyproject.rowlog.api.*;

import java.util.ArrayList;
import java.util.List;

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

        RowLogMessage msg = delegate.putMessage(rowKey, data, payload, put);
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
