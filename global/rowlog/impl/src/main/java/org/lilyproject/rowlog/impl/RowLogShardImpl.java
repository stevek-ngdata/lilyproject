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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilyproject.rowlog.api.RowLog;
import org.lilyproject.rowlog.api.RowLogException;
import org.lilyproject.rowlog.api.RowLogMessage;
import org.lilyproject.rowlog.api.RowLogShard;
import org.lilyproject.rowlog.api.RowLogSubscription;
import org.lilyproject.util.io.Closer;

public class RowLogShardImpl implements RowLogShard {

    public static final byte[] MESSAGES_CF = Bytes.toBytes("messages");
    private static final byte[] MESSAGE_COLUMN = Bytes.toBytes("msg");
    private HTableInterface table;
    private final RowLog rowLog;
    private final String id;
    private byte[] rowKeyPrefix;
    private final int deleteBufferSize;
    private final List<Delete> messagesToDelete;
    private long lastDelete;
    // while 0 is a valid unicode codepoint, it will only occur in utf-8 when using the NULL char
    private byte[] END_OF_SUBSCRIPTION_NAME_MARKER = new byte[]{(byte)0};

    public RowLogShardImpl(String id, byte[] rowKeyPrefix, HTableInterface table, RowLog rowLog, int deleteBufferSize)
            throws IOException {
        this.id = id;
        this.rowKeyPrefix = rowKeyPrefix;
        this.table = table;
        this.rowLog = rowLog;
        this.deleteBufferSize = deleteBufferSize;

        this.messagesToDelete = new ArrayList<Delete>(deleteBufferSize);
        this.lastDelete = System.currentTimeMillis();
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public void putMessage(RowLogMessage message) throws RowLogException {
        for (RowLogSubscription subscription : rowLog.getSubscriptions()) {
            putMessage(message, subscription.getId());
        }
    }

    @Override
    public void putMessage(RowLogMessage message, List<String> subscriptionIds) throws RowLogException {
        for (String subscriptionId : subscriptionIds) {
            putMessage(message, subscriptionId);
        }
    }

    private void putMessage(RowLogMessage message, String subscriptionId) throws RowLogException {
        byte[] rowKey = createRowKey(message, subscriptionId);
        Put put = new Put(rowKey);
        put.add(MESSAGES_CF, MESSAGE_COLUMN, encodeMessage(message));
        try {
            table.put(put);
        } catch (IOException e) {
            throw new RowLogException("Failed to put message on RowLogShard", e);
        }
    }

    /**
     * Removing a message is batched.
     * <p/>
     * <p>A message will only be removed, either when the deleteBufferSize is reached, the last time messages were
     * removed was 5 minutes ago or new batch of messages is requested from the shard. See {@link #next(String, Long, int)}.
     * In case many messages are being processed, this will reduce the number of delete calls on the HBase table to approximately 1
     * per batch.
     */
    @Override
    public void removeMessage(RowLogMessage message, String subscription) throws RowLogException {
        Delete delete = new Delete(createRowKey(message, subscription));
        // We can safely disable write to wal for these deletes for the same reason that we can buffer
        // them: the local queue is the authoritative source. In case a delete would be lost, the
        // RowLogProcessor will simply trigger processing of this message again, but we'll see in the
        // local queue that it's already done (or gone) and this delete will be performed again.
        delete.setWriteToWAL(false);

        if (deleteBufferSize <= 1) {
            try {
                table.delete(delete);
            } catch (IOException e) {
                throw new RowLogException("Failed deleting message from rowlog shard table", e);
            }
        } else {
            synchronized (messagesToDelete) {
                messagesToDelete.add(delete);
            }
            if (messagesToDelete.size() >= deleteBufferSize || (lastDelete + 300000 < System.currentTimeMillis())) {
                flushMessageDeleteBuffer();
            }
        }
    }

    public void flushMessageDeleteBuffer() throws RowLogException {
        List<Delete> deletes = null;
        synchronized (messagesToDelete) {
            if (!messagesToDelete.isEmpty()) {
                deletes = new ArrayList<Delete>(messagesToDelete);
                messagesToDelete.clear();
            }
            lastDelete = System.currentTimeMillis();
        }
        try {
            if ((deletes != null) && !deletes.isEmpty()) // Avoid unnecessary deletes
            {
                table.delete(deletes);
            }
        } catch (IOException e) {
            throw new RowLogException("Failed to remove messages from RowLogShard", e);
        }
    }

    @Override
    public List<RowLogMessage> next(String subscription, int batchSize) throws RowLogException {
        return next(subscription, null, batchSize);
    }

    @Override
    public List<RowLogMessage> next(String subscription, Long minimalTimestamp, int batchSize) throws RowLogException {
        // Before collecting a new batch of messages, any outstanding deletes are executed first. 
        flushMessageDeleteBuffer();
        byte[] rowPrefix = Bytes.add(rowKeyPrefix, Bytes.toBytes(subscription), END_OF_SUBSCRIPTION_NAME_MARKER);
        byte[] startRow = rowPrefix;
        if (minimalTimestamp != null) {
            startRow = Bytes.add(startRow, Bytes.toBytes(minimalTimestamp));
        }
        try {
            List<RowLogMessage> rowLogMessages = new ArrayList<RowLogMessage>();
            Scan scan = new Scan(startRow);
            scan.setCaching(batchSize);
            // Don't filter on HBase timestamp: in some cases it could be behind our timestamp, or in case
            // of clock skew it can even be before
            //if (minimalTimestamp != null)
            //    scan.setTimeRange(minimalTimestamp, Long.MAX_VALUE);
            scan.addColumn(MESSAGES_CF, MESSAGE_COLUMN);
            // Set stop row for the scanner. This avoids the scanner to return entries belonging to the
            // next subscription. In case of rowlog sharding, it also avoids opening a scanner on the next
            // region just to find there is no interesting data there (which would be the case if we stop
            // the scanning using a PrefixFilter).
            byte[] stopRowPrefix = new byte[rowPrefix.length];
            System.arraycopy(rowPrefix, 0, stopRowPrefix, 0, rowPrefix.length);
            stopRowPrefix[stopRowPrefix.length - 1]++; // increasing END_OF_SUBSCRIPTION_NAME_MARKER, can't overflow
            scan.setStopRow(stopRowPrefix);

            ResultScanner scanner = table.getScanner(scan);

            for (int i = 0; i < batchSize; i++) {
                Result result = scanner.next();
                if (result == null) {
                    break;
                }

                byte[] rowKey = result.getRow();
                // This check should be unnecessary because the scan should only return relevant messages
                if (!Bytes.startsWith(rowKey, rowPrefix)) {
                    break; // There were no messages for this subscription
                }
                byte[] value = result.getValue(MESSAGES_CF, MESSAGE_COLUMN);
                byte[] messageId = Bytes.tail(rowKey, rowKey.length - rowPrefix.length);
                rowLogMessages.add(decodeMessage(messageId, value));
            }

            // The scanner is not closed in a finally block, since when we get an IOException from
            // HBase, it is likely that closing the scanner will give problems too. Not closing
            // the scanner is not fatal since HBase will expire it after a while.
            Closer.close(scanner);

            return rowLogMessages;
        } catch (IOException e) {
            throw new RowLogException("Failed to fetch next message from RowLogShard", e);
        }
    }

    private byte[] createRowKey(RowLogMessage message, String subscription) {
        byte[] subscriptionBytes = Bytes.toBytes(subscription);
        byte[] msgRowkey = message.getRowKey();

        byte[] rowKey = new byte[
                rowKeyPrefix.length +
                        subscriptionBytes.length +
                        END_OF_SUBSCRIPTION_NAME_MARKER.length +
                        Bytes.SIZEOF_LONG +
                        Bytes.SIZEOF_LONG +
                        msgRowkey.length];

        System.arraycopy(rowKeyPrefix, 0, rowKey, 0, rowKeyPrefix.length);
        int offset = rowKeyPrefix.length;
        System.arraycopy(subscriptionBytes, 0, rowKey, offset, subscriptionBytes.length);
        offset += subscriptionBytes.length;
        System.arraycopy(END_OF_SUBSCRIPTION_NAME_MARKER, 0, rowKey, offset, END_OF_SUBSCRIPTION_NAME_MARKER.length);
        offset += END_OF_SUBSCRIPTION_NAME_MARKER.length;
        Bytes.putLong(rowKey, offset, message.getTimestamp());
        offset += Bytes.SIZEOF_LONG;
        Bytes.putLong(rowKey, offset, message.getSeqNr());
        offset += Bytes.SIZEOF_LONG;
        System.arraycopy(msgRowkey, 0, rowKey, offset, msgRowkey.length);

        return rowKey;
    }

    private byte[] encodeMessage(RowLogMessage message) {
        return message.getData();
    }

    private RowLogMessage decodeMessage(byte[] messageId, byte[] data) {
        long timestamp = Bytes.toLong(messageId);
        long seqNr = Bytes.toLong(messageId, Bytes.SIZEOF_LONG);
        byte[] rowKey = Bytes.tail(messageId, messageId.length - (2 * Bytes.SIZEOF_LONG));
        return new RowLogMessageImpl(timestamp, rowKey, seqNr, data, rowLog);
    }

}
