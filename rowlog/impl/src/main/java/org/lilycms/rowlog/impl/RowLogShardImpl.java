package org.lilycms.rowlog.impl;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilycms.rowlog.api.RowLog;
import org.lilycms.rowlog.api.RowLogMessage;
import org.lilycms.rowlog.api.RowLogShard;
import org.lilycms.util.Pair;

public class RowLogShardImpl implements RowLogShard {

	private static final byte[] MESSAGES_CF = Bytes.toBytes("MESSAGES");
	private static final byte[] MESSAGE_COLUMN = Bytes.toBytes("MESSAGE");
	private HTable table;
	private final RowLog rowLog;

	public RowLogShardImpl(String id, RowLog rowLog, Configuration configuration) throws IOException {
		this.rowLog = rowLog;
		try {
			table = new HTable(configuration, id);
        } catch (IOException e) {
            HBaseAdmin admin = new HBaseAdmin(configuration);
            HTableDescriptor tableDescriptor = new HTableDescriptor(id);
            tableDescriptor.addFamily(new HColumnDescriptor(MESSAGES_CF));
            admin.createTable(tableDescriptor);
            table = new HTable(configuration, id);
        }
    }
	
	public void putMessage(byte[] messageId, int consumerId, RowLogMessage message) throws IOException {
		byte[] rowKey = createRowKey(messageId, consumerId);
		
		RowLock rowLock = null;
		try {
			rowLock = table.lockRow(rowKey);
			Put put = new Put(rowKey, rowLock);
			put.add(MESSAGES_CF, MESSAGE_COLUMN, message.toBytes());
			table.put(put);
		} finally {
			if (rowLock != null) {
				table.unlockRow(rowLock);
			}
		}
	}

	public void removeMessage(byte[] messageId, int consumerId) throws IOException {
		byte[] rowKey = createRowKey(messageId, consumerId);
		
		RowLock rowLock = null;
		try {
			rowLock = table.lockRow(rowKey);
			Delete delete = new Delete(rowKey, Long.MAX_VALUE, rowLock);
			table.delete(delete);
		} finally {
			if (rowLock != null) {
				table.unlockRow(rowLock);
			}
		}
	}

	public Pair<byte[], RowLogMessage> next(int consumerId) throws IOException {
		Scan scan = new Scan(Bytes.toBytes(consumerId));
		scan.addColumn(MESSAGES_CF, MESSAGE_COLUMN);
        ResultScanner scanner = table.getScanner(scan);
        Result next = scanner.next();
        if (next == null) 
        	return null;
        byte[] rowKey = next.getRow();
		int actualConsumerId = Bytes.toInt(rowKey);
        if (consumerId != actualConsumerId)
        	return null; // There were no messages for this consumer
        byte[] value = next.getValue(MESSAGES_CF, MESSAGE_COLUMN);
        byte[] messageId = Bytes.tail(rowKey, rowKey.length - Bytes.SIZEOF_INT);
        return new Pair<byte[], RowLogMessage>(messageId, RowLogMessageImpl.fromBytes(value, rowLog));
	}

	private byte[] createRowKey(byte[] messageId, int consumerId) {
		byte[] rowKey = Bytes.toBytes(consumerId);
		rowKey = Bytes.add(rowKey, messageId);
		return rowKey;
	}

}
