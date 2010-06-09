package org.lilycms.rowlog.api;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowLock;

public interface RowLog {
	void registerShard(RowLogShard shard);
	void registerConsumer(RowLogMessageConsumer rowLogMessageConsumer);
	void unRegisterConsumer(RowLogMessageConsumer rowLogMessageConsumer);
	long putPayload(byte[] rowKey, byte[] data, Put put, RowLock rowLock) throws IOException;
	byte[] getPayload(byte[] rowKey, long seqnr) throws IOException;
	byte[] putMessage(RowLogMessage message, Put put, RowLock rowLock) throws RowLogException;
	boolean processMessage(byte[] messageId, RowLogMessage message, RowLock rowLock) throws IOException;
	void messageDone(byte[] id, RowLogMessage message, int consumerId, RowLock rowLock) throws IOException, RowLogException;
	List<RowLogMessageConsumer> getConsumers();
}
