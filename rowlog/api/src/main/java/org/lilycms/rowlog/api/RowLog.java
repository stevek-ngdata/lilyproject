package org.lilycms.rowlog.api;

import java.util.List;

import org.apache.hadoop.hbase.client.Put;

public interface RowLog {
	void registerShard(RowLogShard shard);
	void registerConsumer(RowLogMessageConsumer rowLogMessageConsumer);
	void unRegisterConsumer(RowLogMessageConsumer rowLogMessageConsumer);
	byte[] getPayload(byte[] rowKey, long seqnr) throws RowLogException;
	RowLogMessage putMessage(byte[] rowKey, byte[] data, byte[] payload, Put put) throws RowLogException;
	boolean processMessage(RowLogMessage message) throws RowLogException;
	byte[] lockMessage(RowLogMessage message, int consumerId) throws RowLogException;
	boolean unlockMessage(RowLogMessage message, int consumerId, byte[] lock) throws RowLogException;
	boolean isMessageLocked(RowLogMessage message, int consumerId) throws RowLogException;
	boolean messageDone(RowLogMessage message, int consumerId, byte[] lock) throws RowLogException;
	List<RowLogMessageConsumer> getConsumers();
}
