package org.lilycms.rowlog.api;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.Put;

public interface RowLog {
	void registerShard(RowLogShard shard);
	void registerConsumer(RowLogMessageConsumer rowLogMessageConsumer);
	void unRegisterConsumer(RowLogMessageConsumer rowLogMessageConsumer);
	byte[] getPayload(byte[] rowKey, long seqnr) throws IOException;
	RowLogMessage putMessage(byte[] rowKey, byte[] data, byte[] payload, Put put) throws RowLogException;
	boolean processMessage(RowLogMessage message) throws IOException;
	void messageDone(RowLogMessage message, int consumerId) throws IOException, RowLogException;
	List<RowLogMessageConsumer> getConsumers();
}
