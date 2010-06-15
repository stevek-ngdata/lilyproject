package org.lilycms.rowlog.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilycms.rowlog.api.RowLog;
import org.lilycms.rowlog.api.RowLogException;
import org.lilycms.rowlog.api.RowLogMessage;
import org.lilycms.rowlog.api.RowLogMessageConsumer;
import org.lilycms.rowlog.api.RowLogShard;

public class RowLogImpl implements RowLog {

	private static final byte[] SEQ_NR = Bytes.toBytes("SEQNR");
	private RowLogShard shard;
	private final HTable rowTable;
	private final byte[] payloadColumnFamily;
	private final byte[] rowLogColumnFamily;
	
	private List<RowLogMessageConsumer> consumers = Collections.synchronizedList(new ArrayList<RowLogMessageConsumer>());
	
	public RowLogImpl(HTable rowTable, byte[] payloadColumnFamily, byte[] rowLogColumnFamily) {
		this.rowTable = rowTable;
		this.payloadColumnFamily = payloadColumnFamily;
		this.rowLogColumnFamily = rowLogColumnFamily;
    }
	
	public void registerConsumer(RowLogMessageConsumer rowLogMessageConsumer) {
		consumers.add(rowLogMessageConsumer);
	}
	
	public void unRegisterConsumer(RowLogMessageConsumer rowLogMessageConsumer) {
		consumers.remove(rowLogMessageConsumer);
	}
	
	public List<RowLogMessageConsumer> getConsumers() {
		return consumers;
	}
	
	public void registerShard(RowLogShard shard) {
		this.shard = shard;
	}
	
	private long putPayload(byte[] rowKey, byte[] payload, Put put) throws IOException {
		Get get = new Get(rowKey);
    	get.addColumn(payloadColumnFamily, SEQ_NR);
    	Result result = rowTable.get(get);
    	byte[] value = result.getValue(payloadColumnFamily, SEQ_NR);
    	long seqnr = -1;
    	if (value != null) {
    		seqnr = Bytes.toLong(value);
    	}
    	seqnr++;
    	if (put != null) {
    		put.add(payloadColumnFamily, Bytes.toBytes(seqnr), payload);
    	} else {
    		put = new Put(rowKey);
    		put.add(payloadColumnFamily, Bytes.toBytes(seqnr), payload);
    		rowTable.put(put);
    	}
    	return seqnr;
	}

	public byte[] getPayload(byte[] rowKey, long seqnr) throws IOException {
		Get get = new Get(rowKey);
		get.addColumn(payloadColumnFamily, Bytes.toBytes(seqnr));
		Result result = rowTable.get(get);
		return result.getValue(payloadColumnFamily, Bytes.toBytes(seqnr));
	}

	public RowLogMessage putMessage(byte[] rowKey, byte[] data, byte[] payload, Put put) throws RowLogException {
		RowLogShard shard = getShard(); // Fail fast if no shards are registered
		
		try {
			long seqnr = putPayload(rowKey, payload, put);
		
			byte[] messageId = Bytes.toBytes(System.currentTimeMillis()); 
			messageId = Bytes.add(messageId, Bytes.toBytes(seqnr));
			messageId = Bytes.add(messageId, rowKey);
			
			RowLogMessage message = new RowLogMessageImpl(messageId, rowKey, seqnr, data, this);
		
			for (RowLogMessageConsumer consumer : consumers) {
				shard.putMessage(message, consumer.getId());
			}
			initializeConsumers(message, put);
			return message;
		} catch (IOException e) {
			throw new RowLogException("Failed to put message for row <" + rowKey + "> on row log", e);
		}
	}

	private void initializeConsumers(RowLogMessage message, Put put) throws IOException {
		RowLogMessageConsumerExecutionState executionState = new RowLogMessageConsumerExecutionState(message.getId());
		for (RowLogMessageConsumer consumer : consumers) {
			executionState.setState(consumer.getId(), false);
		}
		if (put != null) {
			put.add(rowLogColumnFamily, Bytes.toBytes(message.getSeqNr()), executionState.toBytes());
		} else {
			put = new Put(message.getRowKey());
			put.add(rowLogColumnFamily, Bytes.toBytes(message.getSeqNr()), executionState.toBytes());
			rowTable.put(put);
		}
	}

	public void messageDone(RowLogMessage message, int consumerId) throws IOException, RowLogException {
		RowLogShard shard = getShard(); // Fail fast if no shards are registered
		byte[] rowKey = message.getRowKey();
		long seqnr = message.getSeqNr();
		byte[] messageColumn = Bytes.toBytes(seqnr);
		Get get = new Get(rowKey);
		get.addColumn(rowLogColumnFamily, messageColumn);
		Result result = rowTable.get(get);
		if (!result.isEmpty()) {
			RowLogMessageConsumerExecutionState executionState = RowLogMessageConsumerExecutionState.fromBytes(result.getValue(rowLogColumnFamily, messageColumn));
			executionState.setState(consumerId, true);
			if (executionState.allDone()) {
				removeExecutionState(rowKey, messageColumn);
			} else {
				updateExecutionState(rowKey, messageColumn, executionState);
			}
		}
		shard.removeMessage(message, consumerId);
	}

	public boolean processMessage(RowLogMessage message) throws IOException {
		byte[] rowKey = message.getRowKey();
		long seqnr = message.getSeqNr();
		byte[] messageColumn = Bytes.toBytes(seqnr);
			Get get = new Get(rowKey);
			get.addColumn(rowLogColumnFamily, messageColumn);
			Result result = rowTable.get(get);
			RowLogMessageConsumerExecutionState executionState = RowLogMessageConsumerExecutionState.fromBytes(result.getValue(rowLogColumnFamily, messageColumn));
			
			boolean allDone = processMessage(message, executionState);
			
			if (allDone) {
				removeExecutionState(rowKey, messageColumn);
			} else {
				updateExecutionState(rowKey, messageColumn, executionState);
			}
			return allDone;
	}

	private boolean processMessage(RowLogMessage message, RowLogMessageConsumerExecutionState executionState) throws IOException {
		boolean allDone = true;
		for (RowLogMessageConsumer consumer : consumers) {
			int consumerId = consumer.getId();
			if (!executionState.getState(consumerId)) {
				boolean done = false;
				try {
					done = consumer.processMessage(message);
				} catch (Throwable t) {
					executionState.setState(consumerId, false);
					return false;
				}
				executionState.setState(consumerId, done);
				if (!done) {
					allDone = false;
				} else {
					shard.removeMessage(message, consumerId);
				}
			}
		}
		return allDone;
	}

	private void updateExecutionState(byte[] rowKey, byte[] messageColumn, RowLogMessageConsumerExecutionState executionState) throws IOException {
	    Put put = new Put(rowKey);
	    put.add(rowLogColumnFamily, messageColumn, executionState.toBytes());
	    rowTable.put(put);
    }

	private void removeExecutionState(byte[] rowKey, byte[] messageColumn) throws IOException {
	    Delete delete = new Delete(rowKey); 
	    delete.deleteColumn(rowLogColumnFamily, messageColumn);
	    rowTable.delete(delete);
    }

	
	// For now we work with only one shard
	private RowLogShard getShard() throws RowLogException {
		if (shard == null) {
			throw new RowLogException("No shards registerd");
		}
		return shard;
	}
}
