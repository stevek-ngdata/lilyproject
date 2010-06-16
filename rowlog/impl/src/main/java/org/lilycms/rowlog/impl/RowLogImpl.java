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
	private final long lockTimeout;
	
	public RowLogImpl(HTable rowTable, byte[] payloadColumnFamily, byte[] rowLogColumnFamily, long lockTimeout) {
		this.rowTable = rowTable;
		this.payloadColumnFamily = payloadColumnFamily;
		this.rowLogColumnFamily = rowLogColumnFamily;
		this.lockTimeout = lockTimeout;
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

	

	public boolean processMessage(RowLogMessage message) throws IOException {
		byte[] rowKey = message.getRowKey();
		long seqnr = message.getSeqNr();
		byte[] qualifier = Bytes.toBytes(seqnr);
			Get get = new Get(rowKey);
			get.addColumn(rowLogColumnFamily, qualifier);
			Result result = rowTable.get(get);
			byte[] previousValue = result.getValue(rowLogColumnFamily, qualifier);
			RowLogMessageConsumerExecutionState executionState = RowLogMessageConsumerExecutionState.fromBytes(previousValue);
			
			boolean allDone = processMessage(message, executionState);
			
			if (allDone) {
				removeExecutionStateAndPayload(rowKey, qualifier);
			} else {
				updateExecutionState(rowKey, qualifier, executionState, previousValue);
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
	
	public byte[] lock(RowLogMessage message, int consumerId) throws IOException {
		return lockMessage(message, consumerId, 0);
	}
	
	private byte[] lockMessage(RowLogMessage message, int consumerId, int count) throws IOException {
		if (count >= 10) {
			return null;
		}
		byte[] rowKey = message.getRowKey();
		long seqnr = message.getSeqNr();
		byte[] qualifier = Bytes.toBytes(seqnr);
		Get get = new Get(rowKey);
		get.addColumn(rowLogColumnFamily, qualifier);
		Result result = rowTable.get(get);
		if (result.isEmpty()) return null;
		byte[] previousValue = result.getValue(rowLogColumnFamily, qualifier);
		RowLogMessageConsumerExecutionState executionState = RowLogMessageConsumerExecutionState.fromBytes(previousValue);
		byte[] previousLock = executionState.getLock(consumerId);
		long now = System.currentTimeMillis();
		if (previousLock == null) {
			return putLock(message, consumerId, rowKey, qualifier, previousValue, executionState, now, count);
		} else {
			long previousTimestamp = Bytes.toLong(previousLock);
			if (previousTimestamp + lockTimeout < now) {
				return putLock(message, consumerId, rowKey, qualifier, previousValue, executionState, now, count);
			} else {
				return null;
			}
		}
	}

	private byte[] putLock(RowLogMessage message, int consumerId, byte[] rowKey, byte[] qualifier, byte[] previousValue,
            RowLogMessageConsumerExecutionState executionState, long now, int count) throws IOException {
	    byte[] lock = Bytes.toBytes(now);
		executionState.setLock(consumerId, lock);
	    Put put = new Put(rowKey);
	    put.add(rowLogColumnFamily, qualifier, executionState.toBytes());
	    if (!rowTable.checkAndPut(rowKey, rowLogColumnFamily, qualifier, previousValue, put)) {
	    	return lockMessage(message, consumerId, count+1); // Retry
	    } else {
	    	return lock;
	    }
    }
	
	public boolean unLock(RowLogMessage message, int consumerId, byte[] lock) throws IOException {
		byte[] rowKey = message.getRowKey();
		long seqnr = message.getSeqNr();
		byte[] qualifier = Bytes.toBytes(seqnr);
		Get get = new Get(rowKey);
		get.addColumn(rowLogColumnFamily, qualifier);
		Result result = rowTable.get(get);
		if (result.isEmpty()) return false; // The execution state does not exist anymore, thus no lock to unlock
		
		byte[] previousValue = result.getValue(rowLogColumnFamily, qualifier);
		RowLogMessageConsumerExecutionState executionState = RowLogMessageConsumerExecutionState.fromBytes(previousValue);
		byte[] previousLock = executionState.getLock(consumerId);
		if (!Bytes.equals(lock, previousLock)) return false; // The lock was lost  
		
		executionState.setLock(consumerId, null);
		Put put = new Put(rowKey);
		put.add(rowLogColumnFamily, qualifier, executionState.toBytes());
		return rowTable.checkAndPut(rowKey, rowLogColumnFamily, qualifier, previousValue, put); 
	}
	
	public boolean isLocked(RowLogMessage message, int consumerId) throws IOException {
		byte[] rowKey = message.getRowKey();
		long seqnr = message.getSeqNr();
		byte[] qualifier = Bytes.toBytes(seqnr);
		Get get = new Get(rowKey);
		get.addColumn(rowLogColumnFamily, qualifier);
		Result result = rowTable.get(get);
		if (result.isEmpty()) return false;
		
		RowLogMessageConsumerExecutionState executionState = RowLogMessageConsumerExecutionState.fromBytes(result.getValue(rowLogColumnFamily, qualifier));
		byte[] lock = executionState.getLock(consumerId);
		if (lock == null) return false;
	
		return (Bytes.toLong(lock) + lockTimeout > System.currentTimeMillis());
	}
	
	public boolean messageDone(RowLogMessage message, int consumerId, byte[] lock) throws IOException, RowLogException {
		return messageDone(message, consumerId, lock, 0);
	}
	
	private boolean messageDone(RowLogMessage message, int consumerId, byte[] lock, int count) throws IOException, RowLogException {
		if (count >= 10) {
			return false;
		}
		RowLogShard shard = getShard(); // Fail fast if no shards are registered
		byte[] rowKey = message.getRowKey();
		long seqnr = message.getSeqNr();
		byte[] qualifier = Bytes.toBytes(seqnr);
		Get get = new Get(rowKey);
		get.addColumn(rowLogColumnFamily, qualifier);
		Result result = rowTable.get(get);
		if (!result.isEmpty()) {
			byte[] previousValue = result.getValue(rowLogColumnFamily, qualifier);
			RowLogMessageConsumerExecutionState executionState = RowLogMessageConsumerExecutionState.fromBytes(previousValue);
			if (!Bytes.equals(lock,executionState.getLock(consumerId))) {
				return false; // Not owning the lock
			}
			executionState.setState(consumerId, true);
			executionState.setLock(consumerId, null);
			if (executionState.allDone()) {
				removeExecutionStateAndPayload(rowKey, qualifier);
			} else {
				if (!updateExecutionState(rowKey, qualifier, executionState, previousValue)) {
					return messageDone(message, consumerId, lock, count+1);
				}
			}
		}
		shard.removeMessage(message, consumerId);
		return true;
	}

	private boolean updateExecutionState(byte[] rowKey, byte[] qualifier, RowLogMessageConsumerExecutionState executionState, byte[] previousValue) throws IOException {
	    Put put = new Put(rowKey);
	    put.add(rowLogColumnFamily, qualifier, executionState.toBytes());
	    return rowTable.checkAndPut(rowKey, rowLogColumnFamily, qualifier, previousValue, put);
    }

	private void removeExecutionStateAndPayload(byte[] rowKey, byte[] qualifier) throws IOException {
		// TODO use checkAndDelete
	    Delete delete = new Delete(rowKey); 
	    delete.deleteColumn(rowLogColumnFamily, qualifier);
	    delete.deleteColumn(payloadColumnFamily, qualifier);
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
