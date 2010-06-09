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
import org.apache.hadoop.hbase.client.RowLock;
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

	public void messageDone(byte[] id, RowLogMessage message, int consumerId, RowLock rowLock) throws IOException, RowLogException {
		RowLogShard shard = getShard(); // Fail fast if no shards are registered
	        
		RowLock lock = rowLock;
		boolean maintainOwnLock = false;
		try {
			byte[] rowKey = message.getRowKey();
			if (lock == null) {
				maintainOwnLock = true;
				lock = rowTable.lockRow(rowKey); 
			}
			
	        Get get = new Get(rowKey, lock);
			byte[] messageColumn = Bytes.toBytes(message.getSeqNr());
			get.addColumn(rowLogColumnFamily, messageColumn);
			Result result = rowTable.get(get);
			if (!result.isEmpty()) {
				RowLogMessageConsumerExecutionState executionState = RowLogMessageConsumerExecutionState.fromBytes(result.getValue(rowLogColumnFamily, messageColumn));
				executionState.setConsumerState(consumerId, true);
				if (executionState.allDone()) {
					removeExecutionState(rowKey, messageColumn, lock);
				} else {
					updateExecutionState(rowKey, messageColumn, executionState, lock);
				}
			}
			shard.removeMessage(id, consumerId);
		} finally {
			if (maintainOwnLock) {
				if (lock != null) {
					rowTable.unlockRow(lock);
				}
			}
		}
	}

	public byte[] putMessage(RowLogMessage message, Put put, RowLock rowLock) throws RowLogException {
		
		RowLogShard shard = getShard(); // Fail fast if no shards are registered
		byte[] messageId = Bytes.toBytes(System.currentTimeMillis()); 
		messageId = Bytes.add(messageId, Bytes.toBytes(message.getSeqNr()));
		messageId = Bytes.add(messageId, message.getRowKey());
		try {
	        initializeConsumers(message.getRowKey(), message.getSeqNr(), messageId, put, rowLock);
	        for (RowLogMessageConsumer consumer : consumers) {
	        	shard.putMessage(messageId, consumer.getId(), message);
	        }
        } catch (IOException e) {
	        throw new RowLogException("Failed to put message <" + message.toString() + "> on row log", e);
        }
		return messageId;
	}
	
	private void initializeConsumers(byte[] rowKey, long seqnr, byte[] messageId, Put put, RowLock rowLock) throws IOException {
		RowLock lock = rowLock;
		boolean maintainOwnLock = false;
		try {
			if (lock == null) {
				maintainOwnLock = true;
				lock = rowTable.lockRow(rowKey); 
			}
			RowLogMessageConsumerExecutionState executionState = new RowLogMessageConsumerExecutionState(messageId);
			for (RowLogMessageConsumer consumer : consumers) {
				executionState.setConsumerState(consumer.getId(), false);
			}
			if (put != null) {
				put.add(rowLogColumnFamily, Bytes.toBytes(seqnr), executionState.toBytes());
			} else {
				put = new Put(rowKey, lock);
				put.add(rowLogColumnFamily, Bytes.toBytes(seqnr), executionState.toBytes());
				rowTable.put(put);
			}
		} finally {
			if (maintainOwnLock) {
				if (lock != null) {
					rowTable.unlockRow(lock);
				}
			}
		}
	}

	public long putPayload(byte[] rowKey, byte[] data, Put put, RowLock rowLock) throws IOException {
		RowLock lock = rowLock;
		boolean maintainOwnLock = false;
		try {
			if (lock == null) {
				maintainOwnLock = true;
				lock = rowTable.lockRow(rowKey);
			}
			Get get = new Get(rowKey, lock);
	    	get.addColumn(payloadColumnFamily, SEQ_NR);
	    	Result result = rowTable.get(get);
	    	byte[] value = result.getValue(payloadColumnFamily, SEQ_NR);
	    	long seqnr = -1;
	    	if (value != null) {
	    		seqnr = Bytes.toLong(value);
	    	}
	    	seqnr++;
	    	if (put != null) {
	    		put.add(payloadColumnFamily, Bytes.toBytes(seqnr), data);
	    	} else {
	    		put = new Put(rowKey, lock);
	    		put.add(payloadColumnFamily, Bytes.toBytes(seqnr), data);
	    		rowTable.put(put);
	    	}
	    	return seqnr;
		} finally {
			if (maintainOwnLock) {
				if (lock != null) {
					rowTable.unlockRow(lock);
				}
			}
		}
	}

	public byte[] getPayload(byte[] rowKey, long seqnr) throws IOException {
		Get get = new Get(rowKey);
		get.addColumn(payloadColumnFamily, Bytes.toBytes(seqnr));
		Result result = rowTable.get(get);
		return result.getValue(payloadColumnFamily, Bytes.toBytes(seqnr));
	}
	
	public boolean processMessage(byte[] messageId, RowLogMessage message, RowLock rowLock) throws IOException {
		RowLock lock = rowLock;
		boolean maintainOwnLock = false;
		try {
			byte[] rowKey = message.getRowKey();
			if (lock == null) {
				maintainOwnLock = true;
				lock = rowTable.lockRow(rowKey);
			}
			Get get = new Get(rowKey, lock);
			byte[] messageColumn = Bytes.toBytes(message.getSeqNr());
			get.addColumn(rowLogColumnFamily, messageColumn);
			Result result = rowTable.get(get);
			RowLogMessageConsumerExecutionState executionState = RowLogMessageConsumerExecutionState.fromBytes(result.getValue(rowLogColumnFamily, messageColumn));
			
			boolean allDone = processMessage(messageId, message, executionState, lock);
			
			if (allDone) {
				removeExecutionState(rowKey, messageColumn, lock);
			} else {
				updateExecutionState(rowKey, messageColumn, executionState, lock);
			}
			return allDone;
		} finally {
			if (maintainOwnLock) {
				if (lock != null) {
					rowTable.unlockRow(lock);
				}
			}
		}
	}

	private boolean processMessage(byte[] messageId, RowLogMessage message, RowLogMessageConsumerExecutionState executionState, RowLock rowLock) throws IOException {
		boolean allDone = true;
		for (RowLogMessageConsumer consumer : consumers) {
			if (!executionState.getConsumerState(consumer.getId())) {
				boolean done = consumer.processMessage(message, rowLock);
				if (!done) {
					allDone = false;
				} else {
					shard.removeMessage(messageId, consumer.getId());
				}
				executionState.setConsumerState(consumer.getId(), done);
			}
		}
		return allDone;
	}

	private void updateExecutionState(byte[] rowKey, byte[] messageColumn, RowLogMessageConsumerExecutionState executionState, RowLock rowLock) throws IOException {
	    Put put = new Put(rowKey, rowLock);
	    put.add(rowLogColumnFamily, messageColumn, executionState.toBytes());
	    rowTable.put(put);
    }

	private void removeExecutionState(byte[] rowKey, byte[] messageColumn, RowLock rowLock) throws IOException {
	    Delete delete = new Delete(rowKey, Long.MAX_VALUE, rowLock); // The timestamp in the delete is of no meaning here since we delete a column, not a whole row, see HBase API
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
