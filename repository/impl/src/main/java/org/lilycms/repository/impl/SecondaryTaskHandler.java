package org.lilycms.repository.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilycms.repository.api.RecordId;
import org.lilycms.repository.api.SecondaryTask;
import org.lilycms.wal.api.WalEntryId;

public class SecondaryTaskHandler {

	List<SecondaryTask> secondaryTasks = new ArrayList<SecondaryTask>();

	private final byte[] columnFamily;

	private final HTable recordTable;
	
	public SecondaryTaskHandler(HTable recordTable, byte[] columnFamily) {
		this.recordTable = recordTable;
		this.columnFamily = columnFamily;
    }
	
	public void registerSecondaryTask(SecondaryTask secondaryTask) {
		secondaryTasks.add(secondaryTask);
	}
	
	public void initializeSecondaryTasks(Put recordPut, long seqnr, WalEntryId walEntryId) {
	    SecondaryTasksExecutionState stes = new SecondaryTasksExecutionState(walEntryId);
		for (SecondaryTask secondaryTask : secondaryTasks) {
	        stes.setTaskState(secondaryTask.getId(), secondaryTask.getInitialState());
        }
		recordPut.add(columnFamily, Bytes.toBytes(seqnr), stes.toBytes());
	}
	
	public boolean executeSecondaryTasks(RecordId recordId, long seqnr, RowLock rowLock) throws IOException {
		Get get = new Get(recordId.toBytes(), rowLock);
		byte[] columnName = Bytes.toBytes(seqnr);
		get.addColumn(columnFamily, columnName);
		Result result = recordTable.get(get);
		SecondaryTasksExecutionState stes = SecondaryTasksExecutionState.fromBytes(result.getValue(columnFamily, columnName));
		
		boolean allDone = true;
		for (SecondaryTask secondaryTask : secondaryTasks) {
			if (!secondaryTask.isDone(stes.getTaskState(secondaryTask.getId()))) {
				String executionState = secondaryTask.execute();
				if (!secondaryTask.isDone(executionState)) {
					allDone = false;
				}
				stes.setTaskState(secondaryTask.getId(), executionState);
			}
        }
		
		if (allDone) {
			// The given timestamp is of no meaning here since we delete a column, not a whole row
			Delete delete = new Delete(recordId.toBytes(), Long.MAX_VALUE, rowLock);
			delete.deleteColumn(columnFamily, columnName);
			recordTable.delete(delete);
		} else {
			Put put = new Put(recordId.toBytes(), rowLock);
			put.add(columnFamily, columnName, stes.toBytes());
			recordTable.put(put);
		}
		return allDone;
	}
}
