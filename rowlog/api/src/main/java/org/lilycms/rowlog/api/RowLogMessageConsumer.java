package org.lilycms.rowlog.api;

import org.apache.hadoop.hbase.client.RowLock;

public interface RowLogMessageConsumer {

	int getId();

	boolean processMessage(RowLogMessage message, RowLock rowLock);

}
