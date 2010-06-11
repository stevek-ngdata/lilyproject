package org.lilycms.rowlog.api;


public interface RowLogMessageConsumer {

	int getId();

	boolean processMessage(RowLogMessage message);

}
