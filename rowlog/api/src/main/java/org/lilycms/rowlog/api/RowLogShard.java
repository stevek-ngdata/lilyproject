package org.lilycms.rowlog.api;

import java.io.IOException;

public interface RowLogShard {

	void putMessage(RowLogMessage message, int consumerId) throws IOException ;
	void removeMessage(RowLogMessage message, int consumerId) throws IOException ;
	RowLogMessage next(int consumerId) throws IOException ;
}
