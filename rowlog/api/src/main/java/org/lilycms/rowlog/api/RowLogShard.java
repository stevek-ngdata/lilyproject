package org.lilycms.rowlog.api;

import java.io.IOException;
import org.lilycms.util.Pair; 

public interface RowLogShard {

	void putMessage(byte[] messageId, int consumerId, RowLogMessage message) throws IOException ;
	void removeMessage(byte[] messageId, int consumerId) throws IOException ;
	Pair<byte[], RowLogMessage> next(int consumerId) throws IOException ;
}
