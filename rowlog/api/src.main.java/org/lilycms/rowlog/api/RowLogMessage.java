package org.lilycms.rowlog.api;

import java.io.IOException;

public interface RowLogMessage {
	byte[] getRowKey();
	long getSeqNr();
	byte[] getData();
	byte[] getPayload() throws IOException;
	byte[] toBytes();
}
