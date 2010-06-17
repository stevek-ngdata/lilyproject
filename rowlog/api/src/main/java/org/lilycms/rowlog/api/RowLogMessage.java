package org.lilycms.rowlog.api;


public interface RowLogMessage {
	byte[] getId();
	byte[] getRowKey();
	long getSeqNr();
	byte[] getData();
	byte[] getPayload() throws RowLogException;
}
