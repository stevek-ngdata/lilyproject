package org.lilycms.rowlog.impl;

import java.io.IOException;
import java.util.Arrays;

import org.lilycms.rowlog.api.RowLog;
import org.lilycms.rowlog.api.RowLogMessage;

public class RowLogMessageImpl implements RowLogMessage {

	private final byte[] rowKey;
	private final long seqnr;
	private final byte[] data;
	private final RowLog rowLog;
	private final byte[] id;

	public RowLogMessageImpl(byte[] id, byte[] rowKey, long seqnr, byte[] data, RowLog rowLog) {
		this.id = id;
		this.rowKey = rowKey;
		this.seqnr = seqnr;
		this.data = data;
		this.rowLog = rowLog;
    }
	
	public byte[] getId() {
		return id;
	}
	
	public byte[] getData() {
	    return data;
    }

	public byte[] getPayload() throws IOException {
		return rowLog.getPayload(rowKey, seqnr);
    }

	public byte[] getRowKey() {
	    return rowKey;
    }

	public long getSeqNr() {
		return seqnr;
    }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(data);
		result = prime * result + Arrays.hashCode(id);
		result = prime * result + Arrays.hashCode(rowKey);
		result = prime * result + (int) (seqnr ^ (seqnr >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		RowLogMessageImpl other = (RowLogMessageImpl) obj;
		if (!Arrays.equals(data, other.data))
			return false;
		if (!Arrays.equals(id, other.id))
			return false;
		if (!Arrays.equals(rowKey, other.rowKey))
			return false;
		if (seqnr != other.seqnr)
			return false;
		return true;
	}
	
}
