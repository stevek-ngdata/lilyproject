package org.lilycms.rowlog.impl;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.util.Bytes;
import org.lilycms.rowlog.api.RowLog;
import org.lilycms.rowlog.api.RowLogMessage;
import org.lilycms.util.ArgumentValidator;

public class RowLogMessageImpl implements RowLogMessage {

	private final byte[] rowKey;
	private final long seqnr;
	private final byte[] data;
	private final RowLog rowLog;

	public RowLogMessageImpl(byte[] rowKey, long seqnr, byte[] data, RowLog rowLog) {
		ArgumentValidator.notNull(rowKey, "rowKey");
		this.rowKey = rowKey;
		this.seqnr = seqnr;
		this.data = data;
		this.rowLog = rowLog;
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

	public byte[] toBytes() {
		byte[] bytes = Bytes.toBytes(seqnr);
		bytes = Bytes.add(bytes, Bytes.toBytes(rowKey.length));
		bytes = Bytes.add(bytes, rowKey);
		if (data != null) {
			bytes = Bytes.add(bytes, data);
		}
		return bytes;
    }
	
	public static RowLogMessage fromBytes(byte[] bytes, RowLog rowLog) {
		long seqnr = Bytes.toLong(bytes);
		int rowKeyLength = Bytes.toInt(bytes,Bytes.SIZEOF_LONG);
		byte[] rowKey = Bytes.head(Bytes.tail(bytes, bytes.length-Bytes.SIZEOF_LONG-Bytes.SIZEOF_INT), rowKeyLength);
		int dataLength = bytes.length - Bytes.SIZEOF_LONG - Bytes.SIZEOF_INT - rowKeyLength;
		byte[] data = null;
		if (dataLength > 0) {
			data = Bytes.tail(bytes, dataLength);
		}
		return new RowLogMessageImpl(rowKey, seqnr, data, rowLog);
	}

	@Override
    public int hashCode() {
	    final int prime = 31;
	    int result = 1;
	    result = prime * result + Arrays.hashCode(data);
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
	    if (!Arrays.equals(rowKey, other.rowKey))
		    return false;
	    if (seqnr != other.seqnr)
		    return false;
	    return true;
    }
}
