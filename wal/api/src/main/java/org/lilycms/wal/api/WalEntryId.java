package org.lilycms.wal.api;

import java.util.Arrays;
import org.apache.hadoop.hbase.util.Bytes;

public class WalEntryId {

	private final long timestamp;
	private final byte[] sourceId;

	
	public WalEntryId(long timestamp, byte[] sourceId) {
		this.timestamp = timestamp;
		this.sourceId = sourceId;
    }

	public long getTimestamp() {
    	return timestamp;
    }

	public byte[] getSourceId() {
    	return sourceId;
    }

	public static WalEntryId fromBytes(byte[] bytes) {
		return new WalEntryId(Bytes.toLong(bytes), Bytes.tail(bytes, bytes.length - Bytes.SIZEOF_LONG));
	}
	
	public byte[] toBytes() {
		return Bytes.add(Bytes.toBytes(timestamp), sourceId);
	}

	@Override
    public int hashCode() {
	    final int prime = 31;
	    int result = 1;
	    result = prime * result + Arrays.hashCode(sourceId);
	    result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
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
	    WalEntryId other = (WalEntryId) obj;
	    if (!Arrays.equals(sourceId, other.sourceId))
		    return false;
	    if (timestamp != other.timestamp)
		    return false;
	    return true;
    }
	
}
