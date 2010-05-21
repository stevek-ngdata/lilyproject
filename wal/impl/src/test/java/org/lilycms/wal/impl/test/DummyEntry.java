/**
 * 
 */
package org.lilycms.wal.impl.test;

import java.util.Arrays;

import org.lilycms.wal.api.WalEntry;

public class DummyEntry implements WalEntry {

	private byte[] bytes;

	public DummyEntry(byte[] bytes) {
		this.bytes = bytes;
    }
	
	public byte[] toBytes() {
        return bytes;
    }

	@Override
    public int hashCode() {
	    final int prime = 31;
	    int result = 1;
	    result = prime * result + Arrays.hashCode(bytes);
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
	    DummyEntry other = (DummyEntry) obj;
	    if (!Arrays.equals(bytes, other.bytes))
		    return false;
	    return true;
    }
}