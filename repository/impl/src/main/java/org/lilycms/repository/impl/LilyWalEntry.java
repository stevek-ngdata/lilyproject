package org.lilycms.repository.impl;

import org.apache.hadoop.hbase.util.Bytes;
import org.lilycms.repoutil.RecordEvent.Type;
import org.lilycms.util.ArgumentValidator;
import org.lilycms.wal.api.WalEntry;

public class LilyWalEntry implements WalEntry {

	private final Type eventType;

	public static LilyWalEntry fromBytes(byte[] bytes) {
		return new LilyWalEntry(Type.valueOf(Bytes.toString(bytes)));
	}
	
	public LilyWalEntry(Type eventType) {
		ArgumentValidator.notNull(eventType, "eventType");
		this.eventType = eventType;
		
    }
	
	public byte[] toBytes() {
		return Bytes.toBytes(eventType.name());
	}

}
