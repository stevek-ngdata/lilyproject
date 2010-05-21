package org.lilycms.wal.api;

public interface Wal {

	WalEntryId putEntry(byte[] sourceId, WalEntry entry) throws WalException;
	void entryFinished(WalEntryId id) throws WalException;
	void registerShard(WalShard shard);
	
}
