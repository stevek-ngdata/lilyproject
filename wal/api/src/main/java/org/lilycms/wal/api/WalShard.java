package org.lilycms.wal.api;

import java.util.List;

public interface WalShard {

	void putEntry(WalEntryId id, WalEntry entry) throws WalException;
	void entryFinished(WalEntryId id) throws WalException;
	List<WalEntry> getEntries(long timestamp) throws WalException;
}
