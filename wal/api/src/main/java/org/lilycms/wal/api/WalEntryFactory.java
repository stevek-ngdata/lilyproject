package org.lilycms.wal.api;

public interface WalEntryFactory {
	WalEntry fromBytes(byte[] bytes);
}
