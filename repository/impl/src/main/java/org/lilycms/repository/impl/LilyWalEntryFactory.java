package org.lilycms.repository.impl;

import org.lilycms.wal.api.WalEntry;
import org.lilycms.wal.api.WalEntryFactory;

public class LilyWalEntryFactory implements WalEntryFactory {

	public WalEntry fromBytes(byte[] bytes) {
		return LilyWalEntry.fromBytes(bytes);
	}

}
