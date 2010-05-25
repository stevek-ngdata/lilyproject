package org.lilycms.wal.impl.test;

import org.lilycms.wal.api.WalEntry;
import org.lilycms.wal.api.WalEntryFactory;

public class DummyWalEntryFactory implements WalEntryFactory {

	public WalEntry fromBytes(byte[] bytes) {
		return new DummyEntry(bytes);
	}

}
