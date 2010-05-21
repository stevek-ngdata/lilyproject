package org.lilycms.wal.impl;

import java.util.ArrayList;
import java.util.List;

import org.lilycms.wal.api.Wal;
import org.lilycms.wal.api.WalEntry;
import org.lilycms.wal.api.WalEntryId;
import org.lilycms.wal.api.WalException;
import org.lilycms.wal.api.WalShard;

public class WalImpl implements Wal {
	
	List<WalShard> shards = new ArrayList<WalShard>();
	
	public WalImpl() {
    }

	public void entryFinished(WalEntryId id) throws WalException {
		WalShard shard = getShard();
		shard.entryFinished(id);
	}


	public WalEntryId putEntry(byte[] sourceId, WalEntry entry) throws WalException {
		WalShard shard = getShard();
		WalEntryId id = new WalEntryId(System.currentTimeMillis(), sourceId);
		shard.putEntry(id, entry);
		return id;
	}

	public void registerShard(WalShard shard) {
		shards.add(shard);
	}

	// For now we work with only one shard
	private WalShard getShard() throws WalException {
		if (shards.isEmpty()) {
			throw new WalException("No shards registerd");
		}
		WalShard walShard = shards.get(0);
		return walShard;
	}
}
