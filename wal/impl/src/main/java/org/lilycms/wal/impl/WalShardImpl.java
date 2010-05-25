package org.lilycms.wal.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilycms.wal.api.WalEntry;
import org.lilycms.wal.api.WalEntryFactory;
import org.lilycms.wal.api.WalEntryId;
import org.lilycms.wal.api.WalException;
import org.lilycms.wal.api.WalShard;

public class WalShardImpl implements WalShard {

	private static final byte[] ENTRY_COLUMN = Bytes.toBytes("entry");
	private static final byte[] WALENTRIES_CF = Bytes.toBytes("WalEntriesCF");
	private HTable shardTable;
	private final WalEntryFactory walEntryFactory;

	public WalShardImpl(String id, Configuration configuration, WalEntryFactory walEntryFactory) throws IOException {
		this.walEntryFactory = walEntryFactory;
		try {
            shardTable = new HTable(configuration, id);
        } catch (IOException e) {
            HBaseAdmin admin = new HBaseAdmin(configuration);
            HTableDescriptor tableDescriptor = new HTableDescriptor(id);
            tableDescriptor.addFamily(new HColumnDescriptor(WALENTRIES_CF));
            admin.createTable(tableDescriptor);
            shardTable = new HTable(configuration, id);
        }
    }
	
	public void entryFinished(WalEntryId id) throws WalException {
		Delete delete = new Delete(id.toBytes());
		try {
	        shardTable.delete(delete);
        } catch (IOException e) {
        	throw new WalException("Failed to finish entry <" + id.toString() + ">" , e);
        }
	}


	public void putEntry(WalEntryId id, WalEntry entry) throws WalException {
		Put put = new Put(id.toBytes());
		put.add(WALENTRIES_CF, ENTRY_COLUMN, entry.toBytes());
		try {
	        shardTable.put(put);
        } catch (IOException e) {
	        throw new WalException("Failed to add entry "+ id.toString() +"to WAL", e);
        }
	}
	
	public WalEntry getEntry(WalEntryId id) throws WalException {
		Get get = new Get(id.toBytes());
		get.addColumn(WALENTRIES_CF, ENTRY_COLUMN);
		Result result;
		try {
			result = shardTable.get(get);
			if (result.isEmpty()) {
				return null;
			}
			return walEntryFactory.fromBytes(result.getValue(WALENTRIES_CF, ENTRY_COLUMN));
		} catch (IOException e) {
			throw new WalException("Failed to get entry " + id.toString() + "from WAL", e);
		}
	}

	public List<WalEntry> getEntries(long timestamp) throws WalException {
		List<WalEntry> entries = new ArrayList<WalEntry>();
		Scan scan = new Scan(Bytes.toBytes(0L), Bytes.toBytes(timestamp));
		try {
	        ResultScanner scanner = shardTable.getScanner(scan);
	        for (Result result : scanner) {
	        	entries.add(walEntryFactory.fromBytes(result.getValue(WALENTRIES_CF, ENTRY_COLUMN)));
            }
        } catch (IOException e) {
        	throw new WalException("Failed to get entries for timestamp <" + timestamp + ">", e);
        }
        return entries;
	}

}
