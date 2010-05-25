package org.lilycms.wal.impl.test;


import static org.junit.Assert.*;

import java.util.List;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilycms.testfw.TestHelper;
import org.lilycms.wal.api.WalEntry;
import org.lilycms.wal.api.WalEntryId;
import org.lilycms.wal.api.WalShard;
import org.lilycms.wal.impl.WalShardImpl;

public class WalShardImplTest {

    private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
	private static WalShard walShard;
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging();
        TEST_UTIL.startMiniCluster(1);
        walShard = new WalShardImpl("TestWalShard", TEST_UTIL.getConfiguration(), new DummyWalEntryFactory());
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        TEST_UTIL.shutdownMiniCluster();
    }


	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testEntry() throws Exception {
		WalEntryId id = new WalEntryId(1L, Bytes.toBytes("sourceId"));
		WalEntry entry = new DummyEntry(Bytes.toBytes("dummyBytes"));
		walShard.putEntry(id, entry);
		WalEntry actualEntry = walShard.getEntry(id);
		assertEquals(entry, actualEntry);
		walShard.entryFinished(id);
		assertNull(walShard.getEntry(id));
	}
	
	@Test
	public void testFinishNonExistingEntry() throws Exception {
		WalEntryId id = new WalEntryId(Long.MAX_VALUE, Bytes.toBytes("sourceId"));
		assertNull(walShard.getEntry(id));
		walShard.entryFinished(id);
		assertNull(walShard.getEntry(id));
	}
	
	@Test
	public void testGetEntries() throws Exception {
		// Start with an empty shard
		List<WalEntry> entries = walShard.getEntries(Long.MAX_VALUE);
		assertTrue(entries.isEmpty());
		
		WalEntryId id1 = new WalEntryId(1L, Bytes.toBytes("sourceId"));
		WalEntry entry1 = new DummyEntry(Bytes.toBytes("dummyBytes"));
		walShard.putEntry(id1, entry1);
		WalEntryId id2 = new WalEntryId(10L, Bytes.toBytes("sourceId"));
		WalEntry entry2 = new DummyEntry(Bytes.toBytes("dummyBytes2"));
		walShard.putEntry(id2, entry2);

		// timestamp is exclusive
		entries = walShard.getEntries(1L);
		assertTrue(entries.isEmpty());

		// get list of entries before timestamp
		entries = walShard.getEntries(10L);
		assertEquals(1, entries.size());
		assertEquals(entry1, entries.get(0));
		
		entries = walShard.getEntries(11L);
		assertEquals(2, entries.size());
		assertTrue(entries.contains(entry1));
		assertTrue(entries.contains(entry2));
		
		// finish entry removes entries from the table
		walShard.entryFinished(id1);
		entries = walShard.getEntries(2L);
		assertTrue(entries.isEmpty());
	}
}
