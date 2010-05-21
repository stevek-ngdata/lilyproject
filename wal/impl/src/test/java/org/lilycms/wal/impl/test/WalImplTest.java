package org.lilycms.wal.impl.test;


import static org.junit.Assert.fail;

import org.apache.hadoop.hbase.util.Bytes;
import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilycms.wal.api.Wal;
import org.lilycms.wal.api.WalEntry;
import org.lilycms.wal.api.WalEntryId;
import org.lilycms.wal.api.WalException;
import org.lilycms.wal.api.WalShard;
import org.lilycms.wal.impl.WalImpl;

public class WalImplTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	
	@Test
	public void testNoShard() {
		WalEntry entry = new DummyEntry(Bytes.toBytes("dummyBytes"));
		Wal wal = new WalImpl();
		byte[] sourceId = Bytes.toBytes("sourceId");
		try {
			wal.putEntry(sourceId, entry);
			fail();
		} catch (WalException expected) {
		}
		try {
			wal.entryFinished(new WalEntryId(1, sourceId));
			fail();
		} catch (WalException expected) {
		}
	}
	
	@Test
	public void testEntry() throws Exception {
		IMocksControl control = EasyMock.createControl();
		WalEntry entry = new DummyEntry(Bytes.toBytes("dummyBytes"));
		Wal wal = new WalImpl();
		WalShard shard = control.createMock(WalShard.class);
		shard.putEntry(EasyMock.isA(WalEntryId.class), EasyMock.eq(entry));
		shard.entryFinished(EasyMock.isA(WalEntryId.class));
		
		control.replay();
		wal.registerShard(shard);
		// TODO work with a mocked system clock to be able to control the creation of the WalEntryId
		WalEntryId walEntryId = wal.putEntry(Bytes.toBytes("sourceId"), entry);
		wal.entryFinished(walEntryId);
		control.verify();
	}
}
