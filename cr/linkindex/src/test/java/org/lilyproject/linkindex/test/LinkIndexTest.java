/*
 * Copyright 2010 Outerthought bvba
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lilyproject.linkindex.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.*;

import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilyproject.hbaseindex.IndexManager;
import org.lilyproject.linkindex.FieldedLink;
import org.lilyproject.linkindex.LinkIndex;
import org.lilyproject.linkindex.LinkIndexUpdater;
import org.lilyproject.repository.api.*;
import org.lilyproject.repository.impl.*;
import org.lilyproject.rowlock.HBaseRowLocker;
import org.lilyproject.rowlock.RowLocker;
import org.lilyproject.rowlog.api.RowLog;
import org.lilyproject.rowlog.api.RowLogConfig;
import org.lilyproject.rowlog.api.RowLogConfigurationManager;
import org.lilyproject.rowlog.api.RowLogMessageListenerMapping;
import org.lilyproject.rowlog.api.RowLogShard;
import org.lilyproject.rowlog.api.RowLogSubscription;
import org.lilyproject.rowlog.impl.RowLogConfigurationManagerImpl;
import org.lilyproject.rowlog.impl.RowLogImpl;
import org.lilyproject.rowlog.impl.RowLogShardImpl;
import org.lilyproject.testfw.HBaseProxy;
import org.lilyproject.testfw.TestHelper;
import org.lilyproject.util.hbase.HBaseTableFactory;
import org.lilyproject.util.hbase.HBaseTableFactoryImpl;
import org.lilyproject.util.hbase.LilyHBaseSchema;
import org.lilyproject.util.hbase.LilyHBaseSchema.RecordCf;
import org.lilyproject.util.hbase.LilyHBaseSchema.RecordColumn;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.repo.VersionTag;
import org.lilyproject.util.zookeeper.ZkUtil;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

public class LinkIndexTest {
    private final static HBaseProxy HBASE_PROXY = new HBaseProxy();
    private static ZooKeeperItf zk;
    private static RowLogConfigurationManager rowLogConfMgr;

    private static TypeManager typeManager;
    private static HBaseRepository repository;
    private static IdGenerator ids;
    private static LinkIndex linkIndex;
    private static HBaseTableFactory hbaseTableFactory;

    private SchemaId field1 = new SchemaIdImpl(UUID.randomUUID());

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging("org.lilyproject.linkindex", "org.lilyproject.rowlog.impl.RowLogImpl");

        HBASE_PROXY.start();
        zk = ZkUtil.connect(HBASE_PROXY.getZkConnectString(), 10000);

        IndexManager.createIndexMetaTableIfNotExists(HBASE_PROXY.getConf());

        IdGenerator idGenerator = new IdGeneratorImpl();
        hbaseTableFactory = new HBaseTableFactoryImpl(HBASE_PROXY.getConf());
        typeManager = new HBaseTypeManager(idGenerator, HBASE_PROXY.getConf(), zk, hbaseTableFactory);

        BlobStoreAccess dfsBlobStoreAccess = new DFSBlobStoreAccess(HBASE_PROXY.getBlobFS(), new Path("/lily/blobs"));
        List<BlobStoreAccess> blobStoreAccesses = Arrays.asList(new BlobStoreAccess[]{dfsBlobStoreAccess});
        BlobStoreAccessConfig blobStoreAccessConfig = new BlobStoreAccessConfig(dfsBlobStoreAccess.getId());
        SizeBasedBlobStoreAccessFactory blobStoreAccessFactory = new SizeBasedBlobStoreAccessFactory(blobStoreAccesses, blobStoreAccessConfig);
        BlobManager blobManager = new BlobManagerImpl(hbaseTableFactory, blobStoreAccessFactory, false);

        rowLogConfMgr = new RowLogConfigurationManagerImpl(zk);
        rowLogConfMgr.addRowLog("WAL", new RowLogConfig(10000L, true, false, 0L, 5000L, 5000L));
        
        RowLocker rowLocker = new HBaseRowLocker(LilyHBaseSchema.getRecordTable(hbaseTableFactory), RecordCf.DATA.bytes, RecordColumn.LOCK.bytes, 10000);
        RowLog wal = new RowLogImpl("WAL", LilyHBaseSchema.getRecordTable(hbaseTableFactory),
                RecordCf.ROWLOG.bytes, RecordColumn.WAL_PREFIX, rowLogConfMgr, rowLocker);
        RowLogShard walShard = new RowLogShardImpl("WS1", HBASE_PROXY.getConf(), wal, 100);
        wal.registerShard(walShard);
        repository = new HBaseRepository(typeManager, idGenerator, wal, hbaseTableFactory, blobManager, rowLocker);
        ids = repository.getIdGenerator();
        IndexManager indexManager = new IndexManager(HBASE_PROXY.getConf());

        LinkIndex.createIndexes(indexManager);
        linkIndex = new LinkIndex(indexManager, repository);

        rowLogConfMgr.addSubscription("WAL", "LinkIndexUpdater", RowLogSubscription.Type.VM, 1);
        RowLogMessageListenerMapping.INSTANCE.put("LinkIndexUpdater", new LinkIndexUpdater(repository, linkIndex));

        waitForSubscription(wal, "LinkIndexUpdater");
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        Closer.close(typeManager);
        Closer.close(repository);
        Closer.close(rowLogConfMgr);
        Closer.close(zk);
        HBASE_PROXY.stop();
    }

    @Test
    public void testLinkIndex() throws Exception {
        SchemaId liveTag = repository.getIdGenerator().getSchemaId(UUID.randomUUID());
        
        Set<FieldedLink> links1 = new HashSet<FieldedLink>();
        links1.add(new FieldedLink(ids.newRecordId("id1"), field1));
        links1.add(new FieldedLink(ids.newRecordId("id2"), field1));

        Set<FieldedLink> links2 = new HashSet<FieldedLink>();
        links2.add(new FieldedLink(ids.newRecordId("id3"), field1));
        links2.add(new FieldedLink(ids.newRecordId("id4"), field1));

        linkIndex.updateLinks(ids.newRecordId("idA"), liveTag, links1);
        linkIndex.updateLinks(ids.newRecordId("idB"), liveTag, links1);
        linkIndex.updateLinks(ids.newRecordId("idC"), liveTag, links2);

        // Test forward link retrieval
        Set<FieldedLink> links = linkIndex.getForwardLinks(ids.newRecordId("idA"), liveTag);
        assertTrue(links.contains(new FieldedLink(ids.newRecordId("id1"), field1)));
        assertTrue(links.contains(new FieldedLink(ids.newRecordId("id2"), field1)));
        assertEquals(2, links.size());

        // Test backward link retrieval
        Set<RecordId> referrers = linkIndex.getReferrers(ids.newRecordId("id1"), liveTag);
        assertTrue(referrers.contains(ids.newRecordId("idA")));
        assertTrue(referrers.contains(ids.newRecordId("idB")));
        assertEquals(2, referrers.size());

        // Update the links for record idA and re-check
        links1.add(new FieldedLink(ids.newRecordId("id2a"), field1));
        linkIndex.updateLinks(ids.newRecordId("idA"), liveTag, links1);

        links = linkIndex.getForwardLinks(ids.newRecordId("idA"), liveTag);
        assertTrue(links.contains(new FieldedLink(ids.newRecordId("id1"), field1)));
        assertTrue(links.contains(new FieldedLink(ids.newRecordId("id2"), field1)));
        assertTrue(links.contains(new FieldedLink(ids.newRecordId("id2a"), field1)));
        assertEquals(3, links.size());

        referrers = linkIndex.getReferrers(ids.newRecordId("id1"), liveTag);
        assertTrue(referrers.contains(ids.newRecordId("idA")));
        assertTrue(referrers.contains(ids.newRecordId("idB")));
        assertEquals(2, referrers.size());

        referrers = linkIndex.getReferrers(ids.newRecordId("id2a"), liveTag);
        assertTrue(referrers.contains(ids.newRecordId("idA")));
        assertEquals(1, referrers.size());
    }

    @Test
    public void testLinkIndexUpdater() throws Exception {
        FieldType nonVersionedFt = typeManager.newFieldType(typeManager.getValueType("LINK", false, false),
                new QName("ns", "link1"), Scope.NON_VERSIONED);
        nonVersionedFt = typeManager.createFieldType(nonVersionedFt);

        FieldType versionedFt = typeManager.newFieldType(typeManager.getValueType("LINK", true, false),
                new QName("ns", "link2"), Scope.VERSIONED);
        versionedFt = typeManager.createFieldType(versionedFt);

        FieldType versionedMutableFt = typeManager.newFieldType(typeManager.getValueType("LINK", true, false),
                new QName("ns", "link3"), Scope.VERSIONED_MUTABLE);
        versionedMutableFt = typeManager.createFieldType(versionedMutableFt);

        RecordType recordType = typeManager.newRecordType(new QName("ns", "MyRecordType"));
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(nonVersionedFt.getId(), false));
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(versionedFt.getId(), false));
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(versionedMutableFt.getId(), false));
        recordType = typeManager.createRecordType(recordType);

        //
        // Link extraction from a record without versions
        //
        {
            Record record = repository.newRecord();
            record.setRecordType(recordType.getName());
            record.setField(nonVersionedFt.getName(), new Link(ids.newRecordId("foo1")));
            record = repository.create(record);

            Set<RecordId> referrers = linkIndex.getReferrers(ids.newRecordId("foo1"), VersionTag.VERSIONLESS_TAG);
            assertEquals(1, referrers.size());
            assertTrue(referrers.contains(record.getId()));

            referrers = linkIndex.getReferrers(ids.newRecordId("bar1"), VersionTag.VERSIONLESS_TAG);
            assertEquals(0, referrers.size());

            // Now perform an update so that there is a version
            record.setField(versionedFt.getName(), Arrays.asList(new Link(ids.newRecordId("foo2")), new Link(ids.newRecordId("foo3"))));
            record = repository.update(record);

//            recordEvent = new RecordEvent();
//            recordEvent.setVersionCreated(record.getVersion());
//            recordEvent.addUpdatedField(versionedFt.getId());
//            message = new TestQueueMessage(EventType.EVENT_RECORD_UPDATED, record.getId(), recordEvent.toJsonBytes());
//            queue.broadCastMessage(message);
//
//            // Since there is a version but no vtag yet, there should be nothing in the link index for this record
//            referrers = linkIndex.getReferrers(ids.newRecordId("foo1"), VersionTag.VERSIONLESS_TAG);
//            assertEquals(0, referrers.size());
//
//            assertEquals(0, linkIndex.getAllForwardLinks(record.getId()).size());

        }
    }

    public static void waitForSubscription(RowLog rowLog, String subscriptionId) throws InterruptedException {
        boolean subscriptionKnown = false;
        int timeOut = 10000;
        long waitUntil = System.currentTimeMillis() + 10000;
        while (!subscriptionKnown && System.currentTimeMillis() < waitUntil) {
            for (RowLogSubscription subscription : rowLog.getSubscriptions()) {
                if (subscriptionId.equals(subscription.getId())) {
                    subscriptionKnown = true;
                    break;
                }
            }
            Thread.sleep(10);
        }
        Assert.assertTrue("Subscription '" + subscriptionId + "' not known to rowlog within timeout " + timeOut + "ms",
                subscriptionKnown);
    }
}
