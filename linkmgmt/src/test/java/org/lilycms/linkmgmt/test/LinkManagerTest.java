package org.lilycms.linkmgmt.test;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.lilycms.hbaseindex.IndexManager;
import org.lilycms.linkmgmt.FieldedLink;
import org.lilycms.linkmgmt.LinkManager;
import org.lilycms.repository.api.IdGenerator;
import org.lilycms.repository.api.RecordId;
import org.lilycms.repository.api.Repository;
import org.lilycms.repository.api.TypeManager;
import org.lilycms.repository.impl.*;
import org.lilycms.testfw.TestHelper;

import java.util.HashSet;
import java.util.Set;

public class LinkManagerTest {
    private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging();
        TEST_UTIL.startMiniCluster(1);

        IndexManager.createIndexMetaTable(TEST_UTIL.getConfiguration());
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        TEST_UTIL.shutdownMiniCluster();
    }

    @Test
    public void test() throws Exception {
        IdGenerator idGenerator = new IdGeneratorImpl();
        TypeManager typeManager = new HBaseTypeManager(idGenerator, RecordTypeImpl.class, FieldDescriptorImpl.class, TEST_UTIL.getConfiguration());
        Repository repository = new HBaseRepository(typeManager, idGenerator, RecordImpl.class, TEST_UTIL.getConfiguration());
        IdGenerator ids = repository.getIdGenerator();
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        LinkManager.createIndexes(indexManager);
        LinkManager linkMgr = new LinkManager(indexManager, repository);

        Set<FieldedLink> links1 = new HashSet<FieldedLink>();
        links1.add(new FieldedLink(ids.newRecordId("id1"), "field1"));
        links1.add(new FieldedLink(ids.newRecordId("id2"), "field1"));

        Set<FieldedLink> links2 = new HashSet<FieldedLink>();
        links2.add(new FieldedLink(ids.newRecordId("id3"), "field1"));
        links2.add(new FieldedLink(ids.newRecordId("id4"), "field1"));

        linkMgr.updateLinks(ids.newRecordId("idA"), "live", links1);
        linkMgr.updateLinks(ids.newRecordId("idB"), "live", links1);
        linkMgr.updateLinks(ids.newRecordId("idC"), "live", links2);

        // Test forward link retrieval
        Set<FieldedLink> links = linkMgr.getFieldedLinks(ids.newRecordId("idA"), "live");
        assertTrue(links.contains(new FieldedLink(ids.newRecordId("id1"), "field1")));
        assertTrue(links.contains(new FieldedLink(ids.newRecordId("id2"), "field1")));
        assertEquals(2, links.size());

        // Test backward link retrieval
        Set<RecordId> referrers = linkMgr.getReferrers(ids.newRecordId("id1"), "live");
        assertTrue(referrers.contains(ids.newRecordId("idA")));
        assertTrue(referrers.contains(ids.newRecordId("idB")));
        assertEquals(2, referrers.size());

        // Update the links for record idA and re-check
        links1.add(new FieldedLink(ids.newRecordId("id2a"), "field1"));
        linkMgr.updateLinks(ids.newRecordId("idA"), "live", links1);

        links = linkMgr.getFieldedLinks(ids.newRecordId("idA"), "live");
        assertTrue(links.contains(new FieldedLink(ids.newRecordId("id1"), "field1")));
        assertTrue(links.contains(new FieldedLink(ids.newRecordId("id2"), "field1")));
        assertTrue(links.contains(new FieldedLink(ids.newRecordId("id2a"), "field1")));
        assertEquals(3, links.size());

        referrers = linkMgr.getReferrers(ids.newRecordId("id1"), "live");
        assertTrue(referrers.contains(ids.newRecordId("idA")));
        assertTrue(referrers.contains(ids.newRecordId("idB")));
        assertEquals(2, referrers.size());

        referrers = linkMgr.getReferrers(ids.newRecordId("id2a"), "live");
        assertTrue(referrers.contains(ids.newRecordId("idA")));
        assertEquals(1, referrers.size());
    }
}
