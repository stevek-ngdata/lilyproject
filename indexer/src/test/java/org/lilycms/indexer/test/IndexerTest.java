package org.lilycms.indexer.test;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilycms.indexer.Indexer;
import org.lilycms.indexer.conf.IndexerConf;
import org.lilycms.indexer.conf.IndexerConfBuilder;
import org.lilycms.queue.api.QueueMessage;
import org.lilycms.queue.mock.TestLilyQueue;
import org.lilycms.queue.mock.TestQueueMessage;
import org.lilycms.repository.api.*;
import org.lilycms.repository.impl.*;
import org.lilycms.repoutil.EventType;
import org.lilycms.repoutil.RecordEvent;
import org.lilycms.repoutil.VersionTag;
import org.lilycms.testfw.TestHelper;

// To run this test from an IDE, set a property solr.war pointing to the SOLR war

public class IndexerTest {
    private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    private static IndexerConf INDEXER_CONF;
    private static SolrTestingUtility SOLR_TEST_UTIL;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        INDEXER_CONF = IndexerConfBuilder.build(IndexerTest.class.getClassLoader().getResourceAsStream("org/lilycms/indexer/test/indexerconf1.xml"));
        SOLR_TEST_UTIL = new SolrTestingUtility("org/lilycms/indexer/test/schema1.xml");

        TestHelper.setupLogging();
        TEST_UTIL.startMiniCluster(1);
        SOLR_TEST_UTIL.start();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        TEST_UTIL.shutdownMiniCluster();
        if (SOLR_TEST_UTIL != null)
            SOLR_TEST_UTIL.stop();
    }

    @Test
    public void testIndexer() throws Exception {
        IdGenerator idGenerator = new IdGeneratorImpl();
        TypeManager typeManager = new HBaseTypeManager(idGenerator, TEST_UTIL.getConfiguration());
        BlobStoreAccess dfsBlobStoreAccess = new DFSBlobStoreAccess(TEST_UTIL.getDFSCluster().getFileSystem());
        SizeBasedBlobStoreAccessFactory blobStoreAccessFactory = new SizeBasedBlobStoreAccessFactory(dfsBlobStoreAccess);
        Repository repository = new HBaseRepository(typeManager, idGenerator, blobStoreAccessFactory, TEST_UTIL.getConfiguration());
        SolrServer solrServer = SOLR_TEST_UTIL.getSolrServer();
        TestLilyQueue queue = new TestLilyQueue();
        Indexer indexer = new Indexer(INDEXER_CONF, queue, repository, typeManager, solrServer);

        // Create a record type
        ValueType stringValueType = typeManager.getValueType("STRING", false, false);
        QName fieldType1Name = new QName("org.lilycms.indexer.test", "field1");
        FieldType fieldType1 = typeManager.newFieldType(stringValueType, fieldType1Name, Scope.VERSIONED);
        fieldType1 = typeManager.createFieldType(fieldType1);

        ValueType longValueType = typeManager.getValueType("LONG", false, false);
        QName liveTagName = new QName(VersionTag.NS_VTAG, "live");
        FieldType liveTag = typeManager.newFieldType(longValueType, liveTagName, Scope.NON_VERSIONED);
        liveTag = typeManager.createFieldType(liveTag);

        RecordType recordType1 = typeManager.newRecordType("RecordType1");
        recordType1.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType1.getId(), true));
        recordType1.addFieldTypeEntry(typeManager.newFieldTypeEntry(liveTag.getId(), false));
        recordType1 = typeManager.createRecordType(recordType1);

        // Create a document
        Record record = repository.newRecord(idGenerator.newRecordId());
        record.setRecordType("RecordType1", recordType1.getVersion());
        record.setField(fieldType1Name, "apple");
        record.setField(liveTagName, new Long(1));
        repository.create(record);

        // Generate queue message
        // TODO remove this once the real queue exists
        RecordEvent event = new RecordEvent();
        event.setVersionCreated(1);
        event.addUpdatedField(fieldType1.getId());
        QueueMessage message = new TestQueueMessage(EventType.EVENT_RECORD_CREATED, record.getId(), event.toJsonBytes());
        queue.broadCastMessage(message);

        // Make sure all index writes are committed
        solrServer.commit(true, true);

        // Verify the index was updated
        verifyOneResult("field1:apple");

        // The end
        indexer.stop();
    }
    
    private void verifyOneResult(String query) throws SolrServerException {
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.set("q", query);
        QueryResponse response = SOLR_TEST_UTIL.getSolrServer().query(solrQuery);
        assertEquals(1, response.getResults().getNumFound());
    }

}
