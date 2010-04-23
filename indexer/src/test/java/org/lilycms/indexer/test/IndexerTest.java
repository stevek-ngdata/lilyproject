package org.lilycms.indexer.test;

import static org.junit.Assert.assertEquals;
import static org.lilycms.repoutil.EventType.*;

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
        FieldType fieldType1 = typeManager.newFieldType(stringValueType, fieldType1Name, Scope.NON_VERSIONED);
        fieldType1 = typeManager.createFieldType(fieldType1);

        QName fieldType2Name = new QName("org.lilycms.indexer.test", "field2");
        FieldType fieldType2 = typeManager.newFieldType(stringValueType, fieldType2Name, Scope.NON_VERSIONED);
        fieldType2 = typeManager.createFieldType(fieldType2);

        ValueType longValueType = typeManager.getValueType("LONG", false, false);
        QName liveTagName = new QName(VersionTag.NS_VTAG, "live");
        FieldType liveTag = typeManager.newFieldType(longValueType, liveTagName, Scope.NON_VERSIONED);
        liveTag = typeManager.createFieldType(liveTag);

        RecordType recordType1 = typeManager.newRecordType("RecordType1");
        recordType1.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType1.getId(), true));
        recordType1.addFieldTypeEntry(typeManager.newFieldTypeEntry(liveTag.getId(), false));
        recordType1 = typeManager.createRecordType(recordType1);

        //
        // Create a document
        //
        Record record = repository.newRecord(idGenerator.newRecordId());
        record.setRecordType("RecordType1", recordType1.getVersion());
        record.setField(fieldType1Name, "apple");
        repository.create(record);

        // Generate queue message
        RecordEvent event = new RecordEvent();
        event.addUpdatedField(fieldType1.getId());
        QueueMessage message = new TestQueueMessage(EVENT_RECORD_CREATED, record.getId(), event.toJsonBytes());
        queue.broadCastMessage(message);

        solrServer.commit(true, true);

        // Verify the index was updated
        verifyResultCount("field1:apple", 1);

        //
        // Update the document
        //
        record.setField(fieldType1Name, "pear");
        repository.update(record);

        event = new RecordEvent();
        event.addUpdatedField(fieldType1.getId());
        message = new TestQueueMessage(EVENT_RECORD_UPDATED, record.getId(), event.toJsonBytes());
        queue.broadCastMessage(message);

        solrServer.commit(true, true);

        verifyResultCount("field1:pear", 1);
        verifyResultCount("field1:apple", 0);

        // Do as if field2 changed, while field2 is not present in the document.
        // Such situations can occur if the record is modified before earlier events are processed.
        event = new RecordEvent();
        event.addUpdatedField(fieldType2.getId());
        message = new TestQueueMessage(EVENT_RECORD_UPDATED, record.getId(), event.toJsonBytes());
        queue.broadCastMessage(message);

        solrServer.commit(true, true);

        verifyResultCount("field1:pear", 1);
        verifyResultCount("field1:apple", 0);

        // Add a vtag field. For versionless records, this should have no effect
        record.setField(liveTagName, new Long(1));
        repository.update(record);

        event = new RecordEvent();
        event.addUpdatedField(liveTag.getId());
        message = new TestQueueMessage(EVENT_RECORD_UPDATED, record.getId(), event.toJsonBytes());
        queue.broadCastMessage(message);

        solrServer.commit(true, true);

        verifyResultCount("field1:pear", 1);
        verifyResultCount("field1:apple", 0);

        // The end
        indexer.stop();
    }
    
    private void verifyResultCount(String query, int count) throws SolrServerException {
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.set("q", query);
        QueryResponse response = SOLR_TEST_UTIL.getSolrServer().query(solrQuery);
        assertEquals(count, response.getResults().getNumFound());
    }

}
