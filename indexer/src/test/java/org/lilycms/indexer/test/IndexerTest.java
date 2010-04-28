package org.lilycms.indexer.test;

import static org.junit.Assert.assertEquals;
import static org.lilycms.repoutil.EventType.*;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilycms.hbaseindex.IndexManager;
import org.lilycms.indexer.Indexer;
import org.lilycms.indexer.conf.IndexerConf;
import org.lilycms.indexer.conf.IndexerConfBuilder;
import org.lilycms.linkmgmt.LinkIndex;
import org.lilycms.linkmgmt.LinkIndexUpdater;
import org.lilycms.queue.api.LilyQueue;
import org.lilycms.queue.api.QueueMessage;
import org.lilycms.queue.mock.TestLilyQueue;
import org.lilycms.queue.mock.TestQueueMessage;
import org.lilycms.repository.api.*;
import org.lilycms.repository.impl.*;
import org.lilycms.repoutil.RecordEvent;
import org.lilycms.repoutil.VersionTag;
import org.lilycms.testfw.TestHelper;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

// To run this test from an IDE, set a property solr.war pointing to the SOLR war

public class IndexerTest {
    private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    private static IndexerConf INDEXER_CONF;
    private static SolrTestingUtility SOLR_TEST_UTIL;
    private TestLilyQueue queue;

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
        queue = new TestLilyQueue();

        IndexManager.createIndexMetaTable(TEST_UTIL.getConfiguration());
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        try { LinkIndex.createIndexes(indexManager); } catch (TableExistsException e) { }
        LinkIndex linkIndex = new LinkIndex(indexManager, repository);
        new LinkIndexUpdater(repository, typeManager, linkIndex, queue);

        Indexer indexer = new Indexer(INDEXER_CONF, queue, repository, typeManager, solrServer, linkIndex);

        // Create a record type
        ValueType stringValueType = typeManager.getValueType("STRING", false, false);
        ValueType linkValueType = typeManager.getValueType("LINK", false, false);

        QName fieldType1Name = new QName("org.lilycms.indexer.test", "field1");
        FieldType fieldType1 = typeManager.newFieldType(stringValueType, fieldType1Name, Scope.NON_VERSIONED);
        fieldType1 = typeManager.createFieldType(fieldType1);

        QName fieldType2Name = new QName("org.lilycms.indexer.test", "field2");
        FieldType fieldType2 = typeManager.newFieldType(stringValueType, fieldType2Name, Scope.NON_VERSIONED);
        fieldType2 = typeManager.createFieldType(fieldType2);

        QName linkFieldName = new QName("org.lilycms.indexer.test", "linkfield");
        FieldType linkFieldType = typeManager.newFieldType(linkValueType, linkFieldName, Scope.NON_VERSIONED);
        linkFieldType = typeManager.createFieldType(linkFieldType);

        ValueType longValueType = typeManager.getValueType("LONG", false, false);
        QName liveTagName = new QName(VersionTag.NS_VTAG, "live");
        FieldType liveTag = typeManager.newFieldType(longValueType, liveTagName, Scope.NON_VERSIONED);
        liveTag = typeManager.createFieldType(liveTag);

        RecordType recordType1 = typeManager.newRecordType("RecordType1");
        recordType1.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType1.getId(), false));
        recordType1.addFieldTypeEntry(typeManager.newFieldTypeEntry(liveTag.getId(), false));
        recordType1.addFieldTypeEntry(typeManager.newFieldTypeEntry(linkFieldType.getId(), false));
        recordType1 = typeManager.createRecordType(recordType1);

        //
        // Basic, versionless, create-update-delete
        //
        {
            // Create a record
            Record record = repository.newRecord();
            record.setRecordType("RecordType1", null);
            record.setField(fieldType1Name, "apple");
            record = repository.create(record);

            // Generate queue message
            sendEvent(EVENT_RECORD_CREATED, record.getId(), fieldType1.getId());

            solrServer.commit(true, true);

            // Verify the index was updated
            verifyResultCount("field1:apple", 1);

            // Update the record
            record.setField(fieldType1Name, "pear");
            repository.update(record);

            sendEvent(EVENT_RECORD_UPDATED, record.getId(), fieldType1.getId());

            solrServer.commit(true, true);

            verifyResultCount("field1:pear", 1);
            verifyResultCount("field1:apple", 0);

            // Do as if field2 changed, while field2 is not present in the document.
            // Such situations can occur if the record is modified before earlier events are processed.
            sendEvent(EVENT_RECORD_UPDATED, record.getId(), fieldType2.getId());

            solrServer.commit(true, true);

            verifyResultCount("field1:pear", 1);
            verifyResultCount("field1:apple", 0);

            // Add a vtag field. For versionless records, this should have no effect
            record.setField(liveTagName, new Long(1));
            repository.update(record);

            sendEvent(EVENT_RECORD_UPDATED, record.getId(), liveTag.getId());

            solrServer.commit(true, true);

            verifyResultCount("field1:pear", 1);
            verifyResultCount("field1:apple", 0);

            // Delete the record
            repository.delete(record.getId());

            sendEvent(EVENT_RECORD_DELETED, record.getId());

            solrServer.commit(true, true);

            verifyResultCount("field1:pear", 0);
        }

        //
        // Deref
        //
        {
            Record record = repository.newRecord();
            record.setRecordType("RecordType1", null);
            record.setField(fieldType1Name, "pear");
            record = repository.create(record);
            // be lazy and don't send an event for this create

            Record record2 = repository.newRecord();
            record2.setRecordType("RecordType1", null);
            record2.setField(linkFieldName, record.getId());
            record2 = repository.create(record2);

            // Generate queue message
            RecordEvent event = new RecordEvent();
            event.addUpdatedField(linkFieldType.getId());
            QueueMessage message = new TestQueueMessage(EVENT_RECORD_CREATED, record2.getId(), event.toJsonBytes());
            queue.broadCastMessage(message);

            solrServer.commit(true, true);

            verifyResultCount("dereffield1:pear", 1);
        }


        //
        // Variant deref
        //
        {
            Record masterRecord = repository.newRecord();
            masterRecord.setRecordType("RecordType1", null);
            masterRecord.setField(fieldType1Name, "yellow");
            masterRecord = repository.create(masterRecord);

            RecordId var1Id = idGenerator.newRecordId(masterRecord.getId(), Collections.singletonMap("lang", "en"));
            Record var1Record = repository.newRecord(var1Id);
            var1Record.setRecordType("RecordType1", null);
            var1Record.setField(fieldType1Name, "green");
            repository.create(var1Record);

            sendEvent(EVENT_RECORD_CREATED, var1Id, fieldType2.getId());

            Map<String, String> varProps = new HashMap<String, String>();
            varProps.put("lang", "en");
            varProps.put("branch", "dev");
            RecordId var2Id = idGenerator.newRecordId(masterRecord.getId(), varProps);
            Record var2Record = repository.newRecord(var2Id);
            var2Record.setRecordType("RecordType1", null);
            var2Record.setField(fieldType2Name, "blue");
            repository.create(var2Record);

            sendEvent(EVENT_RECORD_CREATED, var2Id, fieldType2.getId());

            solrServer.commit(true, true);

            verifyResultCount("dereffield2:yellow", 1);
            verifyResultCount("dereffield3:yellow", 2);
            verifyResultCount("dereffield4:green", 1);
            verifyResultCount("dereffield3:green", 0);
        }

        //
        // Update denormalized data
        //
        {
            Record record1 = repository.newRecord(idGenerator.newRecordId("boe"));
            record1.setRecordType("RecordType1", null);
            record1.setField(fieldType1Name, "cumcumber");
            record1 = repository.create(record1);
            sendEvent(EVENT_RECORD_CREATED, record1.getId(), fieldType1.getId());

            // Create a record which will contain denormalized data through linking
            Record record2 = repository.newRecord();
            record2.setRecordType("RecordType1", null);
            record2.setField(linkFieldName, record1.getId());
            record2.setField(fieldType1Name, "mushroom");
            record2 = repository.create(record2);
            sendEvent(EVENT_RECORD_CREATED, record2.getId(), linkFieldType.getId(), fieldType1.getId());

            // Create a record which will contain denormalized data through master-dereferencing
            RecordId record3Id = idGenerator.newRecordId(record1.getId(), Collections.singletonMap("lang", "en"));
            Record record3 = repository.newRecord(record3Id);
            record3.setRecordType("RecordType1", null);
            record3.setField(fieldType1Name, "eggplant");
            record3 = repository.create(record3);
            sendEvent(EVENT_RECORD_CREATED, record3.getId(), fieldType1.getId());

            // Create a record which will contain denormalized data through variant-dereferencing
            Map<String, String> varprops = new HashMap<String, String>();
            varprops.put("lang", "en");
            varprops.put("branch", "dev");
            RecordId record4Id = idGenerator.newRecordId(record1.getId(), varprops);
            Record record4 = repository.newRecord(record4Id);
            record4.setRecordType("RecordType1", null);
            record4.setField(fieldType1Name, "broccoli");
            record4 = repository.create(record4);
            sendEvent(EVENT_RECORD_CREATED, record4.getId(), fieldType1.getId());
            solrServer.commit(true, true);

            verifyResultCount("dereffield1:cumcumber", 1);
            verifyResultCount("dereffield2:cumcumber", 1);
            verifyResultCount("dereffield3:cumcumber", 2);

            // Update record1, check if index of the others is updated
            record1.setField(fieldType1Name, "tomato");
            record1 = repository.update(record1);

            // Generate queue message
            sendEvent(EVENT_RECORD_UPDATED, record1.getId(), fieldType1.getId());
            solrServer.commit(true, true);

            verifyResultCount("dereffield1:tomato", 1);
            verifyResultCount("dereffield2:tomato", 1);
            verifyResultCount("dereffield3:tomato", 2);
            verifyResultCount("dereffield1:cumcumber", 0);
            verifyResultCount("dereffield2:cumcumber", 0);
            verifyResultCount("dereffield3:cumcumber", 0);
            verifyResultCount("dereffield4:eggplant", 1);

            // Update record3, index for record4 should be updated
            record3.setField(fieldType1Name, "courgette");
            repository.update(record3);
            sendEvent(EVENT_RECORD_UPDATED, record3.getId(), fieldType1.getId());
            solrServer.commit(true, true);

            verifyResultCount("dereffield4:courgette", 1);
            verifyResultCount("dereffield4:eggplant", 0);

            // Delete record 3: index for record 4 should be updated
            verifyResultCount("@@id:" + ClientUtils.escapeQueryChars(record3.getId().toString()), 1);
            repository.delete(record3.getId());
            sendEvent(EVENT_RECORD_DELETED, record3.getId());
            solrServer.commit(true, true);

            verifyResultCount("dereffield4:courgette", 0);
            verifyResultCount("dereffield3:tomato", 1);
            verifyResultCount("@@id:" + ClientUtils.escapeQueryChars(record3.getId().toString()), 0);

            // Delete record 4 (at the time of this writing, because it is unsure if we will allow deleting master
            // records while there are variants)
            repository.delete(record4.getId());
            sendEvent(EVENT_RECORD_DELETED, record4.getId());
            solrServer.commit(true, true);

            verifyResultCount("dereffield3:tomato", 0);
            verifyResultCount("field1:broccoli", 0);
            verifyResultCount("@@id:" + ClientUtils.escapeQueryChars(record4.getId().toString()), 0);

            // Delete record 1: index of record 2 should be updated
            repository.delete(record1.getId());
            sendEvent(EVENT_RECORD_DELETED, record1.getId());
            solrServer.commit(true, true);

            verifyResultCount("dereffield1:tomato", 0);
            verifyResultCount("field1:mushroom", 1);
        }


        // The end
        indexer.stop();
    }

    private void sendEvent(String type, RecordId recordId, String... updatedFields) {
        RecordEvent event = new RecordEvent();

        for (String updatedField : updatedFields) {
            event.addUpdatedField(updatedField);
        }

        QueueMessage message = new TestQueueMessage(type, recordId, event.toJsonBytes());
        queue.broadCastMessage(message);
    }
    
    private void verifyResultCount(String query, int count) throws SolrServerException {
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.set("q", query);
        QueryResponse response = SOLR_TEST_UTIL.getSolrServer().query(solrQuery);
        assertEquals(count, response.getResults().getNumFound());
    }

}
