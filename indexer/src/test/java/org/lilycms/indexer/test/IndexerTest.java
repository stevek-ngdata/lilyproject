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
import org.lilycms.linkindex.LinkIndex;
import org.lilycms.linkindex.LinkIndexUpdater;
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

// To run this test from an IDE, set a property solr.war pointing to the SOLR war

public class IndexerTest {
    private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    private static IndexerConf INDEXER_CONF;
    private static SolrTestingUtility SOLR_TEST_UTIL;
    private static TestLilyQueue queue;
    private static Repository repository;
    private static TypeManager typeManager;
    private static IdGenerator idGenerator;
    private static SolrServer solrServer;
    private static Indexer indexer;

    private static FieldType liveTag;
    private static FieldType previewTag;
    private static FieldType lastTag;

    private static FieldType nvfield1;
    private static FieldType nvfield2;
    private static FieldType nvLinkField1;

    private static FieldType vfield1;
    private static FieldType vLinkField1;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        SOLR_TEST_UTIL = new SolrTestingUtility("org/lilycms/indexer/test/schema1.xml");

        TestHelper.setupLogging("org.lilycms.indexer", "org.lilycms.linkindex");
        TEST_UTIL.startMiniCluster(1);
        SOLR_TEST_UTIL.start();

        idGenerator = new IdGeneratorImpl();
        typeManager = new HBaseTypeManager(idGenerator, TEST_UTIL.getConfiguration());
        BlobStoreAccess dfsBlobStoreAccess = new DFSBlobStoreAccess(TEST_UTIL.getDFSCluster().getFileSystem());
        SizeBasedBlobStoreAccessFactory blobStoreAccessFactory = new SizeBasedBlobStoreAccessFactory(dfsBlobStoreAccess);
        repository = new HBaseRepository(typeManager, idGenerator, blobStoreAccessFactory, TEST_UTIL.getConfiguration());
        solrServer = SOLR_TEST_UTIL.getSolrServer();
        queue = new TestLilyQueue();

        IndexManager.createIndexMetaTable(TEST_UTIL.getConfiguration());
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        try { LinkIndex.createIndexes(indexManager); } catch (TableExistsException e) { }
        LinkIndex linkIndex = new LinkIndex(indexManager, repository);
        new LinkIndexUpdater(repository, typeManager, linkIndex, queue);

        // Field types should exist before the indexer conf is loaded
        setupSchema();

        INDEXER_CONF = IndexerConfBuilder.build(IndexerTest.class.getClassLoader().getResourceAsStream("org/lilycms/indexer/test/indexerconf1.xml"), repository);
        indexer = new Indexer(INDEXER_CONF, queue, repository, typeManager, solrServer, linkIndex);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        indexer.stop();        

        TEST_UTIL.shutdownMiniCluster();
        if (SOLR_TEST_UTIL != null)
            SOLR_TEST_UTIL.stop();
    }

    private static void setupSchema() throws Exception {
        //
        // Version tag fields
        //
        ValueType longValueType = typeManager.getValueType("LONG", false, false);

        QName liveTagName = new QName(VersionTag.NAMESPACE, "live");
        liveTag = typeManager.newFieldType(longValueType, liveTagName, Scope.NON_VERSIONED);
        liveTag = typeManager.createFieldType(liveTag);

        QName previewTagName = new QName(VersionTag.NAMESPACE, "preview");
        previewTag = typeManager.newFieldType(longValueType, previewTagName, Scope.NON_VERSIONED);
        previewTag = typeManager.createFieldType(previewTag);

        QName lastTagName = new QName(VersionTag.NAMESPACE, "last");
        lastTag = typeManager.newFieldType(longValueType, lastTagName, Scope.NON_VERSIONED);
        lastTag = typeManager.createFieldType(lastTag);

        //
        // Schema types for the versionless test
        //
        ValueType stringValueType = typeManager.getValueType("STRING", false, false);
        ValueType linkValueType = typeManager.getValueType("LINK", false, false);

        QName field1Name = new QName("org.lilycms.indexer.test", "nv_field1");
        nvfield1 = typeManager.newFieldType(stringValueType, field1Name, Scope.NON_VERSIONED);
        nvfield1 = typeManager.createFieldType(nvfield1);

        QName field2Name = new QName("org.lilycms.indexer.test", "nv_field2");
        nvfield2 = typeManager.newFieldType(stringValueType, field2Name, Scope.NON_VERSIONED);
        nvfield2 = typeManager.createFieldType(nvfield2);

        QName linkField1Name = new QName("org.lilycms.indexer.test", "nv_linkfield1");
        nvLinkField1 = typeManager.newFieldType(linkValueType, linkField1Name, Scope.NON_VERSIONED);
        nvLinkField1 = typeManager.createFieldType(nvLinkField1);

        RecordType nvRecordType1 = typeManager.newRecordType("NVRecordType1");
        nvRecordType1.addFieldTypeEntry(typeManager.newFieldTypeEntry(nvfield1.getId(), false));
        nvRecordType1.addFieldTypeEntry(typeManager.newFieldTypeEntry(liveTag.getId(), false));
        nvRecordType1.addFieldTypeEntry(typeManager.newFieldTypeEntry(nvLinkField1.getId(), false));
        nvRecordType1 = typeManager.createRecordType(nvRecordType1);

        //
        // Schema types for the versioned test
        //
        QName vfield1Name = new QName("org.lilycms.indexer.test.2", "v_field1");
        vfield1 = typeManager.newFieldType(stringValueType, vfield1Name, Scope.VERSIONED);
        vfield1 = typeManager.createFieldType(vfield1);

        QName vlinkField1Name = new QName("org.lilycms.indexer.test.2", "v_linkfield1");
        vLinkField1 = typeManager.newFieldType(linkValueType, vlinkField1Name, Scope.VERSIONED);
        vLinkField1 = typeManager.createFieldType(vLinkField1);

        RecordType vRecordType1 = typeManager.newRecordType("VRecordType1");
        vRecordType1.addFieldTypeEntry(typeManager.newFieldTypeEntry(nvfield1.getId(), false));
        vRecordType1.addFieldTypeEntry(typeManager.newFieldTypeEntry(liveTag.getId(), false));
        vRecordType1.addFieldTypeEntry(typeManager.newFieldTypeEntry(previewTag.getId(), false));
        vRecordType1.addFieldTypeEntry(typeManager.newFieldTypeEntry(nvLinkField1.getId(), false));
        vRecordType1 = typeManager.createRecordType(vRecordType1);
    }

    @Test
    public void testIndexerVersionless() throws Exception {

        //
        // Basic, versionless, create-update-delete
        //
        {
            // Create a record
            Record record = repository.newRecord();
            record.setRecordType("NVRecordType1", null);
            record.setField(nvfield1.getName(), "apple");
            record = repository.create(record);

            // Generate queue message
            sendEvent(EVENT_RECORD_CREATED, record.getId(), nvfield1.getId());
            solrServer.commit(true, true);

            // Verify the index was updated
            verifyResultCount("nv_field1:apple", 1);

            // Update the record
            record.setField(nvfield1.getName(), "pear");
            repository.update(record);

            sendEvent(EVENT_RECORD_UPDATED, record.getId(), nvfield1.getId());
            solrServer.commit(true, true);

            verifyResultCount("nv_field1:pear", 1);
            verifyResultCount("nv_field1:apple", 0);

            // Do as if field2 changed, while field2 is not present in the document.
            // Such situations can occur if the record is modified before earlier events are processed.
            sendEvent(EVENT_RECORD_UPDATED, record.getId(), nvfield2.getId());
            solrServer.commit(true, true);

            verifyResultCount("nv_field1:pear", 1);
            verifyResultCount("nv_field1:apple", 0);

            // Add a vtag field. For versionless records, this should have no effect
            // TODO test version number below should actually be 1, but currently versionless records do have a version 1 (see #1/#2)
            record.setField(liveTag.getName(), new Long(5));
            repository.update(record);

            sendEvent(EVENT_RECORD_UPDATED, record.getId(), liveTag.getId());
            solrServer.commit(true, true);

            verifyResultCount("nv_field1:pear", 1);
            verifyResultCount("nv_field1:apple", 0);

            // Delete the record
            repository.delete(record.getId());

            sendEvent(EVENT_RECORD_DELETED, record.getId());
            solrServer.commit(true, true);

            verifyResultCount("nv_field1:pear", 0);
        }

        //
        // Deref
        //
        {
            Record record1 = repository.newRecord();
            record1.setRecordType("NVRecordType1", null);
            record1.setField(nvfield1.getName(), "pear");
            record1 = repository.create(record1);
            sendEvent(EVENT_RECORD_CREATED, record1.getId(), nvfield1.getId());

            Record record2 = repository.newRecord();
            record2.setRecordType("NVRecordType1", null);
            record2.setField(nvLinkField1.getName(), record1.getId());
            record2 = repository.create(record2);
            sendEvent(EVENT_RECORD_CREATED, record2.getId(), nvLinkField1.getId());

            solrServer.commit(true, true);

            verifyResultCount("nv_deref1:pear", 1);
        }


        //
        // Variant deref
        //
        {
            Record masterRecord = repository.newRecord();
            masterRecord.setRecordType("NVRecordType1", null);
            masterRecord.setField(nvfield1.getName(), "yellow");
            masterRecord = repository.create(masterRecord);
            sendEvent(EVENT_RECORD_CREATED, masterRecord.getId(), nvfield1.getId());

            RecordId var1Id = idGenerator.newRecordId(masterRecord.getId(), Collections.singletonMap("lang", "en"));
            Record var1Record = repository.newRecord(var1Id);
            var1Record.setRecordType("NVRecordType1", null);
            var1Record.setField(nvfield1.getName(), "green");
            repository.create(var1Record);
            sendEvent(EVENT_RECORD_CREATED, var1Id, nvfield2.getId());

            Map<String, String> varProps = new HashMap<String, String>();
            varProps.put("lang", "en");
            varProps.put("branch", "dev");
            RecordId var2Id = idGenerator.newRecordId(masterRecord.getId(), varProps);
            Record var2Record = repository.newRecord(var2Id);
            var2Record.setRecordType("NVRecordType1", null);
            var2Record.setField(nvfield2.getName(), "blue");
            repository.create(var2Record);
            sendEvent(EVENT_RECORD_CREATED, var2Id, nvfield2.getId());

            solrServer.commit(true, true);

            verifyResultCount("nv_deref2:yellow", 1);
            verifyResultCount("nv_deref3:yellow", 2);
            verifyResultCount("nv_deref4:green", 1);
            verifyResultCount("nv_deref3:green", 0);
        }

        //
        // Update denormalized data
        //
        {
            Record record1 = repository.newRecord(idGenerator.newRecordId("boe"));
            record1.setRecordType("NVRecordType1", null);
            record1.setField(nvfield1.getName(), "cumcumber");
            record1 = repository.create(record1);
            sendEvent(EVENT_RECORD_CREATED, record1.getId(), nvfield1.getId());

            // Create a record which will contain denormalized data through linking
            Record record2 = repository.newRecord();
            record2.setRecordType("NVRecordType1", null);
            record2.setField(nvLinkField1.getName(), record1.getId());
            record2.setField(nvfield1.getName(), "mushroom");
            record2 = repository.create(record2);
            sendEvent(EVENT_RECORD_CREATED, record2.getId(), nvLinkField1.getId(), nvfield1.getId());

            // Create a record which will contain denormalized data through master-dereferencing
            RecordId record3Id = idGenerator.newRecordId(record1.getId(), Collections.singletonMap("lang", "en"));
            Record record3 = repository.newRecord(record3Id);
            record3.setRecordType("NVRecordType1", null);
            record3.setField(nvfield1.getName(), "eggplant");
            record3 = repository.create(record3);
            sendEvent(EVENT_RECORD_CREATED, record3.getId(), nvfield1.getId());

            // Create a record which will contain denormalized data through variant-dereferencing
            Map<String, String> varprops = new HashMap<String, String>();
            varprops.put("lang", "en");
            varprops.put("branch", "dev");
            RecordId record4Id = idGenerator.newRecordId(record1.getId(), varprops);
            Record record4 = repository.newRecord(record4Id);
            record4.setRecordType("NVRecordType1", null);
            record4.setField(nvfield1.getName(), "broccoli");
            record4 = repository.create(record4);
            sendEvent(EVENT_RECORD_CREATED, record4.getId(), nvfield1.getId());
            solrServer.commit(true, true);

            verifyResultCount("nv_deref1:cumcumber", 1);
            verifyResultCount("nv_deref2:cumcumber", 1);
            verifyResultCount("nv_deref3:cumcumber", 2);

            // Update record1, check if index of the others is updated
            record1.setField(nvfield1.getName(), "tomato");
            record1 = repository.update(record1);

            // Generate queue message
            sendEvent(EVENT_RECORD_UPDATED, record1.getId(), nvfield1.getId());
            solrServer.commit(true, true);

            verifyResultCount("nv_deref1:tomato", 1);
            verifyResultCount("nv_deref2:tomato", 1);
            verifyResultCount("nv_deref3:tomato", 2);
            verifyResultCount("nv_deref1:cumcumber", 0);
            verifyResultCount("nv_deref2:cumcumber", 0);
            verifyResultCount("nv_deref3:cumcumber", 0);
            verifyResultCount("nv_deref4:eggplant", 1);

            // Update record3, index for record4 should be updated
            record3.setField(nvfield1.getName(), "courgette");
            repository.update(record3);
            sendEvent(EVENT_RECORD_UPDATED, record3.getId(), nvfield1.getId());
            solrServer.commit(true, true);

            verifyResultCount("nv_deref4:courgette", 1);
            verifyResultCount("nv_deref4:eggplant", 0);

            // Delete record 3: index for record 4 should be updated
            verifyResultCount("@@id:" + ClientUtils.escapeQueryChars(record3.getId().toString()), 1);
            repository.delete(record3.getId());
            sendEvent(EVENT_RECORD_DELETED, record3.getId());
            solrServer.commit(true, true);

            verifyResultCount("nv_deref4:courgette", 0);
            verifyResultCount("nv_deref3:tomato", 1);
            verifyResultCount("@@id:" + ClientUtils.escapeQueryChars(record3.getId().toString()), 0);

            // Delete record 4 (at the time of this writing, because it is unsure if we will allow deleting master
            // records while there are variants)
            repository.delete(record4.getId());
            sendEvent(EVENT_RECORD_DELETED, record4.getId());
            solrServer.commit(true, true);

            verifyResultCount("nv_deref3:tomato", 0);
            verifyResultCount("nv_field1:broccoli", 0);
            verifyResultCount("@@id:" + ClientUtils.escapeQueryChars(record4.getId().toString()), 0);

            // Delete record 1: index of record 2 should be updated
            repository.delete(record1.getId());
            sendEvent(EVENT_RECORD_DELETED, record1.getId());
            solrServer.commit(true, true);

            verifyResultCount("nv_deref1:tomato", 0);
            verifyResultCount("nv_field1:mushroom", 1);
        }
    }

    @Test
    public void testIndexerWithVersioning() throws Exception {
        //
        // Basic create-update-delete
        //
        {
            // Create a record
            Record record = repository.newRecord();
            record.setRecordType("VRecordType1", null);
            record.setField(vfield1.getName(), "apple");
            record.setField(liveTag.getName(), new Long(1));
            record = repository.create(record);

            // Generate queue message
            sendEvent(EVENT_RECORD_CREATED, record.getId(), 1L, null, nvfield1.getId(), liveTag.getId());
            solrServer.commit(true, true);

            // Verify the index was updated                                                                        `
            verifyResultCount("v_field1:apple", 1);

            // Update the record, this will create a new version, but we leave the live version tag pointing to version 1
            record.setField(vfield1.getName(), "pear");
            repository.update(record);

            sendEvent(EVENT_RECORD_UPDATED, record.getId(), 2L, null, nvfield1.getId());
            solrServer.commit(true, true);

            verifyResultCount("v_field1:pear", 0);
            verifyResultCount("v_field1:apple", 1);

            // Now move the live version tag to point to version 2
            record.setField(liveTag.getName(), new Long(2));
            record = repository.update(record);
            sendEvent(EVENT_RECORD_UPDATED, record.getId(), liveTag.getId());
            solrServer.commit(true, true);

            verifyResultCount("v_field1:pear", 1);
            verifyResultCount("v_field1:apple", 0);

            // Now remove the live version tag
            record.delete(liveTag.getName(), true);
            record = repository.update(record);
            sendEvent(EVENT_RECORD_UPDATED, record.getId(), liveTag.getId());
            solrServer.commit(true, true);

            verifyResultCount("v_field1:pear", 0);

            // Now test with multiple version tags
            record.setField(liveTag.getName(), new Long(1));
            record.setField(previewTag.getName(), new Long(2));
            record.setField(lastTag.getName(), new Long(2));
            record = repository.update(record);
            sendEvent(EVENT_RECORD_UPDATED, record.getId(), liveTag.getId(), previewTag.getId(), lastTag.getId());
            solrServer.commit(true, true);

            verifyResultCount("v_field1:apple", 1);
            verifyResultCount("v_field1:pear", 2);

            verifyResultCount("+v_field1:pear +@@vtag:" + qesc(previewTag.getId()), 1);
            verifyResultCount("+v_field1:pear +@@vtag:" + qesc(lastTag.getId()), 1);
            verifyResultCount("+v_field1:pear +@@vtag:" + qesc(liveTag.getId()), 0);
            verifyResultCount("+v_field1:apple +@@vtag:" + qesc(liveTag.getId()), 1);
        }

        //
        // Deref
        //
        {
            Record record1 = repository.newRecord();
            record1.setRecordType("VRecordType1", null);
            record1.setField(vfield1.getName(), "pear");
            record1.setField(liveTag.getName(), Long.valueOf(1));
            record1 = repository.create(record1);
            sendEvent(EVENT_RECORD_CREATED, record1.getId(), 1L, null, vfield1.getId(), liveTag.getId());

            Record record2 = repository.newRecord();
            record2.setRecordType("VRecordType1", null);
            record2.setField(vLinkField1.getName(), record1.getId());
            record2.setField(liveTag.getName(), Long.valueOf(1));
            record2 = repository.create(record2);
            sendEvent(EVENT_RECORD_CREATED, record2.getId(), 1L, null, vLinkField1.getId(), liveTag.getId());

            solrServer.commit(true, true);

            verifyResultCount("v_deref1:pear", 1);

            // remove the live tag from record
            record1.delete(liveTag.getName(), true);
            record1 = repository.update(record1);
            sendEvent(EVENT_RECORD_UPDATED, record1.getId(), liveTag.getId());

            solrServer.commit(true, true);
            verifyResultCount("v_deref1:pear", 0);
        }
    }

    private static String qesc(String input) {
        return ClientUtils.escapeQueryChars(input);
    }

    private void sendEvent(String type, RecordId recordId, String... updatedFields) {
        RecordEvent event = new RecordEvent();

        for (String updatedField : updatedFields) {
            event.addUpdatedField(updatedField);
        }

        QueueMessage message = new TestQueueMessage(type, recordId, event.toJsonBytes());
        queue.broadCastMessage(message);
    }
    
    private void sendEvent(String type, RecordId recordId, Long versionCreated, Long versionUpdated,
            String... updatedFields) {

        RecordEvent event = new RecordEvent();

        for (String updatedField : updatedFields) {
            event.addUpdatedField(updatedField);
        }

        if (versionCreated != null)
            event.setVersionCreated(versionCreated);

        if (versionUpdated != null)
            event.setVersionUpdated(versionUpdated);

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
