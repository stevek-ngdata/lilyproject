package org.lilycms.indexer.test;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.util.Bytes;
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
import org.lilycms.queue.api.LilyQueue;
import org.lilycms.queue.api.QueueListener;
import org.lilycms.queue.api.QueueMessage;
import org.lilycms.repository.api.*;
import org.lilycms.repository.impl.*;
import org.lilycms.testfw.TestHelper;

import java.io.File;
import java.io.FileOutputStream;
import java.util.*;

import static org.junit.Assert.*;

public class IndexerTest {
    private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    private static IndexerConf INDEXER_CONF;
    private static SolrTestingUtility SOLR_TEST_UTIL;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        INDEXER_CONF = IndexerConfBuilder.build(IndexerTest.class.getClassLoader().getResourceAsStream("org/lilycms/indexer/test/indexerconf1.xml"));
        File tmpFile = File.createTempFile("solr-schema", "xml");
        FileOutputStream fos = new FileOutputStream(tmpFile);
        INDEXER_CONF.generateSolrSchema(fos);
        fos.close();

        SOLR_TEST_UTIL = new SolrTestingUtility(tmpFile.getAbsolutePath());

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
        TypeManager typeManager = new HBaseTypeManager(RecordTypeImpl.class, FieldDescriptorImpl.class, TEST_UTIL.getConfiguration());
        IdGenerator idGenerator = new IdGeneratorImpl();
        Repository repository = new HBaseRepository(typeManager, idGenerator, RecordImpl.class, FieldImpl.class, TEST_UTIL.getConfiguration());
        SolrServer solrServer = SOLR_TEST_UTIL.getSolrServer();
        TestLilyQueue queue = new TestLilyQueue();
        Indexer indexer = new Indexer(INDEXER_CONF, queue, repository, solrServer);

        // Create a record type
        RecordType recordType = typeManager.newRecordType("RecordType1");
        recordType.addFieldDescriptor(typeManager.newFieldDescriptor("field1", "string", true, true));
        recordType.addFieldDescriptor(typeManager.newFieldDescriptor("field2", "string", true, true));
        recordType.addFieldDescriptor(typeManager.newFieldDescriptor("field3", "string", true, true));
        recordType.addFieldDescriptor(typeManager.newFieldDescriptor("nvfield1", "string", true, false));
        typeManager.createRecordType(recordType);

        // TODO need to re-retrieve the record type because its version property is not updated
        recordType = typeManager.getRecordType("RecordType1");

        // Create a document
        Record record = repository.newRecord();
        record.setRecordType("RecordType1", recordType.getVersion());
        record.addField(repository.newField("field1", Bytes.toBytes("apple")));
        record.addField(repository.newField("field2", Bytes.toBytes("pear")));
        record.addField(repository.newField("field3", Bytes.toBytes("orange")));
        record.addField(repository.newField("nvfield1", Bytes.toBytes("banana")));
        repository.create(record);

        // Generate queue message
        // TODO remove this once the real queue exists
        QueueMessage message = new TestQueueMessage("document-created", record.getId(), null);
        queue.broadCastMessage(message);

        // Make sure all index writes are committed
        solrServer.commit(true, true);

        // Verify the index was updated
        verifyOneResult("RecordType1.ifield1:apple");
        verifyOneResult("RecordType1.ifield2:apple");
        verifyOneResult("RecordType1.ifield2:pear");
        verifyOneResult("gifield1:orange");
        verifyOneResult("RecordType1.nvifield1:banana");

        // Update the document, creating a new version
        record = repository.newRecord(record.getId());
        record.addField(repository.newField("field1", Bytes.toBytes("peach")));
        repository.update(record);
        record = repository.read(record.getId());

        // Generate queue message
        // TODO remove this once the real queue exists
        String msgData = "{ versionCreated: " + record.getVersion() + ", changedFields: [\"field1\"] }";
        message = new TestQueueMessage("document-updated", record.getId(), msgData.getBytes("UTF-8"));
        queue.broadCastMessage(message);

        // Make sure all index writes are committed
        solrServer.commit(true, true);

        // Verify the index was updated
        verifyOneResult("RecordType1.ifield1:peach");

        // The end
        indexer.stop();
    }
    
    private void verifyOneResult(String query) throws SolrServerException {
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.set("q", "RecordType1.ifield1:apple");
        QueryResponse response = SOLR_TEST_UTIL.getSolrServer().query(solrQuery);
        assertEquals(1, response.getResults().getNumFound());
    }

    public static class TestQueueMessage implements QueueMessage {
        private String type;
        private RecordId recordId;
        private byte[] data;

        public TestQueueMessage(String type, RecordId recordId, byte[] data) {
            this.type = type;
            this.recordId = recordId;
            this.data = data;
        }

        public String getType() {
            return type;
        }

        public RecordId getRecordId() {
            return recordId;
        }

        public byte[] getData() {
            return data;
        }
    }

    public static class TestLilyQueue implements LilyQueue {
        private List<QueueListener> listeners = new ArrayList<QueueListener>();
        private Map<String, QueueMessage> messages = new HashMap<String, QueueMessage>();
        private int counter = 0;

        public QueueMessage getMessage(String id) {
            return messages.get(id);
        }

        public void addListener(String listenerName, QueueListener listener) {
            listeners.add(listener);
        }

        public void removeListener(QueueListener listener) {
            listeners.remove(listener);
        }

        public void broadCastMessage(QueueMessage msg) {
            String msgId = String.valueOf(++counter);
            messages.put(msgId, msg);

            for (QueueListener listener : listeners){
                listener.processMessage(msgId);
            }
        }

    }
}
