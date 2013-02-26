package org.lilyproject.util.repo.test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.ngdata.sep.EventListener;
import com.ngdata.sep.SepEvent;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repotestfw.RepositorySetup;
import org.lilyproject.util.hbase.LilyHBaseSchema.Table;
import org.lilyproject.util.repo.RecordEvent;

public class RecordEventTest {
    private static final String NS = "org.lilyproject.util.repo.test";
    private static final String subscriptionId = "MessageVerifier";

    private final static RepositorySetup repoSetup = new RepositorySetup();

    private static Repository repository;
    private static IdGenerator idGenerator;
    private static TypeManager typeManager;
    private static RecordType rt1;
    private static FieldType field1;
    
    private CountingMessageVerifier messageVerifier;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        repoSetup.setupCore();
        repoSetup.setupRepository();

        repository = repoSetup.getRepositoryManager().getRepository(Table.RECORD.name);
        typeManager = repoSetup.getTypeManager();
        idGenerator = repository.getIdGenerator();

        setupSchema();
    }
    
    @Before
    public void setUp() throws Exception {
        messageVerifier = new CountingMessageVerifier();
        repoSetup.getSepModel().addSubscription(subscriptionId);
        repoSetup.startSepEventSlave(subscriptionId, messageVerifier);
        repoSetup.getHBaseProxy().waitOnReplicationPeerReady(subscriptionId);
    }
    
    @After
    public void tearDown() throws Exception {
        repoSetup.stopSepEventSlave();
        repoSetup.getSepModel().removeSubscription(subscriptionId);
        repoSetup.getHBaseProxy().waitOnReplicationPeerStopped(subscriptionId);
    }

    @Test
    public void testRecordEventAttributes() throws Exception{
        final Map<String,String> attr = new HashMap<String,String>();
        attr.put("one", "one");
        attr.put("two", "two");

        // test create
        messageVerifier.setExpectedAttributes(attr);
        Record record = repository.newRecord();
        record.setRecordType(rt1.getName());
        record.setField(field1.getName(), "something");
        record.setAttributes(attr);
        record = repository.create(record);
        
        repoSetup.waitForSepProcessing();
        
        Assert.assertTrue(record.getAttributes().isEmpty());
        Assert.assertEquals(1, messageVerifier.getMessageCount());

        // test update
        attr.clear();
        attr.put("update", "update");
        record.setField(field1.getName(), "something else");
        record.setAttributes(attr);
        repository.update(record);
        
        repoSetup.waitForSepProcessing();
        
        Assert.assertEquals(2, messageVerifier.getMessageCount());

        // test read : attr empty
        record = repository.read(record.getId(), field1.getName());
        Assert.assertTrue(record.getAttributes().isEmpty());

        // test delete
        attr.clear();
        attr.put("delete", "deletevalue");
        record.setField(field1.getName(), "something else");
        record.setAttributes(attr);
        repository.delete(record);
        
        repoSetup.waitForSepProcessing();
        
        Assert.assertEquals(3, messageVerifier.getMessageCount());
    }

    private static void setupSchema () throws Exception{
        QName field1Name = new QName(NS, "field1");
        field1 = typeManager.newFieldType(typeManager.getValueType("STRING"), field1Name, Scope.VERSIONED);
        field1 = typeManager.createFieldType(field1);

        rt1 = typeManager.newRecordType(new QName(NS, "NVRecordType1"));
        rt1 = typeManager.createRecordType(rt1);

    }

    private class CountingMessageVerifier implements EventListener {
        private int messageCounter = 0;
        private Map<String, String> attr;

        @Override
        public void processEvent(SepEvent event) {
            if (event.getPayload() == null) {
                return;
            }
            try {
                RecordEvent recordEvent = new RecordEvent(event.getPayload(), idGenerator);
                Assert.assertEquals(attr, recordEvent.getAttributes());
                messageCounter++;
            } catch (IOException e) {
                Assert.fail(e.getMessage());
            }
        }

        public void setExpectedAttributes(Map<String, String> attr) {
            this.attr = attr;
        }

        public int getMessageCount() {
            return this.messageCounter;
        }
    }

}
