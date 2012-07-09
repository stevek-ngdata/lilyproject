package org.lilyproject.util.repo.test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
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
import org.lilyproject.rowlog.api.RowLogConfigurationManager;
import org.lilyproject.rowlog.api.RowLogException;
import org.lilyproject.rowlog.api.RowLogMessage;
import org.lilyproject.rowlog.api.RowLogMessageListener;
import org.lilyproject.rowlog.api.RowLogMessageListenerMapping;
import org.lilyproject.rowlog.api.RowLogSubscription;
import org.lilyproject.util.repo.RecordEvent;

public class RecordEventTest {
    private static final String NS = "org.lilyproject.util.repo.test";
    
    private final static RepositorySetup repoSetup = new RepositorySetup();
    
    private static Repository repository;
    private static IdGenerator idGenerator;
    private static TypeManager typeManager;
    private static RecordType rt1;
    private static FieldType field1;
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        repoSetup.setupCore();
        repoSetup.setupRepository(true);

        repository = repoSetup.getRepository();
        typeManager = repoSetup.getTypeManager();
        idGenerator = repository.getIdGenerator();
        
        setupSchema();
    }

    @Test
    public void testRecordEventAttributes() throws Exception{
        final Map<String,String> attr = new HashMap<String,String>();
        attr.put("one", "one");
        attr.put("two", "two");
        RowLogMessageListener messageVerifier = new RowLogMessageListener() {
            
            @Override
            public boolean processMessage(RowLogMessage message) throws InterruptedException {
                try {
                    RecordEvent recordEvent = new RecordEvent(message.getPayload(), idGenerator);
                    Assert.assertEquals(attr, recordEvent.getAttributes());
                } catch (IOException e) {
                    Assert.fail(e.getMessage());
                } catch (RowLogException e) {
                    Assert.fail(e.getMessage());
                }
                
                return true;
            }
        };
        
        RowLogConfigurationManager rowLogConfMgr = repoSetup.getRowLogConfManager();
        rowLogConfMgr.addSubscription("WAL", "MessageVerifier", RowLogSubscription.Type.VM, 1);

        repoSetup.waitForSubscription(repoSetup.getWal(), "MessageVerifier");
        RowLogMessageListenerMapping.INSTANCE.put("MessageVerifier", messageVerifier);
        
        // test create
        Record record = repository.newRecord();
        record.setRecordType(rt1.getName());
        record.setField(field1.getName(), "something");
        record.setAttributes(attr);        
        record = repository.create(record);
        Assert.assertTrue(record.getAttributes().isEmpty());
        
        // test update
        attr.clear();
        attr.put("update", "update");
        record.setField(field1.getName(), "something else");
        record.setAttributes(attr);
        repository.update(record);
        
        // test read : attr empty
        record = repository.read(record.getId(), field1.getName());
        Assert.assertTrue(record.getAttributes().isEmpty());
        
        // delete won't be tested since attributes can't be passed on a delete        
    }
    
    private static void setupSchema () throws Exception{
        QName field1Name = new QName(NS, "field1");
        field1 = typeManager.newFieldType(typeManager.getValueType("STRING"), field1Name, Scope.VERSIONED);
        field1 = typeManager.createFieldType(field1);
        
        rt1 = typeManager.newRecordType(new QName(NS, "NVRecordType1"));
        rt1 = typeManager.createRecordType(rt1);

    }

}
