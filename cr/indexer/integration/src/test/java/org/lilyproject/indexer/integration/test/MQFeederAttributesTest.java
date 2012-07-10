package org.lilyproject.indexer.integration.test;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.Scope;

public class MQFeederAttributesTest extends BaseIndexMQFeedingTest{
    private static String NS = "org.lilyproject.indexer.integration.test.attr";
   
    @BeforeClass
    public static void setupBeforeClass () throws Exception{
        BaseIndexMQFeedingTest.baseSetup();
        setupSchema();
    }
    
    public  static void setupSchema()  throws Exception{
        //
        // Define schema
        //
        repository = repoSetup.getRepository();
        typeManager = repository.getTypeManager();

        FieldType field1 = typeManager.fieldTypeBuilder()
                .name(new QName(NS, "noindex_string"))
                .type("STRING")
                .scope(Scope.NON_VERSIONED)
                .createOrUpdate();       

        typeManager.recordTypeBuilder()
                .defaultNamespace(NS)
                .name("NoIndexAttribute")
                .field(field1.getId(), true)
                .createOrUpdate();
    }
    
    @Test
    public void testNoIndexAttribute() throws Exception {
        QName stringFieldName = new QName(NS,"noindex_string");
         
        List<String> conf = new ArrayList<String>();
        conf.add("indexerconf_noindex_configuration.xml");
        setupTwoIndexes(conf);
        
        TrackingIndexUpdater indexUpdater = indexUpdaters.get(0);
        MySolrClient solrClient = solrClients.get(0);
        
        assertEquals(0, indexUpdater.events());
        
        
        Record record = repository.recordBuilder()
                .defaultNamespace(NS)
                .recordType("NoIndexAttribute")
                .field(stringFieldName, "noindex-field-test")
                .build();
        record.getAttributes().put("lily.mq", "false");
        record = repository.create(record);
        repoSetup.processMQ();
        
        // Assert not indexed by checking that the recordEvent didn't reach the indexer
        assertEquals(0, indexUpdaters.get(0).events());       
        assertEquals(0, solrClient.adds());
        
        record.setField(stringFieldName, "index-field-test");
        record.setAttributes(null);
        record = repository.update(record);
        repoSetup.processMQ();
        
        // Assert indexed        
        assertEquals(1, solrClient.adds());
        assertEquals(1, indexUpdater.events());
        
        assertEquals(0, indexUpdater.events());
        assertEquals(0, solrClient.adds());
        
        record.setField(stringFieldName, "updated-changes-noindex");
        record.getAttributes().put("lily.mq", "false");
        record = repository.update(record);        
        repoSetup.processMQ();
        
        // Assert changes not indexed
        assertEquals(0, indexUpdater.events());
        assertEquals(0, solrClient.adds());
    }

}
