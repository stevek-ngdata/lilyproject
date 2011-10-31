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
package org.lilyproject.lilyservertestfw.test;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilyproject.client.LilyClient;
import org.lilyproject.indexer.model.api.IndexDefinition;
import org.lilyproject.indexer.model.api.IndexUpdateState;
import org.lilyproject.indexer.model.api.WriteableIndexerModel;
import org.lilyproject.lilyservertestfw.LilyProxy;
import org.lilyproject.repository.api.*;


public class LilyProxyTest {

    private static final QName RECORDTYPE1 = new QName("org.lilyproject.lilytestutility", "TestRecordType");
    private static final QName FIELD1 = new QName("org.lilyproject.lilytestutility","name");
    private static LilyProxy lilyProxy;
    private static LilyClient lilyClient;
    private Repository repository;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        lilyProxy = new LilyProxy();
        byte[] schemaData = IOUtils.toByteArray(LilyProxyTest.class.getResourceAsStream("lilytestutility_solr_schema.xml"));
        lilyProxy.start(schemaData);
        lilyClient = lilyProxy.getLilyServerProxy().getClient();
    }
    
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        lilyProxy.stop();
    }
    
    @Test
    public void testCreateRecord() throws Exception {
        repository = lilyClient.getRepository();
        // Create schema
        TypeManager typeManager = repository.getTypeManager();
        FieldType fieldType1 = typeManager.createFieldType(typeManager.newFieldType(typeManager.getValueType("STRING"),
                FIELD1, Scope.NON_VERSIONED));
        RecordType recordType1 = typeManager.newRecordType(RECORDTYPE1);
        recordType1.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType1.getId(), false));
        typeManager.createRecordType(recordType1);
        
        // Add index
        String indexName = "testIndex";
        Assert.assertTrue("Adding index took too long", lilyProxy.getLilyServerProxy().addIndexFromResource(indexName,
                "org/lilyproject/lilyservertestfw/test/lilytestutility_indexerconf.xml", 60000L));
       
        // Create record
        Record record = repository.newRecord();
        record.setRecordType(RECORDTYPE1);
        record.setField(FIELD1, "name1");
        record = repository.create(record);
        record = repository.read(record.getId());
        Assert.assertEquals("name1", (String)record.getField(FIELD1));
        
        // Wait for messages to be processed
        Assert.assertTrue("Processing messages took too long", lilyProxy.waitWalAndMQMessagesProcessed(60000L));
        
        // Query Solr
        List<RecordId> recordIds = querySolr("name1");
        
        Assert.assertTrue(recordIds.contains(record.getId()));

        System.out.println("Original record:" +record.getId().toString());
        System.out.println("Queried record:" +recordIds.get(0).toString());

        //
        // Batch index build scenario
        //
        if (System.getProperty("os.name").startsWith("Windows")) {
            System.out.println("Skipping the Batch index build scenario since this does not work under Windows.");
        } else {
            // Disable incremental index updating
            WriteableIndexerModel indexerModel = lilyProxy.getLilyServerProxy().getIndexerModel();
            String lock = indexerModel.lockIndex(indexName);
            String subscriptionId;
            try {
                IndexDefinition index = indexerModel.getMutableIndex(indexName);
                subscriptionId = index.getQueueSubscriptionId();
                index.setUpdateState(IndexUpdateState.DO_NOT_SUBSCRIBE);
                indexerModel.updateIndex(index, lock);
            } finally {
                indexerModel.unlockIndex(lock);
            }
            lilyProxy.getLilyServerProxy().waitOnMQSubscription(subscriptionId, false, 60000L);
    
            // Create record
            record = repository.newRecord();
            record.setRecordType(RECORDTYPE1);
            record.setField(FIELD1, "name2");
            record = repository.create(record);
            
            // Wait for messages to be processed -- there shouldn't be any
            Assert.assertTrue("Processing messages took too long", lilyProxy.waitWalAndMQMessagesProcessed(60000L));
    
            // Record shouldn't be in index yet 
            recordIds = querySolr("name2");        
            Assert.assertFalse(recordIds.contains(record.getId()));
            
            // Trigger batch build
            Assert.assertTrue("Batch index build took too long",
                    lilyProxy.getLilyServerProxy().batchBuildIndex(indexName, 60000L * 4));
    
            // Now record should be in index 
            recordIds = querySolr("name2");        
            Assert.assertFalse(recordIds.contains(record.getId()));
        }
    }
    
    private List<RecordId> querySolr(String name) throws SolrServerException {
        SolrServer solr = lilyProxy.getSolrProxy().getSolrServer();
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setQuery(name);
        solrQuery.set("fl", "lily.id");
        QueryResponse response = solr.query(solrQuery);
        // Convert query result into a list of record IDs
        SolrDocumentList solrDocumentList = response.getResults();
        List<RecordId> recordIds = new ArrayList<RecordId>();
        for (SolrDocument solrDocument : solrDocumentList) {
            String recordId = (String) solrDocument.getFirstValue("lily.id");
            recordIds.add(repository.getIdGenerator().fromString(recordId));
        }
        return recordIds;
    }

    @Test
    public void test2() throws Exception {
        repository = lilyClient.getRepository();
        // Create schema
        TypeManager typeManager = repository.getTypeManager();
        try {
            typeManager.getFieldTypeByName(FIELD1);
            Assert.fail();
        } catch (FieldTypeNotFoundException expected) {
        }
        Assert.assertTrue(typeManager.getFieldTypes().isEmpty());
        testCreateRecord();
    }
}
