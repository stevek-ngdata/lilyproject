/*
 * Copyright 2011 Outerthought bvba
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
import org.lilyproject.lilyservertestfw.LilyProxy;
import org.lilyproject.repository.api.*;

/**
 * This test has as goal to test that data is kept when a data dir is given and the clear flag is put to false. 
 * Example usage:
 *   mvn -DargLine="-Dlily.lilyproxy.dir=/home/lilyuser/tmp/test1 -Dlily.lilyproxy.mode=embed -Dlily.lilyproxy.clear=false" \ 
 *                 test -Dtest=KeepDataTest
 */
public class KeepDataTest {
    private static final QName RECORDTYPE1 = new QName("org.lilyproject.lilytestutility", "TestRecordType");
    private static final QName FIELD1 = new QName("org.lilyproject.lilytestutility","name");
    private static Repository repository;
    private static LilyProxy lilyProxy;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        lilyProxy = new LilyProxy();
        byte[] schemaData = IOUtils.toByteArray(LilyProxyTest.class.getResourceAsStream("lilytestutility_solr_schema.xml"));
        lilyProxy.start(schemaData, null);
        LilyClient lilyClient = lilyProxy.getLilyServerProxy().getClient();
        repository = lilyClient.getRepository();
    }
    
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        lilyProxy.stop();
    }
    
    @Test
    public void testCreateRecord() throws Exception {
        // Create schema
        TypeManager typeManager = repository.getTypeManager();
        
        FieldType fieldType1 = null;
        try {
            fieldType1 = typeManager.getFieldTypeByName(FIELD1);
        } catch (FieldTypeNotFoundException e) {
            System.out.println("[KeepDataTest] Field Type does not exist yet: " + FIELD1);
        }
        
        try {
            fieldType1 = typeManager.createFieldType(typeManager.newFieldType(typeManager.getValueType("STRING"),
                FIELD1, Scope.NON_VERSIONED));
        } catch (FieldTypeExistsException e) {
            System.out.println("[KeepDataTest] Field Type already exists: " + FIELD1);
        }
        
        RecordType recordType1 = null;
        try {
            recordType1 = typeManager.getRecordTypeByName(RECORDTYPE1, 1L);
            System.out.println("[KeepDataTest] RecordType already exists: " + recordType1);
        } catch (RecordTypeNotFoundException e) {
            System.out.println("[KeepDataTest] RecordType does not exist yet. Create it: " + recordType1);
            recordType1 = typeManager.newRecordType(RECORDTYPE1);
            recordType1.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType1.getId(), false));
            typeManager.createRecordType(recordType1);
        }
        
        // Add index
        String indexName = "testIndex";
        lilyProxy.getLilyServerProxy().addIndexFromResource(indexName,
                "org/lilyproject/lilyservertestfw/test/lilytestutility_indexerconf.xml", 60000L);
        
        // Create an extra record
        RecordId recordId = null;
        int i = 0;
        try {
            while(true) {
                recordId = repository.getIdGenerator().newRecordId("MyRecord" + i++);
                Record read = repository.read(recordId);
                System.out.println("[KeepDataTest] Record already exists: " + recordId);
            }
        } catch (RecordNotFoundException e) {
            System.out.println("[KeepDataTest] Record does not exist yet. Create it: " + recordId);
            Record record = repository.newRecord(recordId);
            record.setRecordType(RECORDTYPE1);
            record.setField(FIELD1, "name1");
            record = repository.create(record);
        }
        Record record = repository.read(recordId);
        Assert.assertEquals("name1", (String)record.getField(FIELD1));
        
        // Wait for messages to be processed
        Assert.assertTrue("Processing messages took too long", lilyProxy.waitWalAndMQMessagesProcessed(60000L));
        
        // Query Solr and assert all previously created records are indexed
        List<RecordId> recordIds = querySolr("name1");
        for (RecordId recordId2 : recordIds) {
            System.out.println("[KeepDataTest] RecordId from query : " + recordId2);
        }
        for (int j = 0; j < i; j++) {
            RecordId expectedRecordId = repository.getIdGenerator().newRecordId("MyRecord" + j);
            Assert.assertTrue("Expected " + expectedRecordId + " to be in query result", recordIds.contains(expectedRecordId));
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
}
