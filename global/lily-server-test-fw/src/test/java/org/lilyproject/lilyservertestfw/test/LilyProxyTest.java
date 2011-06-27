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
import org.lilyproject.lilyservertestfw.LilyProxy;
import org.lilyproject.repository.api.*;


public class LilyProxyTest {

    private static final QName RECORDTYPE1 = new QName("org.lilyproject.lilytestutility", "TestRecordType");
    private static final QName FIELD1 = new QName("org.lilyproject.lilytestutility","name");
    private static Repository repository;
    private static LilyProxy lilyProxy;
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        lilyProxy = new LilyProxy();
        byte[] schemaData = IOUtils.toByteArray(LilyProxyTest.class.getResourceAsStream("lilytestutility_solr_schema.xml"));
        lilyProxy.start(schemaData);
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
        FieldType fieldType1 = typeManager.createFieldType(typeManager.newFieldType(typeManager.getValueType("STRING"),
                FIELD1, Scope.NON_VERSIONED));
        RecordType recordType1 = typeManager.newRecordType(RECORDTYPE1);
        recordType1.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType1.getId(), false));
        typeManager.createRecordType(recordType1);
        
        // Add index
        lilyProxy.getLilyServerProxy().addIndexFromResource("testIndex",
                "org/lilyproject/lilyservertestfw/test/lilytestutility_indexerconf.xml");
       
        // Create record
        Record record = repository.newRecord();
        record.setRecordType(RECORDTYPE1);
        record.setField(FIELD1, "aName");
        record = repository.create(record);
        record = repository.read(record.getId());
        Assert.assertEquals("aName", (String)record.getField(FIELD1));
        
        // Query Solr
        SolrServer solrServer = lilyProxy.getSolrProxy().getSolrServer();
        List<RecordId> recordIds = new ArrayList<RecordId>();
        long waitUntil = System.currentTimeMillis() + 60000L;
        while (System.currentTimeMillis() < waitUntil) {
            recordIds = querySolr("aName");
            if (recordIds.size()>0)
                break;
            solrServer.commit();
        }
        
        Assert.assertTrue(recordIds.contains(record.getId()));
        System.out.println("Original record:" +record.getId().toString());
        System.out.println("Queried record:" +recordIds.get(0).toString());
    }
    
    private List<RecordId> querySolr(String name) throws SolrServerException {
        SolrServer solr = lilyProxy.getSolrProxy().getSolrServer();
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setQuery(name);
        solrQuery.set("fl", "lily.id");
        QueryResponse response = solr.query(solrQuery);
        // Convert query result into a list of links
        SolrDocumentList solrDocumentList = response.getResults();
        List<RecordId> recordIds = new ArrayList<RecordId>();
        for (SolrDocument solrDocument : solrDocumentList) {
            String recordId = (String) solrDocument.getFirstValue("lily.id");
            recordIds.add(repository.getIdGenerator().fromString(recordId));
        }
        return recordIds;
    }

}
