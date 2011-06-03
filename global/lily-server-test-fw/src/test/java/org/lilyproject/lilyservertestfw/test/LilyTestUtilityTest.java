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

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

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
import org.lilyproject.lilyservertestfw.LilyTestUtility;
import org.lilyproject.repository.api.*;

/**
 *
 */
public class LilyTestUtilityTest {

    private static final QName RECORDTYPE1 = new QName("org.lilyproject.lilytestutility", "TestRecordType");
    private static final QName FIELD1 = new QName("org.lilyproject.lilytestutility","name");
    private static LilyTestUtility lilyTestUtilily;
    private static Repository repository;
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        // TODO use anothe kauriHome path here
        lilyTestUtilily = new LilyTestUtility("../../cr/process/server/", "org/lilyproject/lilyservertestfw/test/lilytestutility_solr_schema.xml");
        lilyTestUtilily.start();
        LilyClient lilyClient = lilyTestUtilily.getClient();
        repository = lilyClient.getRepository();
    }
    
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        lilyTestUtilily.stop();
    }
    
    @Test
    public void testCreateRecord() throws Exception {
        // Create schema
        TypeManager typeManager = repository.getTypeManager();
        FieldType fieldType1 = typeManager.createFieldType(typeManager.newFieldType(typeManager.getValueType("STRING", false, false), FIELD1, Scope.NON_VERSIONED));
        RecordType recordType1 = typeManager.newRecordType(RECORDTYPE1);
        recordType1.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType1.getId(), false));
        typeManager.createRecordType(recordType1);
        
        // Add index
        lilyTestUtilily.addIndexFromResource("org/lilyproject/lilyservertestfw/test/lilytestutility_indexerconf.xml");
       
        // Create record
        Record record = repository.newRecord();
        record.setRecordType(RECORDTYPE1);
        record.setField(FIELD1, "aName");
        record = repository.create(record);
        record = repository.read(record.getId());
        Assert.assertEquals("aName", (String)record.getField(FIELD1));
        
        // Query Solr
        SolrServer solrServer = lilyTestUtilily.getSolrServer();
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
        SolrServer solr = lilyTestUtilily.getSolrServer();
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
