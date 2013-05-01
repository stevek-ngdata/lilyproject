/*
 * Copyright 2012 NGDATA nv
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
package org.lilyproject.indexer.batchbuild.test;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilyproject.client.LilyClient;
import org.lilyproject.indexer.derefmap.DependantRecordIdsIterator;
import org.lilyproject.indexer.derefmap.DerefMap;
import org.lilyproject.indexer.derefmap.DerefMapHbaseImpl;
import org.lilyproject.indexer.model.api.IndexBatchBuildState;
import org.lilyproject.indexer.model.api.IndexDefinition;
import org.lilyproject.indexer.model.api.IndexUpdateState;
import org.lilyproject.indexer.model.api.WriteableIndexerModel;
import org.lilyproject.lilyservertestfw.LilyProxy;
import org.lilyproject.lilyservertestfw.LilyServerProxy;
import org.lilyproject.repository.api.AbsoluteRecordId;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.LRepository;
import org.lilyproject.repository.api.LTable;
import org.lilyproject.repository.api.Link;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.solrtestfw.SolrProxy;
import org.lilyproject.util.hbase.LilyHBaseSchema.Table;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.json.JsonFormat;
import org.lilyproject.util.repo.VersionTag;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class BatchBuildTest {
    private static LilyProxy lilyProxy;
    private static LilyClient lilyClient;
    private static LRepository repository;
    private static LTable table;
    private static TypeManager typeManager;
    private static SolrServer solrServer;
    private static SolrProxy solrProxy;
    private static LilyServerProxy lilyServerProxy;
    private static WriteableIndexerModel model;
    private static int BUILD_TIMEOUT = 240000;

    private FieldType ft1;
    private FieldType ft2;
    private RecordType rt1;

    private final static String INDEX_NAME = "batchtest";
    private static final String COUNTER_NUM_FAILED_RECORDS =
            "org.lilyproject.indexer.batchbuild.IndexBatchBuildCounters:NUM_FAILED_RECORDS";

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        lilyProxy = new LilyProxy();

        InputStream is = BatchBuildTest.class.getResourceAsStream("solrschema.xml");
        byte[] solrSchema = IOUtils.toByteArray(is);
        IOUtils.closeQuietly(is);

        lilyProxy.start(solrSchema);

        solrProxy = lilyProxy.getSolrProxy();
        solrServer = solrProxy.getSolrServer();
        lilyServerProxy = lilyProxy.getLilyServerProxy();
        lilyClient = lilyServerProxy.getClient();
        repository = lilyClient.getPublicRepository();
        table = repository.getDefaultTable();

        typeManager = repository.getTypeManager();
        FieldType ft1 = typeManager.createFieldType("STRING", new QName("batchindex-test", "field1"),
                Scope.NON_VERSIONED);
        FieldType ft2 =
                typeManager.createFieldType("LINK", new QName("batchindex-test", "linkField"), Scope.NON_VERSIONED);
        typeManager.recordTypeBuilder()
                .defaultNamespace("batchindex-test")
                .name("rt1")
                .fieldEntry().use(ft1).add()
                .fieldEntry().use(ft2).add()
                .create();

        model = lilyServerProxy.getIndexerModel();

        is = BatchBuildTest.class.getResourceAsStream("indexerconf.xml");
        byte[] indexerConfiguration = IOUtils.toByteArray(is);
        IOUtils.closeQuietly(is);

        IndexDefinition index = model.newIndex(INDEX_NAME);
        Map<String, String> solrShards = new HashMap<String, String>();
        solrShards.put("shard1", "http://localhost:8983/solr");
        index.setSolrShards(solrShards);
        index.setConfiguration(indexerConfiguration);
        index.setUpdateState(IndexUpdateState.DO_NOT_SUBSCRIBE);
        model.addIndex(index);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        Closer.close(lilyClient);
        Closer.close(solrServer);
        Closer.close(solrProxy);
        Closer.close(lilyServerProxy);

        lilyProxy.stop();
    }

    @Before
    public void setup() throws Exception {
        this.ft1 = typeManager.getFieldTypeByName(new QName("batchindex-test", "field1"));
        this.ft2 = typeManager.getFieldTypeByName(new QName("batchindex-test", "linkField"));
        this.rt1 = typeManager.getRecordTypeByName(new QName("batchindex-test", "rt1"), null);
    }

    @Test
    public void testBatchIndex() throws Exception {
        String assertId = "batch-index-test";
        //
        // First create some content
        //
        table.recordBuilder()
                .id("batch-index-test")
                .recordType(rt1.getName())
                .field(ft1.getName(), "test1")
                .create();

        this.buildAndCommit();

        QueryResponse response = solrServer.query(new SolrQuery("field1:test1*"));
        assertEquals(1, response.getResults().size());
        assertEquals("USER." + assertId, response.getResults().get(0).getFieldValue("lily.id"));
    }

    /**
     * Test if the default batch index conf setting works
     *
     * @throws Exception
     */
    @Test
    public void testDefaultBatchIndexConf() throws Exception {
        byte[] defaultConf = getResourceAsByteArray("defaultBatchIndexConf-test2.json");
        setBatchIndexConf(defaultConf, null, false);

        String assertId = "batch-index-test2";
        //
        // First create some content
        //
        table.recordBuilder()
                .id(assertId)
                .recordType(rt1.getName())
                .field(ft1.getName(), "test2 index")
                .create();

        table.recordBuilder()
                .id("batch-noindex-test2")
                .recordType(rt1.getName())
                .field(ft1.getName(), "test2 noindex")
                .create();

        // Now start the batch index
        this.buildAndCommit();

        // Check if 1 record and not 2 are in the index
        QueryResponse response = solrServer.query(new SolrQuery("field1:test2*"));
        assertEquals(1, response.getResults().size());
        assertEquals("USER." + "batch-index-test2", response.getResults().get(0).getFieldValue("lily.id"));
        // check that  the last used batch index conf = default
        IndexDefinition index = model.getMutableIndex(INDEX_NAME);

        assertEquals(JsonFormat.deserialize(defaultConf), JsonFormat.deserialize(
                index.getLastBatchBuildInfo().getBatchIndexConfiguration()));
    }

    /**
     * Test setting a custom batch index conf.
     *
     * @throws Exception
     */
    @Test
    public void testCustomBatchIndexConf() throws Exception {
        byte[] defaultConf = getResourceAsByteArray("defaultBatchIndexConf-test2.json");
        setBatchIndexConf(defaultConf, null, false);

        String assertId1 = "batch-index-custom-test3";
        String assertId2 = "batch-index-test3";
        //
        // First create some content
        //
        Record recordToChange1 = table.recordBuilder()
                .id(assertId2)
                .recordType(rt1.getName())
                .field(ft1.getName(), "test3 index run1")
                .create();

        Record recordToChange2 = table.recordBuilder()
                .id(assertId1)
                .recordType(rt1.getName())
                .field(ft1.getName(), "test3 index run1")
                .create();

        table.recordBuilder()
                .id("batch-noindex-test3")
                .recordType(rt1.getName())
                .field(ft1.getName(), "test3 noindex run1")
                .create();

        // Index everything with the default conf
        this.buildAndCommit();

        SolrDocumentList results = solrServer.query(new SolrQuery("field1:test3*").
                addSortField("lily.id", ORDER.asc)).getResults();
        assertEquals(2, results.size());
        assertEquals("USER." + assertId1, results.get(0).getFieldValue("lily.id"));
        assertEquals("USER." + assertId2, results.get(1).getFieldValue("lily.id"));

        // change some fields and reindex using a specific configuration. Only one of the 2 changes should be picked up
        recordToChange1.setField(ft1.getName(), "test3 index run2");
        recordToChange2.setField(ft1.getName(), "test3 index run2");
        table.update(recordToChange1);
        table.update(recordToChange2);

        byte[] batchConf = getResourceAsByteArray("batchIndexConf-test3.json");
        setBatchIndexConf(defaultConf, batchConf, true);

        waitForIndexAndCommit(BUILD_TIMEOUT);

        // Check if 1 record and not 2 are in the index
        QueryResponse response = solrServer.query(new SolrQuery("field1:test3\\ index\\ run2"));
        assertEquals(1, response.getResults().size());
        assertEquals("USER." + assertId1, response.getResults().get(0).getFieldValue("lily.id"));
        // check that the last used batch index conf = default
        assertEquals(JsonFormat.deserialize(batchConf), JsonFormat.deserialize(
                model.getMutableIndex(INDEX_NAME).getLastBatchBuildInfo().getBatchIndexConfiguration()));

        // Set things up for run 3 where the default configuration should be used again
        recordToChange1.setField(ft1.getName(), "test3 index run3");
        recordToChange2.setField(ft1.getName(), "test3 index run3");
        table.update(recordToChange1);
        table.update(recordToChange2);
        // Now rebuild the index and see if the default indexer has kicked in
        this.buildAndCommit();

        response = solrServer.query(new SolrQuery("field1:test3\\ index\\ run3").
                addSortField("lily.id", ORDER.asc));
        assertEquals(2, response.getResults().size());
        assertEquals("USER." + assertId1, response.getResults().get(0).getFieldValue("lily.id"));
        assertEquals("USER." + assertId2, response.getResults().get(1).getFieldValue("lily.id"));
        // check that the last used batch index conf = default
        assertEquals(JsonFormat.deserialize(defaultConf), JsonFormat.deserialize(
                model.getMutableIndex(INDEX_NAME).getLastBatchBuildInfo().getBatchIndexConfiguration()));
    }

    /**
     * This test should cause a failure when adding a custom batchindex conf without setting a buildrequest
     *
     * @throws Exception
     */
    @Test(expected = org.lilyproject.indexer.model.api.IndexValidityException.class)
    public void testCustomBatchIndexConf_NoBuild() throws Exception {
        setBatchIndexConf(getResourceAsByteArray("defaultBatchIndexConf-test2.json"),
                getResourceAsByteArray("batchIndexConf-test3.json"), false);
    }

    private byte[] getResourceAsByteArray(String name) throws IOException {
        InputStream is = null;
        try {
            is = BatchBuildTest.class.getResourceAsStream(name);
            return IOUtils.toByteArray(is);
        } finally {
            IOUtils.closeQuietly(is);
        }
    }

    @Test
    public void testClearDerefMap() throws Exception {
        DerefMap derefMap = DerefMapHbaseImpl
                .create(INDEX_NAME, lilyProxy.getHBaseProxy().getConf(), null, repository.getIdGenerator());

        Record linkedRecord = table.recordBuilder()
                .id("deref-test-linkedrecord")
                .recordType(rt1.getName())
                .field(ft1.getName(), "deref test linkedrecord")
                .create();

        Record record = table.recordBuilder()
                .id("deref-test-main")
                .recordType(rt1.getName())
                .field(ft1.getName(), "deref test main")
                .field(ft2.getName(), new Link(linkedRecord.getId()))
                .create();

        SchemaId vtag = typeManager.getFieldTypeByName(VersionTag.LAST).getId();
        DependantRecordIdsIterator it = null;

        try {
            it = derefMap.findDependantsOf(absId(linkedRecord.getId()),
                    ft1.getId(), vtag);
            assertTrue(!it.hasNext());
        } finally {
            it.close();
        }

        setBatchIndexConf(getResourceAsByteArray("batchIndexConf-testClearDerefmap-false.json"), null, false);

        buildAndCommit();

        QueryResponse response = solrServer.query(new SolrQuery("field1:deref\\ test\\ main"));
        assertEquals(1, response.getResults().size());

        try {
            it = derefMap.findDependantsOf(absId(linkedRecord.getId()), ft1.getId(), vtag);
            assertTrue(it.hasNext());
        } finally {
            it.close();
        }

        setBatchIndexConf(null, getResourceAsByteArray("batchIndexConf-testClearDerefmap-true.json"), true);
        waitForIndexAndCommit(BUILD_TIMEOUT);

        try {
            it = derefMap.findDependantsOf(absId(linkedRecord.getId()), ft1.getId(), vtag);
            assertTrue(!it.hasNext());
        } finally {
            it.close();
        }
    }

    private void buildAndCommit() throws Exception {
        lilyServerProxy.batchBuildIndex(INDEX_NAME, BUILD_TIMEOUT);
        solrServer.commit();
    }

    private void waitForIndexAndCommit(long timeout) throws Exception {
        boolean indexSuccess = false;
        try {
            // Now wait until its finished
            long tryUntil = System.currentTimeMillis() + timeout;
            while (System.currentTimeMillis() < tryUntil) {
                Thread.sleep(100);
                IndexDefinition definition = model.getIndex(INDEX_NAME);
                if (definition.getBatchBuildState() == IndexBatchBuildState.INACTIVE) {
                    Long amountFailed = definition.getLastBatchBuildInfo().getCounters()
                            .get(COUNTER_NUM_FAILED_RECORDS);
                    boolean successFlag = definition.getLastBatchBuildInfo().getSuccess();
                    indexSuccess = successFlag && (amountFailed == null || amountFailed == 0L);
                    if (!indexSuccess) {
                        fail("Batch index build did not finish successfully: success flag = " +
                                successFlag + ", amount failed records = " + amountFailed + ", job state = " +
                                definition.getLastBatchBuildInfo().getJobState() + ", job id = " +
                                definition.getLastBatchBuildInfo().getJobId() + ", job url = " +
                                definition.getLastBatchBuildInfo().getTrackingUrl());
                    } else {
                        break;
                    }
                }
            }
        } catch (Exception e) {
            throw new Exception("Error checking if batch index job ended.", e);
        }

        if (!indexSuccess) {
            fail("Batch build did not end after " + BUILD_TIMEOUT + " millis");
        } else {
            solrServer.commit();
        }
    }

    private static void setBatchIndexConf(byte[] defaultConf, byte[] customConf, boolean buildNow) throws Exception {
        String lock = model.lockIndex(INDEX_NAME);

        try {
            IndexDefinition index = model.getMutableIndex(INDEX_NAME);
            if (defaultConf != null) {
                index.setDefaultBatchIndexConfiguration(defaultConf);
            }
            if (customConf != null) {
                index.setBatchIndexConfiguration(customConf);
            }
            if (buildNow) {
                index.setBatchBuildState(IndexBatchBuildState.BUILD_REQUESTED);
            }

            model.updateIndex(index, lock);
        } finally {
            model.unlockIndex(lock);
        }
    }

    private static AbsoluteRecordId absId(RecordId recordId) {
        return lilyClient.getIdGenerator().newAbsoluteRecordId(Table.RECORD.name, recordId);
    }
}
