package org.lilyproject.indexer.batchbuild.test;

import static com.google.common.collect.ImmutableMap.of;
import static org.junit.Assert.assertEquals;
import static org.lilyproject.indexer.batchbuild.test.IndexerIntegrationTestUtil.CORE1;
import static org.lilyproject.indexer.batchbuild.test.IndexerIntegrationTestUtil.CORE2;
import static org.lilyproject.indexer.batchbuild.test.IndexerIntegrationTestUtil.MINS15;
import static org.lilyproject.indexer.batchbuild.test.IndexerIntegrationTestUtil.PRIMARY_INDEX;
import static org.lilyproject.indexer.batchbuild.test.IndexerIntegrationTestUtil.SECUNDARY_INDEX;
import static org.lilyproject.indexer.batchbuild.test.IndexerIntegrationTestUtil.fieldtype;
import static org.lilyproject.indexer.batchbuild.test.IndexerIntegrationTestUtil.linkField;
import static org.lilyproject.indexer.batchbuild.test.IndexerIntegrationTestUtil.rectype;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.MapSolrParams;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilyproject.lilyservertestfw.LilyProxy;
import org.lilyproject.repository.api.LRepository;
import org.lilyproject.repository.api.Link;
import org.lilyproject.repository.api.RecordId;



public class MultiRepositoryIntegrationTest {

    private static IndexerIntegrationTestUtil util;
    private static LilyProxy lilyProxy;

    @BeforeClass
    public static void startLily() throws Exception{
        lilyProxy = new LilyProxy();
        util = new IndexerIntegrationTestUtil(lilyProxy);
    }

    @AfterClass
    public static void stopLily() throws Exception{
        util.stop();
    }


    //junit 4.10 or (better yet) testng have facilities to specify the order of tests
    //until then...
    @Test
    public void testWrapper() throws Exception{
        testCreateOneRecordInEachRepo();
        testBatchReindexWorks();
        testWithReferences();
    }


    public void testCreateOneRecordInEachRepo() throws Exception{
        createRecord(util.primaryRepo, "testId", "name1");
        createRecord(util.secundaryRepo, "testId", "name2");
        waitForSepAndCommitSolr();
        assertEquals("One document per repository", 1, countDocsInRepo(CORE1));
        assertEquals("One document per repository", 1, countDocsInRepo(CORE2));
        verifyFieldValue(getAllDocs(CORE1), "USER.testId", "name1");
        verifyFieldValue(getAllDocs(CORE2), "USER.testId", "name2");
    }


    public void testBatchReindexWorks() throws Exception{
        wipeSolr(CORE1);
        wipeSolr(CORE2);
        lilyProxy.getLilyServerProxy().batchBuildIndex(PRIMARY_INDEX, MINS15);
        lilyProxy.getLilyServerProxy().batchBuildIndex(SECUNDARY_INDEX, MINS15);
        lilyProxy.getSolrProxy().commit();
        assertEquals("One document per repository", 1, countDocsInRepo(CORE1));
        assertEquals("One document per repository", 1, countDocsInRepo(CORE2));
        verifyFieldValue(getAllDocs(CORE1), "USER.testId", "name1");
        verifyFieldValue(getAllDocs(CORE2), "USER.testId", "name2");
    }

    public void testWithReferences() throws Exception{
        createRecord(util.primaryRepo, "subRec", "name3");
        createRecord(util.secundaryRepo, "subRec", "name4");
        linkToOtherRecord(util.primaryRepo);
        linkToOtherRecord(util.secundaryRepo);
        waitForSepAndCommitSolr();
        SolrDocumentList primaryDocs = getAllDocs(CORE1);
        assertEquals(2, primaryDocs.getNumFound());
        verifyDeref(primaryDocs, "USER.testId", "name3");
        SolrDocumentList secundaryDocs = getAllDocs(CORE2);
        assertEquals(2, secundaryDocs.getNumFound());
        verifyDeref(secundaryDocs, "USER.testId", "name4");
    }

    private void verifyFieldValue(SolrDocumentList docs, String recordId, String fieldValue){
        verifyDocumentValue(docs, recordId, "field1", fieldValue);
    }

    private void verifyDeref(SolrDocumentList docs, String parentRecordId, String derefFieldValue) {
        verifyDocumentValue(docs, parentRecordId, "derefField", derefFieldValue);
    }

    private void verifyDocumentValue(SolrDocumentList docs, String recordId, String fieldName, String derefFieldValue) {
        for (SolrDocument doc : docs) {
            if (doc.getFieldValue("lily.id").equals(recordId)){
                assertEquals(derefFieldValue, doc.getFieldValue(fieldName));
            }
        }
    }

    private void linkToOtherRecord(LRepository repository) throws Exception {
        RecordId id = lilyProxy.getLilyServerProxy().getClient().getDefaultRepository()
                .getIdGenerator().fromString("USER.subRec");
        repository.getDefaultTable().recordBuilder().id("testId").field(linkField, new Link(id)).update();
    }

    private void wipeSolr(String coreName) throws Exception{
        SolrServer server = lilyProxy.getSolrProxy().getSolrServer(coreName);
        server.deleteByQuery("*:*");
        server.commit();
        assertEquals(0, countDocsInRepo(coreName));
    }

    private long countDocsInRepo(String coreName) throws SolrServerException {
        SolrDocumentList results = getAllDocs(coreName);
        return results.getNumFound();
    }

    private SolrDocumentList getAllDocs(String coreName) throws SolrServerException {
        SolrServer server = lilyProxy.getSolrProxy().getSolrServer(coreName);
        QueryResponse queryResponse = server.query(new MapSolrParams(of("q", "*:*")));
        return queryResponse.getResults();
    }

    private void createRecord(LRepository repo, String recId, String fieldData) throws Exception {
        repo.getDefaultTable().recordBuilder()
                .id(recId)
                .recordType(rectype)
                .field(fieldtype, fieldData)
                .create();
    }

    private void waitForSepAndCommitSolr() throws Exception {
        lilyProxy.waitSepEventsProcessed(300000);
        lilyProxy.getSolrProxy().commit();
    }
}