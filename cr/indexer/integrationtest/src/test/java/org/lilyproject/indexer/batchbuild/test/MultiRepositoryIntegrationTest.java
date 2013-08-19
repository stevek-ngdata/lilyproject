package org.lilyproject.indexer.batchbuild.test;

import static com.google.common.collect.ImmutableMap.of;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.MapSolrParams;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilyproject.client.LilyClient;
import org.lilyproject.hadooptestfw.TestHelper;
import org.lilyproject.indexer.model.api.WriteableIndexerModel;
import org.lilyproject.indexer.model.impl.IndexDefinitionImpl;
import org.lilyproject.lilyservertestfw.LilyProxy;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.LRepository;
import org.lilyproject.repository.api.Link;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.model.api.RepositoryDefinition;
import org.lilyproject.repository.model.impl.RepositoryModelImpl;
import org.lilyproject.solrtestfw.SolrDefinition;
import org.lilyproject.util.io.Closer;

public class MultiRepositoryIntegrationTest {

    public static final String CORE1 = "collection1";
    public static final String CORE2 = "collection2";
    public static final String PRIMARY_INDEX = "primary";
    public static final String SECUNDARY_INDEX = "secundary";
    public static final long MINS15 = 15 * 60 * 1000;
    private static LilyProxy lilyProxy;
    private static WriteableIndexerModel indexerModel;
    private static LRepository primaryRepo;
    private static LRepository secundaryRepo;
    private static final String ns = "batchindex-test";
    private static QName fieldtype = new QName(ns, "field1");
    private static QName rectype = new QName(ns, "rt1");
    private static QName linkField = new QName(ns, "linkField");;

    @BeforeClass
    public static void startLily() throws Exception{
        TestHelper.setupLogging("org.lilyproject");

        byte[] schemaBytes = getResource("solrschema.xml");
        byte[] configBytes = org.lilyproject.solrtestfw.SolrDefinition.defaultSolrConfig();
        lilyProxy = new LilyProxy();
        lilyProxy.start(new SolrDefinition(
                new SolrDefinition.CoreDefinition(CORE1, schemaBytes, configBytes),
                new SolrDefinition.CoreDefinition(CORE2, schemaBytes, configBytes)
        ));
        startRepos();
        indexerModel = lilyProxy.getLilyServerProxy().getIndexerModel();
        createIndex(PRIMARY_INDEX, CORE1, primaryRepo);
        createIndex(SECUNDARY_INDEX, CORE2, secundaryRepo);
    }

    private static void createIndex(String name, String core, LRepository repository) throws Exception {
        byte[] indexConf = getResource("indexerconf.xml");
        IndexDefinitionImpl indexDef = new IndexDefinitionImpl(name);
        indexDef.setConfiguration(indexConf);
        Map<String, String> solrShards = new HashMap<String, String>();
        solrShards.put("shard1", "http://localhost:8983/solr" + "/" + core + "/");
        indexDef.setSolrShards(solrShards);
        if (! repository.getRepositoryName().equals("default"))
            indexDef.setRepositoryName(repository.getRepositoryName()); //optional for default
        indexerModel.addIndex(indexDef);
        lilyProxy.getLilyServerProxy().waitOnIndexSubscriptionId(name, MINS15);
        lilyProxy.getHBaseProxy().waitOnReplicationPeerReady("IndexUpdater_" + name);
        lilyProxy.getLilyServerProxy().waitOnIndexerRegistry(name, System.currentTimeMillis() + MINS15);
    }

    private static void startRepos() throws Exception {
        LilyClient client = lilyProxy.getLilyServerProxy().getClient();
        primaryRepo = client.getDefaultRepository();
        secundaryRepo = getAlternateTestRespository("alternateRepo");

        TypeManager typeManager = primaryRepo.getTypeManager(); //FIXME: if typemanager ever gets split between repos
        FieldType ft1 = typeManager.createFieldType("STRING", fieldtype, Scope.NON_VERSIONED);
        FieldType ft2 = typeManager.createFieldType("LINK", linkField, Scope.NON_VERSIONED);
        typeManager.recordTypeBuilder()
                .defaultNamespace(ns)
                .name(rectype)
                .fieldEntry().use(ft1).add()
                .fieldEntry().use(ft2).add()
                .create();
    }

    public static LRepository getAlternateTestRespository(String name) throws Exception {
        RepositoryModelImpl model = new RepositoryModelImpl(lilyProxy.getLilyServerProxy().getZooKeeper());
        if (!model.repositoryExistsAndActive(name)) {
            model.create(name);
            model.waitUntilRepositoryInState(name, RepositoryDefinition.RepositoryLifecycleState.ACTIVE, MINS15);
        }
        return lilyProxy.getLilyServerProxy().getClient().getRepository(name);
    }

    private static byte[] getResource(String name) throws IOException {
        return IOUtils.toByteArray(MultiRepositoryIntegrationTest.class.getResourceAsStream(name));
    }


    @AfterClass
    public static void stop() {
        Closer.close(lilyProxy);
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
        createRecord(primaryRepo, "testId", "name1");
        createRecord(secundaryRepo, "testId", "name2");
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
        createRecord(primaryRepo, "subRec", "name3");
        createRecord(secundaryRepo, "subRec", "name4");
        linkToOtherRecord(primaryRepo);
        linkToOtherRecord(secundaryRepo);
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