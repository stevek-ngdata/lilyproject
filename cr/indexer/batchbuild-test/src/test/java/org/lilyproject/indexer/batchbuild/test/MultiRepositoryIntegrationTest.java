package org.lilyproject.indexer.batchbuild.test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilyproject.client.LilyClient;
import org.lilyproject.indexer.model.api.WriteableIndexerModel;
import org.lilyproject.indexer.model.impl.IndexDefinitionImpl;
import org.lilyproject.lilyservertestfw.LilyProxy;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.LRepository;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.model.api.RepositoryDefinition;
import org.lilyproject.repository.model.impl.RepositoryModelImpl;
import org.lilyproject.solrtestfw.SolrDefinition;
import org.lilyproject.util.io.Closer;

public class MultiRepositoryIntegrationTest {

    public static final String CORE1 = "collection1";
    public static final String CORE2 = "collection2";
    private static LilyProxy lilyProxy;
    private static WriteableIndexerModel indexerModel;
    private static LRepository primaryRepo;
    private static LRepository secundaryRepo;
    private static final String ns = "batchindex-test";
    private static QName fieldtype = new QName(ns, "field1");
    private static QName rectype = new QName(ns, "rt1");

    @BeforeClass
    public static void startLily() throws Exception{
        byte[] schemaBytes = getResource("solrschema.xml");
        byte[] configBytes = org.lilyproject.solrtestfw.SolrDefinition.defaultSolrConfig();
        lilyProxy = new LilyProxy();
        lilyProxy.start(new SolrDefinition(
                new SolrDefinition.CoreDefinition(CORE1, schemaBytes, configBytes),
                new SolrDefinition.CoreDefinition(CORE2, schemaBytes, configBytes)
        ));
        startRepos();
        indexerModel = lilyProxy.getLilyServerProxy().getIndexerModel();
        createIndex("primary", CORE1, primaryRepo);
        createIndex("secundary", CORE2, secundaryRepo);
    }

    private static void createIndex(String name, String core, LRepository repository) throws Exception {
        byte[] indexConf = getResource("indexerconf.xml");
        IndexDefinitionImpl indexDef = new IndexDefinitionImpl(name);
        indexDef.setConfiguration(indexConf);
        Map<String, String> solrShards = new HashMap<String, String>();
        solrShards.put("shard1", "http://localhost:8983/solr" + "/" + core + "/");
        indexDef.setSolrShards(solrShards);
        indexDef.setRepositoryName(repository.getRepositoryName());
        indexerModel.addIndex(indexDef);
        lilyProxy.getLilyServerProxy().waitOnIndexSubscriptionId(name, 300000);
        lilyProxy.getHBaseProxy().waitOnReplicationPeerReady("IndexUpdater_" + name);
        lilyProxy.getLilyServerProxy().waitOnIndexerRegistry(name, System.currentTimeMillis() + 300000);
    }

    private static void startRepos() throws Exception {
        LilyClient client = lilyProxy.getLilyServerProxy().getClient();
        primaryRepo = client.getDefaultRepository();
        secundaryRepo = getAlternateTestRespository("alternateRepo");

        TypeManager typeManager = primaryRepo.getTypeManager(); //FIXME: if typemanager ever gets split between repos
        FieldType ft1 = typeManager.createFieldType("STRING", fieldtype, Scope.NON_VERSIONED);
        FieldType ft2 = typeManager.createFieldType("LINK", new QName(ns, "linkField"), Scope.NON_VERSIONED);
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
            model.waitUntilRepositoryInState(name, RepositoryDefinition.RepositoryLifecycleState.ACTIVE, 100000);
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

    @Test
    public void createRecordInDefaultRepository() throws Exception {
        primaryRepo.getDefaultTable().recordBuilder()
                .id("test1")
                .recordType(rectype)
                .field(fieldtype, "test1name")
                .create();
        lilyProxy.waitSepEventsProcessed(300000);
        lilyProxy.getSolrProxy().commit(CORE1);
    }

    @Test
    public void createRecordInAlternateRepository() throws Exception {
        secundaryRepo.getDefaultTable().recordBuilder()
                .recordType(rectype)
                .id("test2")
                .field(fieldtype, "test2name")
                .create();
        lilyProxy.waitSepEventsProcessed(300000);
        lilyProxy.getSolrProxy().commit(CORE2);
    }
}