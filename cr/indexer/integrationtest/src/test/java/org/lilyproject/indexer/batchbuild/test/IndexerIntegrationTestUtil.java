package org.lilyproject.indexer.batchbuild.test;

import java.io.IOException;
import java.util.Map;

import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.ngdata.hbaseindexer.SolrConnectionParams;
import com.ngdata.hbaseindexer.model.api.IndexerDefinitionBuilder;
import com.ngdata.hbaseindexer.model.api.WriteableIndexerModel;
import org.lilyproject.hadooptestfw.HBaseProxy;
import org.lilyproject.hadooptestfw.TestHelper;
import org.lilyproject.indexer.hbase.mapper.LilyIndexerComponentFactory;
import org.lilyproject.lilyservertestfw.LilyProxy;
import org.lilyproject.lilyservertestfw.launcher.HbaseIndexerLauncherService;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.LRepository;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.model.api.RepositoryDefinition;
import org.lilyproject.repository.model.impl.RepositoryModelImpl;
import org.lilyproject.solrtestfw.SolrDefinition;
import org.lilyproject.util.io.Closer;

class IndexerIntegrationTestUtil {
    public static final String CORE1 = "collection1";
    public static final String CORE2 = "collection2";
    public static final String PRIMARY_INDEX = "primary";
    public static final String SECUNDARY_INDEX = "secundary";
    public static final long MINS15 = 15 * 60 * 1000;
    public static final String ns = "batchindex-test";
    public static final QName fieldtype = new QName(ns, "field1");
    public static final QName rectype = new QName(ns, "rt1");
    public static final QName linkField = new QName(ns, "linkField");

    public final LilyProxy lilyProxy;
    public final LRepository primaryRepo;
    public final LRepository secundaryRepo;

    private static WriteableIndexerModel indexerModel;

    private static HbaseIndexerLauncherService hbaseIndexerLauncherService;

    IndexerIntegrationTestUtil(LilyProxy lilyProxy) throws Exception{
        this.lilyProxy = lilyProxy;
        startLily();
        primaryRepo = lilyProxy.getLilyServerProxy().getClient().getDefaultRepository();
        secundaryRepo = getAlternateTestRespository("alternateRepo");
    }


    public void startLily() throws Exception{
        TestHelper.setupLogging("org.lilyproject");

        byte[] schemaBytes = getResource("solrschema.xml");
        byte[] configBytes = org.lilyproject.solrtestfw.SolrDefinition.defaultSolrConfig();

        lilyProxy.start(new SolrDefinition(
                new SolrDefinition.CoreDefinition(CORE1, schemaBytes, configBytes),
                new SolrDefinition.CoreDefinition(CORE2, schemaBytes, configBytes)
        ));

        hbaseIndexerLauncherService = new HbaseIndexerLauncherService();
        hbaseIndexerLauncherService.setup(null, null, false);
        hbaseIndexerLauncherService.start(null);

        configureLilySchema();
        indexerModel = lilyProxy.getLilyServerProxy().getIndexerModel();
        LRepository primaryRepo = lilyProxy.getLilyServerProxy().getClient().getDefaultRepository();
        LRepository secundaryRepo = getAlternateTestRespository("alternateRepo");



        createIndex(PRIMARY_INDEX, CORE1, primaryRepo);
        createIndex(SECUNDARY_INDEX, CORE2, secundaryRepo);
    }

    void createIndex(String name, String core, LRepository repository) throws Exception {
        byte[] indexConf = getResource("indexerconf.xml");
        Map<String,String> connectionParams = Maps.newHashMap();
        connectionParams.put(SolrConnectionParams.ZOOKEEPER, "localhost:2181/solr");
        connectionParams.put(SolrConnectionParams.COLLECTION, core);
        indexerModel.addIndexer(new IndexerDefinitionBuilder()
                .name(name)
                .connectionType("solr")
                .connectionParams(connectionParams)
                .indexerComponentFactory(LilyIndexerComponentFactory.class.getName())
                .configuration(indexConf)
                /*
                 Map<String, String> solrShards = new HashMap<String, String>();
        solrShards.put("shard1", "http://localhost:8983/solr" + "/" + core + "/");
        indexDef.setSolrShards(solrShards);
        if (! repository.getRepositoryName().equals("default"))
            indexDef.setRepositoryName(repository.getRepositoryName()); //optional for default
                */
                .build());
        lilyProxy.getLilyServerProxy().waitOnIndexSubscriptionId(name, MINS15);
        if (lilyProxy.getHBaseProxy().getMode() != HBaseProxy.Mode.CONNECT)
            lilyProxy.getHBaseProxy().waitOnReplicationPeerReady("Indexer_" + name);
        lilyProxy.getLilyServerProxy().waitOnIndexerRegistry(name, System.currentTimeMillis() + MINS15);
    }

    void configureLilySchema() throws Exception {
        LRepository defaultRepository = lilyProxy.getLilyServerProxy().getClient().getDefaultRepository();
        TypeManager typeManager = defaultRepository.getTypeManager(); //FIXME: if typemanager ever gets split between repos
        FieldType ft1 = typeManager.createFieldType("STRING", fieldtype, Scope.NON_VERSIONED);
        FieldType ft2 = typeManager.createFieldType("LINK", linkField, Scope.NON_VERSIONED);
        typeManager.recordTypeBuilder()
                .defaultNamespace(ns)
                .name(rectype)
                .fieldEntry().use(ft1).add()
                .fieldEntry().use(ft2).add()
                .create();
    }

    public LRepository getAlternateTestRespository(String name) throws Exception {
        RepositoryModelImpl model = new RepositoryModelImpl(lilyProxy.getLilyServerProxy().getZooKeeper());
        if (!model.repositoryExistsAndActive(name)) {
            model.create(name);
            model.waitUntilRepositoryInState(name, RepositoryDefinition.RepositoryLifecycleState.ACTIVE, MINS15);
        }
        return lilyProxy.getLilyServerProxy().getClient().getRepository(name);
    }

    private byte[] getResource(String name) throws IOException {
        return ByteStreams.toByteArray(MultiRepositoryIntegrationTest.class.getResourceAsStream(name));
    }

    public void stop() {
        hbaseIndexerLauncherService.stop();
        Closer.close(lilyProxy);
    }
}
