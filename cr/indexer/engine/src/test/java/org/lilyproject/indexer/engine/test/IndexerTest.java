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
package org.lilyproject.indexer.engine.test;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.ngdata.sep.EventListener;
import com.ngdata.sep.SepEvent;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilyproject.hadooptestfw.CleanupUtil;
import org.lilyproject.hadooptestfw.TestHelper;
import org.lilyproject.indexer.derefmap.DerefMap;
import org.lilyproject.indexer.derefmap.DerefMapHbaseImpl;
import org.lilyproject.indexer.engine.ClassicSolrShardManager;
import org.lilyproject.indexer.engine.IndexLocker;
import org.lilyproject.indexer.engine.IndexUpdater;
import org.lilyproject.indexer.engine.IndexUpdaterMetrics;
import org.lilyproject.indexer.engine.Indexer;
import org.lilyproject.indexer.engine.IndexerMetrics;
import org.lilyproject.indexer.engine.SolrClientException;
import org.lilyproject.indexer.integration.IndexRecordFilterHook;
import org.lilyproject.indexer.model.api.IndexDefinition;
import org.lilyproject.indexer.model.api.WriteableIndexerModel;
import org.lilyproject.indexer.model.impl.IndexerModelImpl;
import org.lilyproject.indexer.model.indexerconf.DerefValue;
import org.lilyproject.indexer.model.indexerconf.Follow;
import org.lilyproject.indexer.model.indexerconf.ForwardVariantFollow;
import org.lilyproject.indexer.model.indexerconf.IndexField;
import org.lilyproject.indexer.model.indexerconf.IndexFields;
import org.lilyproject.indexer.model.indexerconf.IndexerConf;
import org.lilyproject.indexer.model.indexerconf.IndexerConfBuilder;
import org.lilyproject.indexer.model.indexerconf.IndexerConfException;
import org.lilyproject.indexer.model.indexerconf.MappingNode;
import org.lilyproject.indexer.model.indexerconf.VariantFollow;
import org.lilyproject.indexer.model.util.IndexInfo;
import org.lilyproject.indexer.model.util.IndexesInfo;
import org.lilyproject.indexer.model.util.IndexesInfoImpl;
import org.lilyproject.repository.api.Blob;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.HierarchyPath;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.IdRecord;
import org.lilyproject.repository.api.Link;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordBuilder;
import org.lilyproject.repository.api.RecordFactory;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.RepositoryManager;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.api.ValueType;
import org.lilyproject.repository.spi.BaseRepositoryDecorator;
import org.lilyproject.repository.spi.RecordUpdateHook;
import org.lilyproject.repotestfw.RepositorySetup;
import org.lilyproject.solrtestfw.SolrDefinition;
import org.lilyproject.solrtestfw.SolrTestingUtility;
import org.lilyproject.util.Pair;
import org.lilyproject.util.hbase.LilyHBaseSchema.Table;
import org.lilyproject.util.repo.PrematureRepositoryManager;
import org.lilyproject.util.repo.PrematureRepositoryManagerImpl;
import org.lilyproject.util.repo.RecordEvent;
import org.lilyproject.util.repo.VersionTag;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.lilyproject.util.repo.RecordEvent.Type.CREATE;
import static org.lilyproject.util.repo.RecordEvent.Type.DELETE;
import static org.lilyproject.util.repo.RecordEvent.Type.UPDATE;

public class IndexerTest {

    public static final String ALTERNATE_TABLE = "alternate";

    private final static RepositorySetup repoSetup = new RepositorySetup();
    private static IndexerConf INDEXER_CONF;
    private static SolrTestingUtility SOLR_TEST_UTIL;
    private static RepositoryManager repositoryManager;
    private static Repository defaultRepository;
    private static Repository alternateRepository;
    private static TypeManager typeManager;
    private static IdGenerator idGenerator;
    private static ClassicSolrShardManager solrShardManager;
    private static DerefMap derefMap;
    private static WriteableIndexerModel indexerModel;
    private static IndexesInfo indexesInfo;
    private static TrackingRepositoryManager indexUpdaterRepositoryMgr;

    private static FieldType nvTag;
    private static FieldType liveTag;
    private static FieldType previewTag;
    private static FieldType latestTag;
    private static FieldType lastTag;

    private static FieldType nvfield1;
    private static FieldType nvfield2;
    private static FieldType nvLinkField1;
    private static FieldType nvLinkField2;

    private static FieldType vfield1;
    private static FieldType vfield2;
    private static FieldType vLinkField1;
    private static FieldType vLinkField2;

    private static FieldType vStringMvField;
    private static FieldType vLongField;
    private static FieldType vBlobField;
    private static FieldType vBlobMvHierField;
    private static FieldType vBlobNestedField;
    private static FieldType vDateTimeField;
    private static FieldType vDateField;
    private static FieldType vIntHierField;

    private static final String NS = "org.lilyproject.indexer.test";
    private static final String NS2 = "org.lilyproject.indexer.test.2";
    private static final String DYN_NS1 = "org.lilyproject.indexer.test.dyn1";
    private static final String DYN_NS2 = "org.lilyproject.indexer.test.dyn2";

    private static Log log = LogFactory.getLog(IndexerTest.class);

    private static MessageVerifier messageVerifier = new MessageVerifier();
    private static OtherListener otherListener = new OtherListener();

    private static RecordType nvRecordType1;
    private static RecordType vRecordType1;
    private static RecordType lastRecordType;

    private static Map<String, FieldType> fields = Maps.newHashMap();
    private final Map<String, Integer> matchResultCounts = Maps.newHashMap();

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {

        SOLR_TEST_UTIL = new SolrTestingUtility();
        SOLR_TEST_UTIL.setSolrDefinition(
                new SolrDefinition(IOUtils.toByteArray(IndexerTest.class.getResourceAsStream("schema1.xml"))));

        TestHelper.setupLogging("org.lilyproject.indexer", "org.lilyproject.indexer.engine",
                "org.lilyproject.indexer.engine.test.IndexerTest");

        SOLR_TEST_UTIL.start();

        repoSetup.setupCore();

        indexerModel = new IndexerModelImpl(repoSetup.getZk());
        PrematureRepositoryManager prematureRepositoryManager = new PrematureRepositoryManagerImpl();

        indexesInfo = new IndexesInfoImpl(indexerModel, prematureRepositoryManager);
        RecordUpdateHook hook = new IndexRecordFilterHook(indexesInfo);

        repoSetup.setRecordUpdateHooks(Collections.singletonList(hook));

        repoSetup.setupRepository();

        prematureRepositoryManager.setRepositoryManager(repoSetup.getRepositoryManager());
        repositoryManager = repoSetup.getRepositoryManager();
        repoSetup.getTableManager().createTable(ALTERNATE_TABLE);

        defaultRepository = repositoryManager.getRepository(Table.RECORD.name);
        alternateRepository = repositoryManager.getRepository(ALTERNATE_TABLE);
        indexUpdaterRepositoryMgr = new TrackingRepositoryManager(repoSetup.getRepositoryManager());


        typeManager = repoSetup.getTypeManager();
        idGenerator = defaultRepository.getIdGenerator();

        // Field types should exist before the indexer conf is loaded
        setupSchema();

        solrShardManager = ClassicSolrShardManager.createForOneShard(SOLR_TEST_UTIL.getUri());
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        // cleanup the last created index (especially the SEP part), this is important when running tests in connect mode
        cleanupIndex("test" + idxChangeCnt);

        repoSetup.stop();

        if (SOLR_TEST_UTIL != null) {
            SOLR_TEST_UTIL.stop();
        }
    }

    // augmented each time we change the indexerconf, to give the indexes unique names
    private static int idxChangeCnt = 0;

    public static void changeIndexUpdater(String confName) throws Exception {
        String prevIndexName = "test" + idxChangeCnt;
        idxChangeCnt++;
        String indexName = "test" + idxChangeCnt;

        System.out.println("changeIndexUpdater invocation " + idxChangeCnt + " - " + confName);

        // First clean up stuff of old index, to be sure this also gets executed in case of invalid indexerconf
        cleanupIndex(prevIndexName);
        waitForIndexesInfoUpdate(0);

        // warning: the below line will throw an exception in case of invalid conf, which is an exception
        // which some test cases expect, and hence it won't be visible but will cause the remainder of the
        // code in this method not to be executed! (so keep this in mind for anything related to resource cleanup)
        INDEXER_CONF = IndexerConfBuilder.build(IndexerTest.class.getResourceAsStream(confName), repoSetup.getRepositoryManager());
        IndexLocker indexLocker = new IndexLocker(repoSetup.getZk(), false);

        Configuration hbaseConf = repoSetup.getHadoopConf();
        if (derefMap != null) {
            // We don't call the following:
            //    DerefMapHbaseImpl.delete("test", hbaseConf);
            // because deleting / creating the tables during the test is very slow.
            // Instead we just delete all rows within the table.
            for (String tableName : ImmutableList.of("deref-forward-test", "deref-backward-test")) {
                HTable htable = new HTable(hbaseConf, tableName);
                CleanupUtil.clearTable(htable);
                htable.close();
            }
        }
        derefMap = DerefMapHbaseImpl.create("test", hbaseConf, null, repositoryManager.getIdGenerator());
        Indexer indexer = new Indexer(indexName, INDEXER_CONF, repositoryManager, solrShardManager, indexLocker,
                new IndexerMetrics(indexName), derefMap);

        // The registration of the index into the IndexerModel is only needed for the IndexRecordFilterHook
        IndexDefinition indexDef = indexerModel.newIndex(indexName);
        indexDef.setConfiguration(IOUtils.toByteArray(IndexerTest.class.getResourceAsStream(confName)));
        indexDef.setSolrShards(Collections.singletonMap("shard1", "http://somewhere/"));
        indexDef.setQueueSubscriptionId("IndexUpdater_" + indexName);

        indexerModel.addIndex(indexDef);

        repoSetup.getSepModel().addSubscription(indexDef.getQueueSubscriptionId());

        repoSetup.getHBaseProxy().waitOnReplicationPeerReady("IndexUpdater_" + indexName);

        IndexUpdater indexUpdater = new IndexUpdater(indexer, indexUpdaterRepositoryMgr, indexLocker,
                new IndexUpdaterMetrics(indexName), derefMap, repoSetup.getEventPublisherManager(), "IndexUpdater_" + indexName);
        repoSetup.startSepEventSlave("IndexUpdater_" + indexName,
                new CompositeEventListener(indexUpdater, messageVerifier, otherListener));

        waitForIndexesInfoUpdate(1);
    }

    private static void cleanupIndex(String indexName) throws Exception {
        if (indexerModel != null) {
            if (indexerModel.hasIndex(indexName)) {
                System.out.println("doing the cleanup of " + indexName);
                indexerModel.deleteIndex(indexName);
                repoSetup.getSepModel().removeSubscription("IndexUpdater_" + indexName);
                repoSetup.getHBaseProxy().waitOnReplicationPeerStopped("IndexUpdater_" + indexName);
                repoSetup.stopSepEventSlave();
            } else {
                System.out.println("Not doing cleanup because index does not exist in indexer model, index name = " + indexName);
            }
        } else {
            System.out.println("Not doing cleanup because indexerModel is null, index name = " + indexName);
        }
    }

    protected static void waitForIndexesInfoUpdate(int expectedCount) throws InterruptedException {
        // IndexesInfo will be updated asynchronously: wait for that to happen
        long now = System.currentTimeMillis();
        while (indexesInfo.getIndexInfos().size() != expectedCount) {
            if (System.currentTimeMillis() - now > 10000) {
                String indexNames = Joiner.on(",").join(
                        Collections2.transform(indexesInfo.getIndexInfos(), new Function<IndexInfo, String>() {
                            @Override
                            public String apply(@Nullable IndexInfo input) {
                                return input.getIndexDefinition().getName();
                            }
                        }));
                fail("IndexesInfo was not updated within the expected timeout, expected count = " + expectedCount +
                        " actual entries = " + indexNames);
            }
            Thread.sleep(20);
        }
    }

    private static void setupSchema() throws Exception {
        ValueType stringValueType = typeManager.getValueType("STRING");
        ValueType stringMvValueType = typeManager.getValueType("LIST<STRING>");

        ValueType longValueType = typeManager.getValueType("LONG");

        ValueType linkValueType = typeManager.getValueType("LINK");

        ValueType blobValueType = typeManager.getValueType("BLOB");
        ValueType blobMvHierValueType = typeManager.getValueType("LIST<PATH<BLOB>>");
        ValueType blobNestedValueType = typeManager.getValueType("LIST<LIST<LIST<BLOB>>>");

        ValueType dateTimeValueType = typeManager.getValueType("DATETIME");
        ValueType dateValueType = typeManager.getValueType("DATE");

        ValueType intHierValueType = typeManager.getValueType("PATH<INTEGER>");


        //
        // Version tag fields
        //

        lastTag = typeManager.getFieldTypeByName(VersionTag.LAST);

        QName nvTagName = new QName(VersionTag.NAMESPACE, "nonversioned");
        nvTag = typeManager.newFieldType(longValueType, nvTagName, Scope.NON_VERSIONED);
        nvTag = typeManager.createFieldType(nvTag);

        QName liveTagName = new QName(VersionTag.NAMESPACE, "live");
        liveTag = typeManager.newFieldType(longValueType, liveTagName, Scope.NON_VERSIONED);
        liveTag = typeManager.createFieldType(liveTag);

        QName previewTagName = new QName(VersionTag.NAMESPACE, "preview");
        previewTag = typeManager.newFieldType(longValueType, previewTagName, Scope.NON_VERSIONED);
        previewTag = typeManager.createFieldType(previewTag);

        // Note: tag 'last' was renamed to 'latest' because there is now built-in behaviour for the tag named 'last'
        QName lastTagName = new QName(VersionTag.NAMESPACE, "latest");
        latestTag = typeManager.newFieldType(longValueType, lastTagName, Scope.NON_VERSIONED);
        latestTag = typeManager.createFieldType(latestTag);

        //
        // Schema types for the nonversioned test
        //

        QName field1Name = new QName(NS, "nv_field1");
        nvfield1 = typeManager.newFieldType(stringValueType, field1Name, Scope.NON_VERSIONED);
        nvfield1 = typeManager.createFieldType(nvfield1);

        QName field2Name = new QName(NS, "nv_field2");
        nvfield2 = typeManager.newFieldType(stringValueType, field2Name, Scope.NON_VERSIONED);
        nvfield2 = typeManager.createFieldType(nvfield2);

        QName linkField1Name = new QName(NS, "nv_linkfield1");
        nvLinkField1 = typeManager.newFieldType(linkValueType, linkField1Name, Scope.NON_VERSIONED);
        nvLinkField1 = typeManager.createFieldType(nvLinkField1);

        QName linkField2Name = new QName(NS, "nv_linkfield2");
        nvLinkField2 = typeManager.newFieldType(linkValueType, linkField2Name, Scope.NON_VERSIONED);
        nvLinkField2 = typeManager.createFieldType(nvLinkField2);

        nvRecordType1 = typeManager.newRecordType(new QName(NS, "NVRecordType1"));
        addNvFieldTypes(nvRecordType1);
        nvRecordType1 = typeManager.createRecordType(nvRecordType1);

        //
        // Schema types for the versioned test
        //
        QName vfield1Name = new QName(NS2, "v_field1");
        vfield1 = typeManager.newFieldType(stringValueType, vfield1Name, Scope.VERSIONED);
        vfield1 = typeManager.createFieldType(vfield1);

        QName vfield2Name = new QName(NS2, "v_field2");
        vfield2 = typeManager.newFieldType(stringValueType, vfield2Name, Scope.VERSIONED);
        vfield2 = typeManager.createFieldType(vfield2);

        QName vlinkField1Name = new QName(NS2, "v_linkfield1");
        vLinkField1 = typeManager.newFieldType(linkValueType, vlinkField1Name, Scope.VERSIONED);
        vLinkField1 = typeManager.createFieldType(vLinkField1);

        QName vlinkField2Name = new QName(NS2, "v_linkfield2");
        vLinkField2 = typeManager.newFieldType(linkValueType, vlinkField2Name, Scope.VERSIONED);
        vLinkField2 = typeManager.createFieldType(vLinkField2);

        QName vStringMvFieldName = new QName(NS2, "v_string_mv_field");
        vStringMvField = typeManager.newFieldType(stringMvValueType, vStringMvFieldName, Scope.VERSIONED);
        vStringMvField = typeManager.createFieldType(vStringMvField);

        QName vLongFieldName = new QName(NS2, "v_long_field");
        vLongField = typeManager.newFieldType(longValueType, vLongFieldName, Scope.VERSIONED);
        vLongField = typeManager.createFieldType(vLongField);

        QName vBlobFieldName = new QName(NS2, "v_blob_field");
        vBlobField = typeManager.newFieldType(blobValueType, vBlobFieldName, Scope.VERSIONED);
        vBlobField = typeManager.createFieldType(vBlobField);

        QName vBlobMvHierFieldName = new QName(NS2, "v_blob_mv_hier_field");
        vBlobMvHierField = typeManager.newFieldType(blobMvHierValueType, vBlobMvHierFieldName, Scope.VERSIONED);
        vBlobMvHierField = typeManager.createFieldType(vBlobMvHierField);

        QName vBlobNestedFieldName = new QName(NS2, "v_blob_nested_field");
        vBlobNestedField = typeManager.newFieldType(blobNestedValueType, vBlobNestedFieldName, Scope.VERSIONED);
        vBlobNestedField = typeManager.createFieldType(vBlobNestedField);

        QName vDateTimeFieldName = new QName(NS2, "v_datetime_field");
        vDateTimeField = typeManager.newFieldType(dateTimeValueType, vDateTimeFieldName, Scope.VERSIONED);
        vDateTimeField = typeManager.createFieldType(vDateTimeField);

        QName vDateFieldName = new QName(NS2, "v_date_field");
        vDateField = typeManager.newFieldType(dateValueType, vDateFieldName, Scope.VERSIONED);
        vDateField = typeManager.createFieldType(vDateField);

        QName vIntHierFieldName = new QName(NS2, "v_int_hier_field");
        vIntHierField = typeManager.newFieldType(intHierValueType, vIntHierFieldName, Scope.VERSIONED);
        vIntHierField = typeManager.createFieldType(vIntHierField);

        vRecordType1 = typeManager.newRecordType(new QName(NS2, "VRecordType1"));
        addVFieldTypes(vRecordType1);
        vRecordType1 = typeManager.createRecordType(vRecordType1);

        //
        // Schema types for testing last tag
        //
        lastRecordType = typeManager.newRecordType(new QName(NS2, "LastRecordType"));
        lastRecordType.addFieldTypeEntry(vfield1.getId(), false);
        lastRecordType.addFieldTypeEntry(nvfield1.getId(), false);
        lastRecordType = typeManager.createRecordType(lastRecordType);

        //
        // Schema types for testing <match> and <foreach>
        //
        for (int i = 1; i <= 6; i++) {
            typeManager.fieldTypeBuilder().name(new QName(NS, "nvmatch" + i))
                    .scope(Scope.NON_VERSIONED)
                    .type("STRING").create();
            typeManager.fieldTypeBuilder().name(new QName(NS2, "match" + i))
                    .scope(Scope.VERSIONED)
                    .type("STRING").create();
        }
        for (QName name : new QName[]{new QName(NS, "Alpha"), new QName(NS, "Beta"), new QName(NS2, "Alpha"),
                new QName(NS2, "Beta")}) {
            RecordType recordType = typeManager.newRecordType(name);
            addNvFieldTypes(recordType);
            addVFieldTypes(recordType);
            for (int i = 1; i <= 6; i++) {
                recordType
                        .addFieldTypeEntry(typeManager.getFieldTypeByName(new QName(NS, "nvmatch" + i)).getId(), false);
                recordType
                        .addFieldTypeEntry(typeManager.getFieldTypeByName(new QName(NS2, "match" + i)).getId(), false);
            }
            // Link fields
            for (int i = 1; i <= 2; i++) {
                recordType.addFieldTypeEntry(typeManager.getFieldTypeByName(new QName(NS, "nv_linkfield" + i)).getId(),
                        false);
                recordType.addFieldTypeEntry(typeManager.getFieldTypeByName(new QName(NS2, "v_linkfield" + i)).getId(),
                        false);
            }
            typeManager.createRecordType(recordType);
        }

    }

    private static void addVFieldTypes(RecordType recordType) {
        recordType.addFieldTypeEntry(vfield1.getId(), false);
        recordType.addFieldTypeEntry(liveTag.getId(), false);
        recordType.addFieldTypeEntry(latestTag.getId(), false);
        recordType.addFieldTypeEntry(previewTag.getId(), false);
        recordType.addFieldTypeEntry(vLinkField1.getId(), false);
        recordType.addFieldTypeEntry(nvLinkField2.getId(), false);
        recordType.addFieldTypeEntry(vStringMvField.getId(), false);
        recordType.addFieldTypeEntry(vLongField.getId(), false);
        recordType.addFieldTypeEntry(vBlobField.getId(), false);
        recordType.addFieldTypeEntry(vBlobMvHierField.getId(), false);
        recordType.addFieldTypeEntry(vBlobNestedField.getId(), false);
        recordType.addFieldTypeEntry(vDateTimeField.getId(), false);
        recordType.addFieldTypeEntry(vDateField.getId(), false);
        recordType.addFieldTypeEntry(vIntHierField.getId(), false);
    }

    private static void addNvFieldTypes(RecordType recordType) {
        recordType.addFieldTypeEntry(nvfield1.getId(), false);
        recordType.addFieldTypeEntry(nvfield2.getId(), false);
        recordType.addFieldTypeEntry(liveTag.getId(), false);
        recordType.addFieldTypeEntry(latestTag.getId(), false);
        recordType.addFieldTypeEntry(previewTag.getId(), false);
        recordType.addFieldTypeEntry(nvLinkField1.getId(), false);
        recordType.addFieldTypeEntry(nvLinkField2.getId(), false);
    }

    @Test
    public void testForEach() throws Exception {
        changeIndexUpdater("indexerconf_match.xml");
        messageVerifier.disable();

        //
        // Test ForEach
        //

        log.debug("Begin test forEach");

        String baseProductId = "product29485";
        String linkedProductId = "linkedProduct12345";
        RecordId linkedRecordId = defaultRepository.getIdGenerator().newRecordId(linkedProductId);

        defaultRepository.recordBuilder()
                .id(defaultRepository.getIdGenerator().newRecordId(baseProductId))
                .recordType(new QName(NS, "Alpha"))
                .field(nvfield1.getName(), "29485")
                .field(nvLinkField1.getName(), new Link(linkedRecordId))
                .create();

        defaultRepository.recordBuilder()
                .id(defaultRepository.getIdGenerator().newRecordId(baseProductId,
                        Collections.singletonMap("country", "france")))
                .recordType(new QName(NS, "Alpha"))
                .field(nvfield1.getName(), "louche")
                .field(nvfield2.getName(), "10")
                .create();

        defaultRepository.recordBuilder()
                .id(defaultRepository.getIdGenerator().newRecordId(baseProductId,
                        Collections.singletonMap("country", "belgium")))
                .recordType(new QName(NS, "Alpha"))
                .field(nvfield1.getName(), "schuimspaan")
                .field(nvfield2.getName(), "11")
                .create();

        defaultRepository.recordBuilder()
                .id(linkedRecordId)
                .recordType(new QName(NS, "Alpha"))
                .field(nvfield1.getName(), "12345")
                .create();

        commitIndex();

        verifyResultCount("product_description_france_string:louche", 1);
        verifyResultCount("product_price_france_string:10", 1);
        verifyResultCount("linked_product:12345", 1);

        // update the price in france:
        log.debug("Begin test forEach - update");
        defaultRepository.recordBuilder()
                .id(defaultRepository.getIdGenerator()
                        .newRecordId("product29485", Collections.singletonMap("country", "france")))
                .field(nvfield2.getName(), "12")
                .update();

        commitIndex();

        verifyResultCount("product_price_france_string:12", 1);

    }

    @Test
    public void testMatch() throws Exception {
        changeIndexUpdater("indexerconf_match.xml");

        // disabling since we are not verifying the messages
        messageVerifier.disable();

        //
        // Test Match
        //
        log.debug("Begin test match");
        createMatchTestRecord(NS, "Alpha", "alpha");
        createMatchTestRecord(NS, "Beta", "beta");
        createMatchTestRecord(NS2, "Alpha", "gamma");
        createMatchTestRecord(NS2, "Beta", "delta");

        // Initialise a map containing all the expected result counts (2, since we have 'last' and 'preview')
        setExpectedCountsForMatch("nvmatch1", 2, 2, 2, 2, 0, 0, 0, 0); // all
        setExpectedCountsForMatch("nvmatch2", 2, 2, 0, 0, 0, 0, 0, 0); // ns:*
        setExpectedCountsForMatch("nvmatch3", 2, 0, 2, 0, 0, 0, 0, 0); // *:Alpha
        setExpectedCountsForMatch("nvmatch4", 2, 0, 0, 0, 0, 0, 0, 0); // ns:Alpha

        setExpectedCountsForMatch("match1", 2, 2, 2, 2, 0, 0, 0, 0); // all
        setExpectedCountsForMatch("match2", 2, 2, 0, 0, 0, 0, 0, 0); // ns:*
        setExpectedCountsForMatch("match3", 2, 0, 2, 0, 0, 0, 0, 0); // *:Alpha
        setExpectedCountsForMatch("match4", 2, 0, 0, 0, 0, 0, 0, 0); // ns:Alpha

        verifyMatchResultCounts();

        // Update non-versioned fields
        updateMatchTestRecords(new QName(NS, "nvmatch1"), "nvmatch1");
        setExpectedCountsForMatch("nvmatch1", 0, 0, 0, 0, 2, 2, 2, 2);
        verifyMatchResultCounts();
        updateMatchTestRecords(new QName(NS, "nvmatch2"), "nvmatch2");
        setExpectedCountsForMatch("nvmatch2", 0, 0, 0, 0, 2, 2, 0, 0);
        verifyMatchResultCounts();
        updateMatchTestRecords(new QName(NS, "nvmatch3"), "nvmatch3");
        setExpectedCountsForMatch("nvmatch3", 0, 0, 0, 0, 2, 0, 2, 0);
        verifyMatchResultCounts();
        updateMatchTestRecords(new QName(NS, "nvmatch4"), "nvmatch4");
        setExpectedCountsForMatch("nvmatch4", 0, 0, 0, 0, 2, 0, 0, 0);
        verifyMatchResultCounts();

        // Update versioned fields
        updateMatchTestRecords(new QName(NS2, "match1"), "match1");
        setExpectedCountsForMatch("match1", 1, 1, 1, 1, 1, 1, 1, 1);
        verifyMatchResultCounts();
        updateMatchTestRecords(new QName(NS2, "match2"), "match2");
        setExpectedCountsForMatch("match2", 1, 1, 0, 0, 1, 1, 0, 0);
        verifyMatchResultCounts();
        updateMatchTestRecords(new QName(NS2, "match3"), "match3");
        setExpectedCountsForMatch("match3", 1, 0, 1, 0, 1, 0, 1, 0);
        verifyMatchResultCounts();
        updateMatchTestRecords(new QName(NS2, "match4"), "match4");
        setExpectedCountsForMatch("match4", 1, 0, 0, 0, 1, 0, 0, 0);
        verifyMatchResultCounts();

        //
        // Test match on field conditions using non versioned fields
        // Note: the lines marked with /*hasfield*/ test for a feature we don't support yet, hence commented out
        //
        defaultRepository.recordBuilder().id("match_nvfield")
                .recordType(new QName(NS, "Alpha"))
                .field(nvfield1.getName(), "jupiter")
                .field(nvfield2.getName(), "pancake")
                .field(previewTag.getName(), new Long(0))
                .create();

        commitIndex();

        /*hasfield*/// verifyResultCount("nvmatch5:jupiter", 2); // vfield2 is present
        verifyResultCount("nvmatch6:jupiter", 2); // nvfield2=specialvalue

        defaultRepository.recordBuilder().id("match_nvfield")
                .field(nvfield1.getName(), "waffle")
                .update();

        commitIndex();

        verifyResultCount("nvmatch6:jupiter", 0); // nvfield2=specialvalue

        /*hasfield*/// verifyResultCount("nvmatch5:jupiter", 2); // nvfield2 is present
        /*hasfield*/// Record record = repository.read(repository.getIdGenerator().newRecordId("match_nvfield"));
        /*hasfield*/// record.delete(nvfield2.getName(), true);

        /*hasfield*/// commitIndex();
        /*hasfield*/// verifyResultCount("nvmatch5:jupiter", 0); // nvfield2 is present
        /*hasfield*/// verifyResultCount("nvmatch6:jupiter", 0); // nvfield2=specialvalue

        //
        // Test match on field conditions (has field, field equals) using non versioned fields
        //
        defaultRepository.recordBuilder().id("match_vfield")
                .recordType(new QName(NS, "Alpha"))
                .field(vfield1.getName(), "apollo")
                .field(vfield2.getName(), "bacon")
                .field(previewTag.getName(), new Long(1))
                .create();

        commitIndex();
        /*hasfield*/// verifyResultCount("match5:apollo", 2); // vfield2 is present
        verifyResultCount("match6:apollo", 2); // vfield2=specialvalue

        defaultRepository.recordBuilder().id("match_vfield")
                .field(vfield1.getName(), "eggs")
                .update();

        commitIndex();

        /*hasfield*/// verifyResultCount("match5:apollo", 2); // vfield2 is present in preview and last
        verifyResultCount("match6:apollo", 1); // vfield2=specialvalue (version tagged with preview still matches)

        /*hasfield*/// record = repository.read(repository.getIdGenerator().newRecordId("match_vfield"));
        /*hasfield*/// record.delete(vfield2.getName(), true);

        /*hasfield*/// commitIndex();
        /*hasfield*/// verifyResultCount("match5:apollo", 1); // vfield2 is present
        /*hasfield*/// verifyResultCount("match6:apollo", 1); // vfield2=specialvalue

        //
        // TODO: match on variant properties
        //
        for (String lang : new String[]{"en", "fr"}) {
            defaultRepository.recordBuilder().id("match_varprops_cupid", vprops("lang", lang))
                    .recordType(new QName(NS, "Alpha"))
                    .field(vfield1.getName(), "cupido_" + lang + "_nobranch")
                    .create();

            defaultRepository.recordBuilder().id("match_varprops_merc", vprops("lang", lang))
                    .recordType(new QName(NS, "Alpha"))
                    .field(nvfield1.getName(), "mercurius_" + lang + "_nobranch")
                    .create();

            for (String branch : new String[]{"dev", "prod"}) {
                defaultRepository.recordBuilder().id("match_varprops_cupid", vprops("lang", lang, "branch", branch))
                        .recordType(new QName(NS, "Alpha"))
                        .field(vfield1.getName(), "cupido_" + lang + "_" + branch)
                        .create();
                defaultRepository.recordBuilder().id("match_varprops_merc", vprops("lang", lang, "branch", branch))
                        .recordType(new QName(NS, "Alpha"))
                        .field(nvfield1.getName(), "mercurius_" + lang + "_" + branch)
                        .create();
            }
        }
        commitIndex();

        verifyResultCount("match7:cupido_en_nobranch", 0);
        verifyResultCount("match7:cupido_en_dev", 1);
        verifyResultCount("match7:cupido_en_prod", 1);
        verifyResultCount("match7:cupido_fr_nobranch", 0);
        verifyResultCount("match7:cupido_fr_dev", 0);
        verifyResultCount("match7:cupido_fr_prod", 0);

        verifyResultCount("nvmatch7:mercurius_en_nobranch", 0);
        verifyResultCount("nvmatch7:mercurius_en_dev", 1);
        verifyResultCount("nvmatch7:mercurius_en_prod", 1);
        verifyResultCount("nvmatch7:mercurius_fr_nobranch", 0);
        verifyResultCount("nvmatch7:mercurius_fr_dev", 0);
        verifyResultCount("nvmatch7:mercurius_fr_prod", 0);
    }

    private Map<String, String> vprops(String... args) {
        Map<String, String> result = Maps.newHashMap();
        for (int i = 0; i < args.length; i += 2) {
            result.put(args[i], args[i + 1]);
        }
        return result;
    }

    private void verifyMatchResultCounts() throws Exception {
        List<String> results = Lists.newArrayList();
        boolean allOk = true;

        commitIndex();
        for (String condition : matchResultCounts.keySet()) {
            Integer expected = matchResultCounts.get(condition);
            long numFound = getQueryResponse(condition).getResults().getNumFound();
            if (numFound == expected.longValue()) {
                results.add("OK: " + condition + " => " + expected);
            } else {
                results.add("ERROR: " + condition + " => " + numFound + " in stead of " + expected);
                allOk = false;
            }
        }

        if (!allOk) {
            fail(Joiner.on("\n").join(results));
        }
    }

    private void updateMatchTestRecords(QName lilyField, String solrField)
            throws InterruptedException, RepositoryException {
        for (String id : new String[]{"alpha", "beta", "gamma", "delta"}) {
            defaultRepository.recordBuilder()
                    .id(defaultRepository.getIdGenerator().newRecordId(id))
                    .field(lilyField, id + "_" + solrField + "_updated")
                    .field(previewTag.getName(), new Long(1))
                    .update();
        }
    }

    private void setExpectedCountsForMatch(String indexField, int... counts) {
        matchResultCounts.put(indexField + ":alpha_" + indexField + "_orig", counts[0]);
        matchResultCounts.put(indexField + ":beta_" + indexField + "_orig", counts[1]);
        matchResultCounts.put(indexField + ":gamma_" + indexField + "_orig", counts[2]);
        matchResultCounts.put(indexField + ":delta_" + indexField + "_orig", counts[3]);
        matchResultCounts.put(indexField + ":alpha_" + indexField + "_updated", counts[4]);
        matchResultCounts.put(indexField + ":beta_" + indexField + "_updated", counts[5]);
        matchResultCounts.put(indexField + ":gamma_" + indexField + "_updated", counts[6]);
        matchResultCounts.put(indexField + ":delta_" + indexField + "_updated", counts[7]);
    }

    private void createMatchTestRecord(String ns, String name, String id) throws Exception {
        RecordBuilder builder = defaultRepository.recordBuilder();

        builder.recordType(new QName(ns, name))
                .id(id);

        for (int i = 1; i <= 4; i++) {
            builder.field(new QName(NS2, "match" + i), id + "_" + "match" + i + "_orig");
            builder.field(new QName(NS, "nvmatch" + i), id + "_" + "nvmatch" + i + "_orig");
        }
        builder.field(previewTag.getName(), new Long(1));

        builder.create();
    }

    @Test
    public void changeIndexUpdaterMultipleTimes() throws Exception {
        changeIndexUpdater("indexerconf1.xml");
        changeIndexUpdater("indexerconf1.xml");
        changeIndexUpdater("indexerconf1.xml");
    }

    // Test the behaviour of using the "lily.mq" attribute when indexing
    @Test
    public void testIndexer_AddAndUpdate_DisabledIndexing() throws Exception {
       changeIndexUpdater("indexerconf1.xml");

       // Create a record that should be indexed
       Record recordToIndex = defaultRepository.newRecord();
       recordToIndex.setRecordType(nvRecordType1.getName());
       recordToIndex.setField(nvfield1.getName(), "mango");
       recordToIndex.setField(nvTag.getName(), 0L);
       recordToIndex = defaultRepository.create(recordToIndex);


       // Create a record that shouldn't be indexed (due to the "lily.mq" attribute
       Record recordToNotIndex = defaultRepository.newRecord();
       recordToNotIndex.setRecordType(nvRecordType1.getName());
       recordToNotIndex.setField(nvfield1.getName(), "mango");
       recordToNotIndex.setField(nvTag.getName(), 0L);

       // Mark this record to not be indexed
       recordToNotIndex.getAttributes().put("lily.mq", "false");
       recordToNotIndex = defaultRepository.create(recordToNotIndex);

       commitIndex();
       verifyResultCount("lily.id:" + recordToIndex.getId().toString(), 1);
       verifyResultCount("lily.id:" + recordToNotIndex.getId().toString(), 0);
       verifyResultCount("nv_field1:mango", 1);

       // Now we'll update the recordToIndex, first without indexing, and then with it

       recordToIndex.setField(nvfield1.getName(), "orange");
       recordToIndex.getAttributes().put("lily.mq", "false");

       recordToIndex = defaultRepository.update(recordToIndex);
       commitIndex();

       verifyResultCount("nv_field1:orange", 0);
       verifyResultCount("nv_field1:mango", 1);


    }

    // Test the behaviour of using the "lily.mq" attribute when indexing
    @Test
    public void testIndexer_Delete_DisabledIndexing() throws Exception {
       changeIndexUpdater("indexerconf1.xml");

       // Create a record that should be indexed
       Record recordToIndex = defaultRepository.newRecord();
       recordToIndex.setRecordType(nvRecordType1.getName());
       recordToIndex.setField(nvfield1.getName(), "papaya");
       recordToIndex.setField(nvTag.getName(), 0L);
       recordToIndex = defaultRepository.create(recordToIndex);


       // Create a record that shouldn't be indexed (due to the "lily.mq" attribute
       Record recordToNotIndex = defaultRepository.newRecord();
       recordToNotIndex.setRecordType(nvRecordType1.getName());
       recordToNotIndex.setField(nvfield1.getName(), "papaya");
       recordToNotIndex.setField(nvTag.getName(), 0L);
       recordToNotIndex = defaultRepository.create(recordToNotIndex);

       commitIndex();

       // Sanity check
       verifyResultCount("lily.id:" + recordToIndex.getId().toString(), 1);
       verifyResultCount("lily.id:" + recordToNotIndex.getId().toString(), 1);
       verifyResultCount("nv_field1:papaya", 2);


       // Now delete both records, but disable indexing on one of them
       recordToNotIndex.getAttributes().put("lily.mq", "false");

       defaultRepository.delete(recordToIndex);
       defaultRepository.delete(recordToNotIndex);
       commitIndex();

       // And check that the index has only been updated for the record that should be indexed
       verifyResultCount("lily.id:" + recordToIndex.getId().toString(), 0);
       verifyResultCount("lily.id:" + recordToNotIndex.getId().toString(), 1);
       verifyResultCount("nv_field1:papaya", 1);
    }

    @Test
    public void testDereferencing_SingleTable() throws Exception {
        changeIndexUpdater("indexerconf1.xml");

        messageVerifier.init();

        log.debug("Begin test deref single table");
        Record record1 = defaultRepository.newRecord();
        record1.setRecordType(nvRecordType1.getName());
        record1.setField(nvfield1.getName(), "derefsinglepear");
        record1.setField(nvTag.getName(), 0L);
        expectEvent(CREATE, Table.RECORD.name, record1.getId(), nvfield1.getId(), nvTag.getId());
        record1 = defaultRepository.create(record1);

        Record record2 = defaultRepository.newRecord();
        record2.setRecordType(nvRecordType1.getName());
        record2.setField(nvLinkField1.getName(), new Link(record1.getId()));
        record2.setField(nvTag.getName(), 0L);
        expectEvent(CREATE, Table.RECORD.name, record2.getId(), nvLinkField1.getId(), nvTag.getId());
        record2 = defaultRepository.create(record2);

        commitIndex();
        verifyResultCount("nv_deref1:derefsinglepear", 1);

        record1.setField(nvfield1.getName(), "derefsingleapple");
        expectEvent(UPDATE, Table.RECORD.name, record1.getId(), nvfield1.getId());
        defaultRepository.update(record1);

        commitIndex();
        verifyResultCount("nv_deref1:derefsinglepear", 0);
        verifyResultCount("nv_deref1:derefsingleapple", 1);

        assertEquals("All received messages are correct.", 0, messageVerifier.getFailures());

    }

    @Test
    public void testDereferencing_SingleNonstandardTable() throws Exception {
        changeIndexUpdater("indexerconf1.xml");

        messageVerifier.init();

        log.debug("Begin test deref single non-standard table");
        Record record1 = alternateRepository.newRecord();
        record1.setRecordType(nvRecordType1.getName());
        record1.setField(nvfield1.getName(), "derefsinglenonstandardpear");
        record1.setField(nvTag.getName(), 0L);
        expectEvent(CREATE, ALTERNATE_TABLE, record1.getId(), nvfield1.getId(), nvTag.getId());
        record1 = alternateRepository.create(record1);

        Record record2 = alternateRepository.newRecord();
        record2.setRecordType(nvRecordType1.getName());
        record2.setField(nvLinkField1.getName(), new Link(ALTERNATE_TABLE, record1.getId()));
        record2.setField(nvTag.getName(), 0L);
        expectEvent(CREATE, ALTERNATE_TABLE, record2.getId(), nvLinkField1.getId(), nvTag.getId());
        record2 = alternateRepository.create(record2);

        commitIndex(ALTERNATE_TABLE);
        commitIndex();
        verifyResultCount("nv_deref1:derefsinglenonstandardpear", 1);

        record1.setField(nvfield1.getName(),  "derefsinglenonstandardapple");
        expectEvent(UPDATE, ALTERNATE_TABLE, record1.getId(), nvfield1.getId());
        alternateRepository.update(record1);

        commitIndex(ALTERNATE_TABLE);
        commitIndex();
        verifyResultCount("nv_deref1:derefsinglenonstandardpear", 0);
        verifyResultCount("nv_deref1:derefsinglenonstandardapple", 1);

        assertEquals("All received messages are correct.", 0, messageVerifier.getFailures());

    }

    @Test
    public void testDereferencing_SingleNonstandardTable_LinkWithoutTable() throws Exception {
        changeIndexUpdater("indexerconf1.xml");

        messageVerifier.init();

        log.debug("Begin test deref single non-standard table");
        Record record1 = alternateRepository.newRecord();
        record1.setRecordType(nvRecordType1.getName());
        record1.setField(nvfield1.getName(), "derefsinglenonstandardnolinkpear");
        record1.setField(nvTag.getName(), 0L);
        expectEvent(CREATE, ALTERNATE_TABLE, record1.getId(), nvfield1.getId(), nvTag.getId());
        record1 = alternateRepository.create(record1);

        Record record2 = alternateRepository.newRecord();
        record2.setRecordType(nvRecordType1.getName());
        record2.setField(nvLinkField1.getName(), new Link(record1.getId()));
        record2.setField(nvTag.getName(), 0L);
        expectEvent(CREATE, ALTERNATE_TABLE, record2.getId(), nvLinkField1.getId(), nvTag.getId());
        record2 = alternateRepository.create(record2);

        commitIndex(ALTERNATE_TABLE);
        commitIndex();
        verifyResultCount("nv_deref1:derefsinglenonstandardnolinkpear", 1);

        record1.setField(nvfield1.getName(),  "derefsinglenonstandardnolinkapple");
        expectEvent(UPDATE, ALTERNATE_TABLE, record1.getId(), nvfield1.getId());
        alternateRepository.update(record1);

        commitIndex(ALTERNATE_TABLE);
        commitIndex();
        verifyResultCount("nv_deref1:derefsinglenonstandardnolinkpear", 0);
        verifyResultCount("nv_deref1:derefsinglenonstandardnolinkapple", 1);

        assertEquals("All received messages are correct.", 0, messageVerifier.getFailures());

    }

    @Test
    public void testDereferencing_MultipleTables() throws Exception {
        changeIndexUpdater("indexerconf1.xml");

        messageVerifier.init();

        log.debug("Begin test deref multiple tables");
        Record record1 = defaultRepository.newRecord();
        record1.setRecordType(nvRecordType1.getName());
        record1.setField(nvfield1.getName(), "derefmultipear");
        record1.setField(nvTag.getName(), 0L);
        expectEvent(CREATE, ALTERNATE_TABLE, record1.getId(), nvfield1.getId(), nvTag.getId());
        record1 = alternateRepository.create(record1);

        Record record2 = defaultRepository.newRecord();
        record2.setRecordType(nvRecordType1.getName());
        record2.setField(nvLinkField1.getName(), new Link(ALTERNATE_TABLE, record1.getId()));
        record2.setField(nvTag.getName(), 0L);
        expectEvent(CREATE, Table.RECORD.name, record2.getId(), nvLinkField1.getId(), nvTag.getId());
        record2 = defaultRepository.create(record2);

        commitIndex(Table.RECORD.name);
        commitIndex(ALTERNATE_TABLE);
        verifyResultCount("nv_deref1:derefmultipear", 1);

        record1.setField(nvfield1.getName(),  "derefmulti_apple");
        expectEvent(UPDATE, ALTERNATE_TABLE, record1.getId(), nvfield1.getId());
        alternateRepository.update(record1);

        commitIndex(ALTERNATE_TABLE);
        commitIndex();
        verifyResultCount("nv_deref1:derefmultipear", 0);
        verifyResultCount("nv_deref1:derefmultiapple", 1);

        assertEquals("All received messages are correct.", 0, messageVerifier.getFailures());
    }


    @Test
    public void testIndexerNonVersioned() throws Exception {
        changeIndexUpdater("indexerconf1.xml");

        messageVerifier.init();

        //
        // Basic create-update-delete
        //
        {
            // Create a record
            log.debug("Begin test NV1");
            Record record = defaultRepository.newRecord();
            record.setRecordType(nvRecordType1.getName());
            record.setField(nvfield1.getName(), "nvapple");
            record.setField(nvTag.getName(), 0L);
            expectEvent(CREATE, Table.RECORD.name, record.getId(), nvfield1.getId(), nvTag.getId());
            record = defaultRepository.create(record);

            commitIndex();
            verifyResultCount("lily.id:" + record.getId().toString(), 1);
            verifyResultCount("nv_field1:nvapple", 1);

            // Update the record
            log.debug("Begin test NV2");
            record.setField(nvfield1.getName(), "nvpear");
            expectEvent(UPDATE, Table.RECORD.name, record.getId(), nvfield1.getId());
            defaultRepository.update(record);

            System.out.println("Updated " + record.getId());
            commitIndex();
            verifyResultCount("nv_field1:nvpear", 1);
            verifyResultCount("nv_field1:nvapple", 0);

            // Do as if field2 changed, while field2 is not present in the document.
            // Such situations can occur if the record is modified before earlier events are processed.
            log.debug("Begin test NV3");
            // TODO send event directly to the Indexer
            // sendEvent(EVENT_RECORD_UPDATED, record.getId(), nvfield2.getId());

            verifyResultCount("nv_field1:nvpear", 1);
            verifyResultCount("nv_field1:nvapple", 0);

            // Add a vtag field pointing to a version. For versionless records, this should have no effect
            log.debug("Begin test NV4");
            record.setField(liveTag.getName(), new Long(1));
            expectEvent(UPDATE, Table.RECORD.name, record.getId(), liveTag.getId());
            defaultRepository.update(record);

            commitIndex();
            verifyResultCount("nv_field1:nvpear", 1);
            verifyResultCount("nv_field1:nvapple", 0);

            // Delete the record
            log.debug("Begin test NV5");
            expectEvent(DELETE, Table.RECORD.name, record.getId());
            defaultRepository.delete(record.getId());

            commitIndex();

            verifyResultCount("nv_field1:nvpear", 0);
        }

        //
        // Deref
        //
        {
            log.debug("Begin test NV6");
            Record record1 = defaultRepository.newRecord();
            record1.setRecordType(nvRecordType1.getName());
            record1.setField(nvfield1.getName(), "nvpear");
            record1.setField(nvTag.getName(), 0L);
            expectEvent(CREATE, Table.RECORD.name, record1.getId(), nvfield1.getId(), nvTag.getId());
            record1 = defaultRepository.create(record1);

            Record record2 = defaultRepository.newRecord();
            record2.setRecordType(nvRecordType1.getName());
            record2.setField(nvLinkField1.getName(), new Link(record1.getId()));
            record2.setField(nvTag.getName(), 0L);
            expectEvent(CREATE, Table.RECORD.name, record2.getId(), nvLinkField1.getId(), nvTag.getId());
            record2 = defaultRepository.create(record2);

            commitIndex();
            verifyResultCount("nv_deref1:nvpear", 1);
        }


        //
        // Variant deref
        //
        {
            log.debug("Begin test NV7");
            Record masterRecord = defaultRepository.newRecord();
            masterRecord.setRecordType(nvRecordType1.getName());
            masterRecord.setField(nvfield1.getName(), "yellow");
            masterRecord.setField(nvTag.getName(), 0L);
            expectEvent(CREATE, Table.RECORD.name, masterRecord.getId(), nvfield1.getId(), nvTag.getId());
            masterRecord = defaultRepository.create(masterRecord);

            RecordId var1Id = idGenerator.newRecordId(masterRecord.getId(), Collections.singletonMap("lang", "en"));
            Record var1Record = defaultRepository.newRecord(var1Id);
            var1Record.setRecordType(nvRecordType1.getName());
            var1Record.setField(nvfield1.getName(), "green");
            var1Record.setField(nvTag.getName(), 0L);
            expectEvent(CREATE, Table.RECORD.name, var1Id, nvfield1.getId(), nvTag.getId());
            defaultRepository.create(var1Record);

            Map<String, String> varProps = new HashMap<String, String>();
            varProps.put("lang", "en");
            varProps.put("branch", "dev");
            RecordId var2Id = idGenerator.newRecordId(masterRecord.getId(), varProps);
            Record var2Record = defaultRepository.newRecord(var2Id);
            var2Record.setRecordType(nvRecordType1.getName());
            var2Record.setField(nvfield1.getName(), "blue");
            var2Record.setField(nvTag.getName(), 0L);
            expectEvent(CREATE, Table.RECORD.name, var2Id, nvfield1.getId(), nvTag.getId());
            defaultRepository.create(var2Record);

            commitIndex();
            verifyResultCount("nv_deref2:yellow", 1);
            verifyResultCount("nv_deref3:yellow", 2);
            verifyResultCount("nv_deref4:green", 1);
            verifyResultCount("nv_deref3:green", 0);
            verifyResultCount("nv_deref5:blue", 1);
            verifyResultCount("nv_deref5:green", 0);
            verifyResultCount("nv_deref5:yellow", 0);
            verifyResultCount("nv_deref6:blue", 2);
            verifyResultCount("nv_deref7:blue", 1);
            verifyResultCount("nv_deref8:blue", 0);
        }

        //
        // Update denormalized data
        //
        {
            log.debug("Begin test NV8");
            Record record1 = defaultRepository.newRecord(idGenerator.newRecordId("boe"));
            record1.setRecordType(nvRecordType1.getName());
            record1.setField(nvfield1.getName(), "cucumber");
            record1.setField(nvTag.getName(), 0L);
            expectEvent(CREATE, Table.RECORD.name, record1.getId(), nvfield1.getId(), nvTag.getId());
            record1 = defaultRepository.create(record1);

            // Create a record which will contain denormalized data through linking
            Record record2 = defaultRepository.newRecord();
            record2.setRecordType(nvRecordType1.getName());
            record2.setField(nvLinkField1.getName(), new Link(record1.getId()));
            record2.setField(nvfield1.getName(), "mushroom");
            record2.setField(nvTag.getName(), 0L);
            expectEvent(CREATE, Table.RECORD.name, record2.getId(), nvLinkField1.getId(), nvfield1.getId(), nvTag.getId());
            record2 = defaultRepository.create(record2);

            // Create a record which will contain denormalized data through master-dereferencing and forward-variant-dereferencing
            RecordId record3Id = idGenerator.newRecordId(record1.getId(), Collections.singletonMap("lang", "en"));
            Record record3 = defaultRepository.newRecord(record3Id);
            record3.setRecordType(nvRecordType1.getName());
            record3.setField(nvfield1.getName(), "eggplant");
            record3.setField(nvTag.getName(), 0L);
            expectEvent(CREATE, Table.RECORD.name, record3.getId(), nvfield1.getId(), nvTag.getId());
            record3 = defaultRepository.create(record3);

            // Create a record which will contain denormalized data through variant-dereferencing
            Map<String, String> varprops = new HashMap<String, String>();
            varprops.put("lang", "en");
            varprops.put("branch", "dev");
            RecordId record4Id = idGenerator.newRecordId(record1.getId(), varprops);
            Record record4 = defaultRepository.newRecord(record4Id);
            record4.setRecordType(nvRecordType1.getName());
            record4.setField(nvfield1.getName(), "broccoli");
            record4.setField(nvTag.getName(), 0L);
            expectEvent(CREATE, Table.RECORD.name, record4.getId(), nvfield1.getId(), nvTag.getId());
            record4 = defaultRepository.create(record4);

            commitIndex();
            verifyResultCount("nv_deref1:cucumber", 1); // record2[nv:linkField1] = record1
            verifyResultCount("nv_deref2:cucumber", 1); // record4{-branch,-lang} = record1
            verifyResultCount("nv_deref3:cucumber", 2); // record3{master} and record4{master} = record1
            verifyResultCount("nv_deref4:eggplant", 1); // record4{-branch} = record3
            verifyResultCount("nv_deref5:broccoli", 1); // record3{+branch} = record4
            verifyResultCount("nv_deref6:broccoli", 2); // record1{+branch,+lang} = record4 (2 tags!)
            verifyResultCount("nv_deref7:broccoli", 1); // record3{+branch=dev} = record4

            // Update record1, check if the others are updated in the index
            log.debug("Begin test NV9");
            record1.setField(nvfield1.getName(), "tomato");
            expectEvent(UPDATE, Table.RECORD.name, record1.getId(), nvfield1.getId());
            record1 = defaultRepository.update(record1);

            commitIndex();
            verifyResultCount("nv_deref1:tomato", 1); // record2[ns:nvLinkField1] = record1
            verifyResultCount("nv_deref2:tomato", 1); // record4{-branch,-lang} = record1
            verifyResultCount("nv_deref3:tomato", 2); // record3{master} and record4{master} = record1
            verifyResultCount("nv_deref1:cucumber", 0); // old value should be removed from index (non versioned field!)
            verifyResultCount("nv_deref2:cucumber", 0); // old value should be removed from index (non versioned field!)
            verifyResultCount("nv_deref3:cucumber", 0); // old value should be removed from index (non versioned field!)
            verifyResultCount("nv_deref4:eggplant", 1); // record4(-branch) = record3

            // Update record3, index for record4 should be updated
            log.debug("Begin test NV10");
            record3.setField(nvfield1.getName(), "courgette");
            expectEvent(UPDATE, Table.RECORD.name, record3.getId(), nvfield1.getId());
            defaultRepository.update(record3);

            commitIndex();
            verifyResultCount("nv_deref4:courgette", 1); // record4(-branch) = record3
            verifyResultCount("nv_deref4:eggplant", 0); // old value should be removed from index (non versioned field!)

            // Update record4, index for record3 and record1 should be updated
            log.debug("Begin test NV10.1");
            record4.setField(nvfield1.getName(), "cauliflower"); //FIXME: 2nd courgette; use something else here
            expectEvent(UPDATE, Table.RECORD.name, record4.getId(), nvfield1.getId());
            defaultRepository.update(record4);

            commitIndex();
            verifyResultCount("nv_deref5:cauliflower", 1); // record3{+branch} produces record4
            verifyResultCount("nv_deref5:broccoli", 0);    // old value should be removed from index
            verifyResultCount("nv_deref5:courgette", 0);   // sanity check, nothing could match
            verifyResultCount("nv_deref6:cauliflower", 2); // record1{+branch,+lang} = record4 (2 vtags!)
            verifyResultCount("nv_deref6:broccoli", 0);    // old value should be removed from index
            verifyResultCount("nv_deref7:cauliflower", 1); // record3{+branch=dev} = record4
            verifyResultCount("nv_deref7:broccoli", 0);    // old value should be removed from index

            // Delete record 3: index for record 4 should be updated
            log.debug("Begin test NV11");
            verifyResultCount("lily.id:" + ClientUtils.escapeQueryChars(record3.getId().toString()), 1);
            expectEvent(DELETE, Table.RECORD.name, record3.getId());
            defaultRepository.delete(record3.getId());

            commitIndex();
            verifyResultCount("nv_deref4:cauliflower", 0);
            verifyResultCount("nv_deref3:tomato", 1);
            verifyResultCount("lily.id:" + ClientUtils.escapeQueryChars(record3.getId().toString()), 0);

            // Delete record 4
            log.debug("Begin test NV12");
            expectEvent(DELETE, Table.RECORD.name, record4.getId());
            defaultRepository.delete(record4.getId());

            commitIndex();
            verifyResultCount("nv_deref3:tomato", 0);
            verifyResultCount("nv_field1:broccoli", 0);
            verifyResultCount("lily.id:" + ClientUtils.escapeQueryChars(record4.getId().toString()), 0);

            // Delete record 1: index of record 2 should be updated
            log.debug("Begin test NV13");
            expectEvent(DELETE, Table.RECORD.name, record1.getId());
            defaultRepository.delete(record1.getId());

            commitIndex();
            verifyResultCount("nv_deref1:tomato", 0);
            verifyResultCount("nv_field1:mushroom", 1);
        }

        assertEquals("All received messages are correct.", 0, messageVerifier.getFailures());
    }

    @Test
    public void testIndexerWithVersioning() throws Exception {
        changeIndexUpdater("indexerconf1.xml");

        messageVerifier.init();

        //
        // Basic create-update-delete
        //
        {
            log.debug("Begin test V1");
            // Create a record
            Record record = defaultRepository.newRecord();
            record.setRecordType(vRecordType1.getName());
            record.setField(vfield1.getName(), "apple");
            record.setField(liveTag.getName(), new Long(1));
            expectEvent(CREATE, Table.RECORD.name, record.getId(), 1L, null, vfield1.getId(), liveTag.getId());
            record = defaultRepository.create(record);

            commitIndex();
            verifyResultCount("v_field1:apple", 1);
            verifyResultCount("+v_field1:apple +lily.version:1", 1);
            verifyResultCount("+v_field1:apple +lily.version:2", 0);

            // Update the record, this will create a new version, but we leave the live version tag pointing to version 1
            log.debug("Begin test V2");
            record.setField(vfield1.getName(), "pear");
            expectEvent(UPDATE, Table.RECORD.name, record.getId(), 2L, null, vfield1.getId());
            defaultRepository.update(record);

            commitIndex();
            verifyResultCount("v_field1:pear", 0);
            verifyResultCount("v_field1:apple", 1);

            // Now move the live version tag to point to version 2
            log.debug("Begin test V3");
            record.setField(liveTag.getName(), new Long(2));
            expectEvent(UPDATE, Table.RECORD.name, record.getId(), liveTag.getId());
            record = defaultRepository.update(record);

            commitIndex();
            verifyResultCount("v_field1:pear", 1);
            verifyResultCount("v_field1:apple", 0);

            // Now remove the live version tag
            log.debug("Begin test V4");
            record.delete(liveTag.getName(), true);
            expectEvent(UPDATE, Table.RECORD.name, record.getId(), liveTag.getId());
            record = defaultRepository.update(record);

            commitIndex();
            verifyResultCount("v_field1:pear", 0);

            // Now test with multiple version tags
            log.debug("Begin test V5");
            record.setField(liveTag.getName(), new Long(1));
            record.setField(previewTag.getName(), new Long(2));
            record.setField(latestTag.getName(), new Long(2));
            expectEvent(UPDATE, Table.RECORD.name, record.getId(), liveTag.getId(), previewTag.getId(), latestTag.getId());
            record = defaultRepository.update(record);

            commitIndex();
            verifyResultCount("v_field1:apple", 1);
            verifyResultCount("v_field1:pear", 2);

            verifyResultCount("+v_field1:pear +lily.vtagId:" + qesc(previewTag.getId().toString()), 1);
            verifyResultCount("+v_field1:pear +lily.vtagId:" + qesc(latestTag.getId().toString()), 1);
            verifyResultCount("+v_field1:pear +lily.vtagId:" + qesc(liveTag.getId().toString()), 0);
            verifyResultCount("+v_field1:apple +lily.vtagId:" + qesc(liveTag.getId().toString()), 1);
        }

        //
        // Deref
        //
        {
            // Create 4 records for the 4 kinds of dereferenced fields
            log.debug("Begin test V6");
            Record record1 = defaultRepository.newRecord();
            record1.setRecordType(vRecordType1.getName());
            record1.setField(vfield1.getName(), "fig");
            record1.setField(liveTag.getName(), Long.valueOf(1));
            expectEvent(CREATE, Table.RECORD.name, record1.getId(), 1L, null, vfield1.getId(), liveTag.getId());
            record1 = defaultRepository.create(record1);

            Record record2 = defaultRepository.newRecord();
            record2.setRecordType(vRecordType1.getName());
            record2.setField(vLinkField1.getName(), new Link(record1.getId()));
            record2.setField(liveTag.getName(), Long.valueOf(1));
            expectEvent(CREATE, Table.RECORD.name, record2.getId(), 1L, null, vLinkField1.getId(), liveTag.getId());
            record2 = defaultRepository.create(record2);

            commitIndex();
            verifyResultCount("v_deref1:fig", 1);

            log.debug("Begin test V6.1");
            RecordId record3Id = idGenerator.newRecordId(record1.getId(), Collections.singletonMap("lang", "en"));
            Record record3 = defaultRepository.newRecord(record3Id);
            record3.setRecordType(vRecordType1.getName());
            record3.setField(vfield1.getName(), "banana");
            record3.setField(liveTag.getName(), Long.valueOf(1));
            expectEvent(CREATE, Table.RECORD.name, record3.getId(), 1L, null, vfield1.getId(), liveTag.getId());
            record3 = defaultRepository.create(record3);

            commitIndex();
            verifyResultCount("v_deref3:fig", 1);

            log.debug("Begin test V6.2");
            Map<String, String> varprops = new HashMap<String, String>();
            varprops.put("lang", "en");
            varprops.put("branch", "dev");
            RecordId record4Id = idGenerator.newRecordId(record1.getId(), varprops);
            Record record4 = defaultRepository.newRecord(record4Id);
            record4.setRecordType(vRecordType1.getName());
            record4.setField(vfield1.getName(), "coconut");
            record4.setField(liveTag.getName(), Long.valueOf(1));
            expectEvent(CREATE, Table.RECORD.name, record4.getId(), 1L, null, vfield1.getId(), liveTag.getId());
            record4 = defaultRepository.create(record4);

            commitIndex();
            verifyResultCount("v_deref3:fig", 2);       //master=>v_field1 (record3 and record4)
            verifyResultCount("v_deref2:fig", 1);       //-branch,-lang=>v_field1 (record4)
            verifyResultCount("v_deref4:banana", 1);    //-branch=>v_field1 (record4)
            verifyResultCount("v_deref5:coconut", 1);   //+branch=>v_field1 (record3)

            // remove the live tag from record1
            log.debug("Begin test V7");
            record1.delete(liveTag.getName(), true);
            expectEvent(UPDATE, Table.RECORD.name, record1.getId(), liveTag.getId());
            record1 = defaultRepository.update(record1);

            commitIndex();
            verifyResultCount("v_deref1:fig", 0);

            // and add the live tag again record1
            log.debug("Begin test V8");
            record1.setField(liveTag.getName(), Long.valueOf(1));
            expectEvent(UPDATE, Table.RECORD.name, record1.getId(), liveTag.getId());
            record1 = defaultRepository.update(record1);

            commitIndex();
            verifyResultCount("v_deref1:fig", 1);

            // Make second version of record1, assign both versions different tags, and assign these tags also
            // to version1 of record2.
            log.debug("Begin test V9");
            record1.setField(vfield1.getName(), "strawberries");
            record1.setField(previewTag.getName(), Long.valueOf(2));
            expectEvent(UPDATE, Table.RECORD.name, record1.getId(), 2L, null, vfield1.getId(), previewTag.getId());
            record1 = defaultRepository.update(record1);

            record2.setField(previewTag.getName(), Long.valueOf(1));
            expectEvent(UPDATE, Table.RECORD.name, record2.getId(), previewTag.getId());
            record2 = defaultRepository.update(record2);

            commitIndex();
            verifyResultCount("+v_deref1:strawberries +lily.vtagId:" + qesc(previewTag.getId().toString()), 1);
            verifyResultCount("+v_deref1:strawberries +lily.vtagId:" + qesc(liveTag.getId().toString()), 0);
            verifyResultCount("+v_deref1:strawberries", 1);
            verifyResultCount("+v_deref1:fig +lily.vtagId:" + qesc(liveTag.getId().toString()), 1);
            verifyResultCount("+v_deref1:fig +lily.vtagId:" + qesc(previewTag.getId().toString()), 0);
            verifyResultCount("+v_deref1:fig", 1);

            // Now do something similar with a 3th version, but first update record2 and then record1
            log.debug("Begin test V10");
            record2.setField(latestTag.getName(), Long.valueOf(1));
            expectEvent(UPDATE, Table.RECORD.name, record2.getId(), latestTag.getId());
            record2 = defaultRepository.update(record2);

            record1.setField(vfield1.getName(), "kiwi");
            record1.setField(latestTag.getName(), Long.valueOf(3));
            expectEvent(UPDATE, Table.RECORD.name, record1.getId(), 3L, null, vfield1.getId(), latestTag.getId());
            record1 = defaultRepository.update(record1);

            commitIndex();
            verifyResultCount("+v_deref1:kiwi +lily.vtag:latest", 1);
            verifyResultCount("+v_deref1:strawberries +lily.vtag:preview", 1);
            verifyResultCount("+v_deref1:fig +lily.vtag:live", 1);
            verifyResultCount("+v_deref1:kiwi +lily.vtag:live", 0);
            verifyResultCount("+v_field1:kiwi +lily.vtag:latest", 1);
            verifyResultCount("+v_field1:fig +lily.vtag:live", 1);

            // Perform updates to record3 and check if denorm'ed data in index of record4 follows
            log.debug("Begin test V11");
            record3.delete(vfield1.getName(), true);
            expectEvent(UPDATE, Table.RECORD.name, record3.getId(), 2L, null, vfield1.getId());
            record3 = defaultRepository.update(record3);

            commitIndex();
            verifyResultCount("v_deref4:banana", 1); // live tag still points to version 1!

            log.debug("Begin test V11.1");
            defaultRepository.read(record3Id, Long.valueOf(2)); // check version 2 really exists
            record3.setField(liveTag.getName(), Long.valueOf(2));
            expectEvent(UPDATE, Table.RECORD.name, record3.getId(), liveTag.getId());
            defaultRepository.update(record3);

            commitIndex();
            verifyResultCount("v_deref4:banana", 0);
            verifyResultCount("v_field1:coconut", 1);

            // Perform updates to record4 and check if denorm'ed data in index of record3 follows
            log.debug("Begin test V12");
            record4.delete(vfield1.getName(), true);
            expectEvent(UPDATE, Table.RECORD.name, record4.getId(), 2L, null, vfield1.getId());
            record4 = defaultRepository.update(record4);

            commitIndex();
            verifyResultCount("v_deref5:coconut", 1); // live tag still points to version 1!

            log.debug("Begin test V12.1");
            defaultRepository.read(record4Id, Long.valueOf(2)); // check version 2 really exists
            record4.setField(liveTag.getName(), Long.valueOf(2));
            expectEvent(UPDATE, Table.RECORD.name, record4.getId(), liveTag.getId());
            defaultRepository.update(record4);

            commitIndex();
            verifyResultCount("v_deref5:coconut", 0); // now it's gone

            // Delete master
            log.debug("Begin test V13");
            expectEvent(DELETE, Table.RECORD.name, record1.getId());
            defaultRepository.delete(record1.getId());

            commitIndex();
            verifyResultCount("v_deref1:fig", 0);
            verifyResultCount("v_deref2:fig", 0);
            verifyResultCount("v_deref3:fig", 0);
        }

        //
        // Test that when using vtag pointing to version '0', versioned content is not accessible
        //
        {
            // Plain (without deref)
            log.debug("Begin test V14");
            Record record1 = defaultRepository.newRecord();
            record1.setRecordType(vRecordType1.getName());
            record1.setField(nvfield1.getName(), "rollerblades");
            record1.setField(vfield1.getName(), "bicycle");
            record1.setField(liveTag.getName(), 1L);
            record1.setField(nvTag.getName(), 0L);
            expectEvent(CREATE, Table.RECORD.name, record1.getId(), 1L, null, nvfield1.getId(), vfield1.getId(), liveTag.getId(),
                    nvTag.getId());
            record1 = defaultRepository.create(record1);

            commitIndex();
            verifyResultCount("+lily.vtagId:" + qesc(nvTag.getId().toString()) + " +nv_field1:rollerblades", 1);
            verifyResultCount("+lily.vtagId:" + qesc(nvTag.getId().toString()) + " +v_field1:bicycle", 0);
            verifyResultCount("+lily.vtagId:" + qesc(liveTag.getId().toString()) + " +nv_field1:rollerblades", 1);
            verifyResultCount("+lily.vtagId:" + qesc(liveTag.getId().toString()) + " +v_field1:bicycle", 1);

            // With deref
            log.debug("Begin test V15");
            Record record2 = defaultRepository.newRecord();
            record2.setRecordType(vRecordType1.getName());
            record2.setField(nvLinkField2.getName(), new Link(record1.getId()));
            record2.setField(nvTag.getName(), 0L);
            record2.setField(liveTag.getName(), 0L);
            expectEvent(CREATE, Table.RECORD.name, record2.getId(), nvLinkField2.getId(), nvTag.getId(), liveTag.getId());
            record2 = defaultRepository.create(record2);

            commitIndex();
            verifyResultCount("+lily.vtagId:" + qesc(nvTag.getId().toString()) + " +nv_v_deref:bicycle", 0);
            verifyResultCount("+lily.vtagId:" + qesc(liveTag.getId().toString()) + " +nv_v_deref:bicycle", 1);
        }

        //
        // Test deref from a versionless record via a versioned field to a non-versioned field.
        // From the moment a versioned field is in the deref chain, when the vtag points to version 0,
        // the deref should evaluate to null.
        //
        {
            log.debug("Begin test V18");
            Record record1 = defaultRepository.newRecord();
            record1.setRecordType(vRecordType1.getName());
            record1.setField(nvfield1.getName(), "Brussels");
            record1.setField(nvTag.getName(), 0L);
            expectEvent(CREATE, Table.RECORD.name, record1.getId(), (Long) null, null, nvfield1.getId(), nvTag.getId());
            record1 = defaultRepository.create(record1);

            Record record2 = defaultRepository.newRecord();
            record2.setRecordType(vRecordType1.getName());
            record2.setField(vLinkField1.getName(), new Link(record1.getId()));
            record2.setField(nvTag.getName(), 0L);
            expectEvent(CREATE, Table.RECORD.name, record2.getId(), 1L, null, vLinkField1.getId(), nvTag.getId());
            record2 = defaultRepository.create(record2);

            Record record3 = defaultRepository.newRecord();
            record3.setRecordType(vRecordType1.getName());
            record3.setField(nvLinkField2.getName(), new Link(record2.getId()));
            record3.setField(nvTag.getName(), 0L);
            expectEvent(CREATE, Table.RECORD.name, record3.getId(), (Long) null, null, nvLinkField2.getId(), nvTag.getId());
            record3 = defaultRepository.create(record3);

            commitIndex();
            verifyResultCount("+lily.vtagId:" + qesc(nvTag.getId().toString()) + " +nv_v_nv_deref:Brussels", 0);

            // Give the records a live tag
            log.debug("Begin test V19");
            record1.setField(liveTag.getName(), 0L);
            expectEvent(UPDATE, Table.RECORD.name, record1.getId(), liveTag.getId());
            record1 = defaultRepository.update(record1);

            record2.setField(liveTag.getName(), 1L);
            expectEvent(UPDATE, Table.RECORD.name, record2.getId(), liveTag.getId());
            record2 = defaultRepository.update(record2);

            record3.setField(liveTag.getName(), 0L);
            expectEvent(UPDATE, Table.RECORD.name, record3.getId(), liveTag.getId());
            record3 = defaultRepository.update(record3);

            commitIndex();
            verifyResultCount("+lily.vtagId:" + qesc(liveTag.getId().toString()) + " +nv_v_nv_deref:Brussels", 1);
        }

        //
        // Test many-to-one dereferencing (= deref where there's actually more than one record pointing to another
        // record)
        // (Besides correctness, this test was also added to check/evaluate the processing time)
        //
        {
            log.debug("Begin test V19.1");

            final int COUNT = 5;

            Record record1 = defaultRepository.newRecord();
            record1.setRecordType(vRecordType1.getName());
            record1.setField(vfield1.getName(), "hyponiem");
            record1.setField(liveTag.getName(), 1L);
            expectEvent(CREATE, Table.RECORD.name, record1.getId(), 1L, null, vfield1.getId(), liveTag.getId());
            record1 = defaultRepository.create(record1);

            // Create multiple records
            for (int i = 0; i < COUNT; i++) {
                Record record2 = defaultRepository.newRecord();
                record2.setRecordType(vRecordType1.getName());
                record2.setField(vLinkField1.getName(), new Link(record1.getId()));
                record2.setField(liveTag.getName(), 1L);
                expectEvent(CREATE, Table.RECORD.name, record2.getId(), 1L, null, vLinkField1.getId(), liveTag.getId());
                record2 = defaultRepository.create(record2);
            }

            commitIndex();
            verifyResultCount("v_deref1:hyponiem", COUNT);

            record1.setField(vfield1.getName(), "hyperoniem");
            record1.setField(liveTag.getName(), 2L);
            expectEvent(UPDATE, Table.RECORD.name, record1.getId(), 2L, null, vfield1.getId(), liveTag.getId());
            record1 = defaultRepository.update(record1);
            commitIndex();
            verifyResultCount("v_deref1:hyperoniem", COUNT);
        }

        //
        // Multi-value field tests
        //
        {
            // Test multi-value field
            log.debug("Begin test V30");
            Record record1 = defaultRepository.newRecord();
            record1.setRecordType(vRecordType1.getName());
            record1.setField(vStringMvField.getName(), Arrays.asList("Dog", "Cat"));
            record1.setField(liveTag.getName(), 1L);
            expectEvent(CREATE, Table.RECORD.name, record1.getId(), 1L, null, vStringMvField.getId(), liveTag.getId());
            record1 = defaultRepository.create(record1);

            commitIndex();
            verifyResultCount("v_string_mv:Dog", 1);
            verifyResultCount("v_string_mv:Cat", 1);
            verifyResultCount("v_string_mv:(Dog Cat)", 1);
            verifyResultCount("v_string_mv:(\"Dog Cat\")", 0);

            // Test multiple single-valued fields indexed into one MV field
            // TODO

            // Test single-value field turned into multivalue by formatter
            // TODO

            // Test multi-valued deref to single-valued field
            // TODO
        }

        //
        // Long type tests
        //
        {
            log.debug("Begin test V40");
            Record record1 = defaultRepository.newRecord();
            record1.setRecordType(vRecordType1.getName());
            record1.setField(vLongField.getName(), 123L);
            record1.setField(liveTag.getName(), 1L);
            expectEvent(CREATE, Table.RECORD.name, record1.getId(), 1L, null, vLongField.getId(), liveTag.getId());
            record1 = defaultRepository.create(record1);

            commitIndex();
            verifyResultCount("v_long:123", 1);
            verifyResultCount("v_long:[100 TO 150]", 1);
        }

        //
        // Datetime type test
        //
        {
            log.debug("Begin test V50");
            Record record1 = defaultRepository.newRecord();
            record1.setRecordType(vRecordType1.getName());
            record1.setField(vDateTimeField.getName(), new DateTime(2010, 10, 14, 15, 30, 12, 756, DateTimeZone.UTC));
            record1.setField(liveTag.getName(), 1L);
            expectEvent(CREATE, Table.RECORD.name, record1.getId(), 1L, null, vDateTimeField.getId(), liveTag.getId());
            record1 = defaultRepository.create(record1);

            commitIndex();
            verifyResultCount("v_datetime:\"2010-10-14T15:30:12.756Z\"", 1);
            verifyResultCount("v_datetime:\"2010-10-14T15:30:12Z\"", 0);

            // Test without milliseconds
            log.debug("Begin test V51");
            Record record2 = defaultRepository.newRecord();
            record2.setRecordType(vRecordType1.getName());
            record2.setField(vDateTimeField.getName(), new DateTime(2010, 10, 14, 15, 30, 12, 000, DateTimeZone.UTC));
            record2.setField(liveTag.getName(), 1L);
            expectEvent(CREATE, Table.RECORD.name, record2.getId(), 1L, null, vDateTimeField.getId(), liveTag.getId());
            record2 = defaultRepository.create(record2);

            commitIndex();
            verifyResultCount("v_datetime:\"2010-10-14T15:30:12Z\"", 1);
            verifyResultCount("v_datetime:\"2010-10-14T15:30:12.000Z\"", 1);
            verifyResultCount("v_datetime:\"2010-10-14T15:30:12.000Z/SECOND\"", 1);
        }

        //
        // Date type test
        //
        {
            log.debug("Begin test V60");
            Record record1 = defaultRepository.newRecord();
            record1.setRecordType(vRecordType1.getName());
            record1.setField(vDateField.getName(), new LocalDate(2020, 1, 30));
            record1.setField(liveTag.getName(), 1L);
            expectEvent(CREATE, Table.RECORD.name, record1.getId(), 1L, null, vDateField.getId(), liveTag.getId());
            record1 = defaultRepository.create(record1);

            commitIndex();
            verifyResultCount("v_date:\"2020-01-30T00:00:00Z/DAY\"", 1);
            verifyResultCount("v_date:\"2020-01-30T00:00:00.000Z\"", 1);
            verifyResultCount("v_date:\"2020-01-30T00:00:00Z\"", 1);
            verifyResultCount("v_date:\"2020-01-30T00:00:01Z\"", 0);

            verifyResultCount("v_date:[2020-01-29T00:00:00Z/DAY TO 2020-01-31T00:00:00Z/DAY]", 1);

            log.debug("Begin test V61");
            Record record2 = defaultRepository.newRecord();
            record2.setRecordType(vRecordType1.getName());
            record2.setField(vDateField.getName(), new LocalDate(2020, 1, 30));
            record2.setField(liveTag.getName(), 1L);
            expectEvent(CREATE, Table.RECORD.name, record2.getId(), 1L, null, vDateField.getId(), liveTag.getId());
            record2 = defaultRepository.create(record2);

            commitIndex();
            verifyResultCount("v_date:\"2020-01-30T00:00:00Z/DAY\"", 2);
        }

        //
        // Blob tests
        //
        {
            log.debug("Begin test V70");
            Blob blob1 = createBlob("blob1_msword.doc", "application/msword", "blob1_msword.doc");
            Blob blob1dup = createBlob("blob1_msword.doc", "application/msword", "blob1_msword.doc");
            Blob blob2 = createBlob("blob2.pdf", "application/pdf", "blob2.pdf");
            Blob blob3 =
                    createBlob("blob3_oowriter.odt", "application/vnd.oasis.opendocument.text", "blob3_oowriter.odt");
            Blob blob4 = createBlob("blob4_excel.xls", "application/excel", "blob4_excel.xls");

            // Single-valued blob field
            Record record1 = defaultRepository.newRecord();
            record1.setRecordType(vRecordType1.getName());
            record1.setField(vBlobField.getName(), blob1);
            record1.setField(liveTag.getName(), 1L);
            expectEvent(CREATE, Table.RECORD.name, record1.getId(), 1L, null, vBlobField.getId(), liveTag.getId());
            record1 = defaultRepository.create(record1);

            commitIndex();
            verifyResultCount("v_blob:sollicitudin", 1);
            verifyResultCount("v_blob:\"Sed pretium pretium lorem\"", 1);
            verifyResultCount("v_blob:lily", 0);

            // Multi-value and hierarchical blob field
            log.debug("Begin test V71");
            HierarchyPath path1 = new HierarchyPath(blob1dup, blob2);
            HierarchyPath path2 = new HierarchyPath(blob3, blob4);
            List<HierarchyPath> blobs = Arrays.asList(path1, path2);

            Record record2 = defaultRepository.newRecord();
            record2.setRecordType(vRecordType1.getName());
            record2.setField(vBlobMvHierField.getName(), blobs);
            record2.setField(liveTag.getName(), 1L);
            expectEvent(CREATE, Table.RECORD.name, record2.getId(), 1L, null, vBlobMvHierField.getId(), liveTag.getId());
            record2 = defaultRepository.create(record2);

            commitIndex();
            verifyResultCount("v_blob:blob1", 2);
            verifyResultCount("v_blob:blob2", 1);
            verifyResultCount("v_blob:blob3", 1);
            verifyResultCount("+v_blob:blob4 +v_blob:\"Netherfield Park\"", 1);

            // Nested blob field
            log.debug("Begin test V72");
            List<List<List<Blob>>> nestedBlobs = Arrays.asList(
                    Arrays.<List<Blob>>asList(
                            Arrays.<Blob>asList(
                                    createBlob("niobium".getBytes(), "text/plain", "foo.txt"),
                                    createBlob("tantalum".getBytes(), "text/plain", "foo.txt")
                            ),
                            Arrays.<Blob>asList(
                                    createBlob("fermium".getBytes(), "text/plain", "foo.txt"),
                                    createBlob("seaborgium".getBytes(), "text/plain", "foo.txt")
                            )
                    ),
                    Arrays.<List<Blob>>asList(
                            Arrays.<Blob>asList(
                                    createBlob("einsteinium".getBytes(), "text/plain", "foo.txt")
                            )
                    )
            );

            Record record3 = defaultRepository.newRecord();
            record3.setRecordType(vRecordType1.getName());
            record3.setField(vBlobNestedField.getName(), nestedBlobs);
            record3.setField(liveTag.getName(), 1L);
            expectEvent(CREATE, Table.RECORD.name, record3.getId(), 1L, null, vBlobNestedField.getId(), liveTag.getId());
            record3 = defaultRepository.create(record3);

            commitIndex();
            verifyResultCount("v_blob:niobium", 1);
            verifyResultCount("v_blob:tantalum", 1);
            verifyResultCount("v_blob:fermium", 1);
            verifyResultCount("v_blob:seaborgium", 1);
            verifyResultCount("v_blob:einsteinium", 1);
        }

        //
        // Test field with explicitly configured formatter
        //
        {
            log.debug("Begin test V80");
            Record record1 = defaultRepository.newRecord();
            record1.setRecordType(vRecordType1.getName());
            record1.setField(vDateTimeField.getName(), new DateTime(2058, 10, 14, 15, 30, 12, 756, DateTimeZone.UTC));
            record1.setField(vStringMvField.getName(), Arrays.asList("wood", "plastic"));
            record1.setField(liveTag.getName(), 1L);
            expectEvent(CREATE, Table.RECORD.name, record1.getId(), 1L, null, vDateTimeField.getId(), vStringMvField.getId(),
                    liveTag.getId());
            record1 = defaultRepository.create(record1);

            commitIndex();
            verifyResultCount("year:2058", 1);
            verifyResultCount("firstValue:wood", 1);
            verifyResultCount("firstValue:plastic", 0);
        }

        //
        // Test inheritance of variant properties for link fields
        //
        {
            log.debug("Begin test V100");
            Map<String, String> varProps = new HashMap<String, String>();
            varProps.put("lang", "nl");
            varProps.put("user", "ali");

            RecordId record1Id = defaultRepository.getIdGenerator().newRecordId(varProps);
            Record record1 = defaultRepository.newRecord(record1Id);
            record1.setRecordType(vRecordType1.getName());
            record1.setField(vfield1.getName(), "venus");
            record1.setField(liveTag.getName(), 1L);
            expectEvent(CREATE, Table.RECORD.name, record1.getId(), 1L, null, vfield1.getId(), liveTag.getId());
            record1 = defaultRepository.create(record1);

            RecordId record2Id = defaultRepository.getIdGenerator().newRecordId(varProps);
            Record record2 = defaultRepository.newRecord(record2Id);
            record2.setRecordType(vRecordType1.getName());
            // Notice we make the link to the record without variant properties
            record2.setField(vLinkField1.getName(), new Link(record1.getId().getMaster()));
            record2.setField(liveTag.getName(), 1L);
            expectEvent(CREATE, Table.RECORD.name, record2.getId(), 1L, null, vLinkField1.getId(), liveTag.getId());
            record2 = defaultRepository.create(record2);

            commitIndex();
            verifyResultCount("v_deref1:venus", 1);

            log.debug("Begin test V101");
            record1.setField(vfield1.getName(), "mars");
            record1.setField(liveTag.getName(), 2L);
            expectEvent(UPDATE, Table.RECORD.name, record1.getId(), 2L, null, vfield1.getId(), liveTag.getId());
            record1 = defaultRepository.update(record1);

            commitIndex();
            verifyResultCount("v_deref1:mars", 1);
        }

        // Test that the index is updated when a version is created, in absence of changes to the vtag fields.
        // This would fail if the 'versionCreated' is not in the record event.
        {
            log.debug("Begin test V120");
            Record record1 = defaultRepository.newRecord();
            record1.setRecordType(vRecordType1.getName());
            record1.setField(liveTag.getName(), 1L);
            expectEvent(CREATE, Table.RECORD.name, record1.getId(), liveTag.getId());
            record1 = defaultRepository.create(record1);

            record1.setField(vfield1.getName(), "stool");
            expectEvent(UPDATE, Table.RECORD.name, record1.getId(), 1L, null, vfield1.getId());
            record1 = defaultRepository.update(record1);

            commitIndex();
            verifyResultCount("v_field1:stool", 1);
        }

        // Test that the index is updated when a version is updated, in absence of changes to the vtag fields.
        // This would fail if the 'versionCreated' is not in the record event.
        {
            log.debug("Begin test V130");
            Record record1 = defaultRepository.newRecord();
            record1.setRecordType(vRecordType1.getName());
            record1.setField(liveTag.getName(), 2L);
            record1.setField(vfield1.getName(), "wall");
            expectEvent(CREATE, Table.RECORD.name, record1.getId(), 1L, null, vfield1.getId(), liveTag.getId());
            record1 = defaultRepository.create(record1);

            record1.setField(vfield1.getName(), "floor");
            expectEvent(UPDATE, Table.RECORD.name, record1.getId(), 2L, null, vfield1.getId());
            record1 = defaultRepository.update(record1);

            commitIndex();
            verifyResultCount("v_field1:floor", 1);
        }

        //
        // Test the automatic vtag 'last', which is a virtual vtag which always points to the last version
        // of any record, without having to add it to the record or record type.
        //
        {
            log.debug("Begin test V140");
            Record record1 = defaultRepository.newRecord();
            record1.setRecordType(lastRecordType.getName());
            record1.setField(nvfield1.getName(), "north");
            expectEvent(CREATE, Table.RECORD.name, record1.getId(), nvfield1.getId());
            record1 = defaultRepository.create(record1);

            commitIndex();
            verifyResultCount("+lily.vtagId:" + qesc(lastTag.getId().toString()) + " +nv_field1:north", 1);

            record1.setField(vfield1.getName(), "south");
            expectEvent(UPDATE, Table.RECORD.name, record1.getId(), 1L, null, vfield1.getId());
            record1 = defaultRepository.update(record1);

            commitIndex();
            verifyResultCount("+lily.vtagId:" + qesc(lastTag.getId().toString()) + " +nv_field1:north", 1);
            verifyResultCount("+lily.vtag:last +v_field1:south", 1);
        }

        assertEquals("All received messages are correct.", 0, messageVerifier.getFailures());
    }

    @Test
    public void testDynamicFields() throws Exception {
        messageVerifier.init();

        //
        // Create schema
        //
        ValueType stringValueType = typeManager.getValueType("STRING");
        ValueType longValueType = typeManager.getValueType("LONG");
        ValueType mvStringValueType = typeManager.getValueType("LIST<STRING>");
        ValueType hierStringValueType = typeManager.getValueType("PATH<STRING>");
        ValueType dateValueType = typeManager.getValueType("DATE");
        ValueType blobValueType = typeManager.getValueType("BLOB");

        FieldType field1 = typeManager.createFieldType(stringValueType, new QName(DYN_NS1, "field1"), Scope.VERSIONED);

        FieldType field2 = typeManager.createFieldType(stringValueType, new QName(DYN_NS2, "field2"), Scope.VERSIONED);

        FieldType field3 =
                typeManager.createFieldType(stringValueType, new QName(DYN_NS2, "name_field3"), Scope.VERSIONED);

        FieldType field4 =
                typeManager.createFieldType(longValueType, new QName(DYN_NS2, "name_field4"), Scope.VERSIONED);

        FieldType field5 =
                typeManager.createFieldType(mvStringValueType, new QName(DYN_NS2, "name_field5"), Scope.VERSIONED);

        FieldType field6 =
                typeManager.createFieldType(stringValueType, new QName(DYN_NS2, "scope_field6"), Scope.VERSIONED);

        FieldType field7 =
                typeManager.createFieldType(stringValueType, new QName(DYN_NS2, "scope_field7"), Scope.NON_VERSIONED);

        FieldType field8 = typeManager.createFieldType(dateValueType, new QName(DYN_NS2, "field8"), Scope.VERSIONED);

        FieldType field9 =
                typeManager.createFieldType(mvStringValueType, new QName(DYN_NS2, "mv_field9"), Scope.VERSIONED);

        FieldType field10 =
                typeManager.createFieldType(hierStringValueType, new QName(DYN_NS2, "hier_field10"), Scope.VERSIONED);

        FieldType field11 =
                typeManager.createFieldType(stringValueType, new QName(DYN_NS2, "field11"), Scope.VERSIONED_MUTABLE);

        FieldType field12 =
                typeManager.createFieldType(stringValueType, new QName(DYN_NS2, "field12"), Scope.VERSIONED_MUTABLE);

        FieldType field13 = typeManager.createFieldType(blobValueType, new QName(DYN_NS2, "field13"), Scope.VERSIONED);

        FieldType field14 = typeManager.createFieldType(blobValueType, new QName(DYN_NS2, "field14"), Scope.VERSIONED);

        RecordType rt = typeManager.newRecordType(new QName(DYN_NS1, "RecordType"));
        // It's not necessary to add the fields
        rt = typeManager.createRecordType(rt);

        changeIndexUpdater("indexerconf_dynfields.xml");

        //
        // Test various matching options
        //
        {
            log.debug("Begin test V300");
            // Create a record
            Record record = defaultRepository.newRecord();
            record.setRecordType(rt.getName());

            // namespace match fields
            record.setField(field1.getName(), "vector");
            record.setField(field2.getName(), "circle");
            // name match fields
            record.setField(field3.getName(), "sphere");
            record.setField(field4.getName(), new Long(983));
            record.setField(field5.getName(), Arrays.asList("prism", "cone"));
            // scope match fields
            record.setField(field6.getName(), "polygon");
            record.setField(field7.getName(), "polyhedron");
            // type match fields
            record.setField(field8.getName(), new LocalDate(2011, 4, 11));
            // multi-value match fields
            record.setField(field9.getName(), Arrays.asList("decagon", "dodecahedron"));
            // hierarchical match fields
            record.setField(field10.getName(), new HierarchyPath("triangle", "knot"));

            expectEvent(CREATE,  Table.RECORD.name, record.getId(), 1L, null, field1.getId(), field2.getId(), field3.getId(),
                    field4.getId(), field5.getId(), field6.getId(), field7.getId(), field8.getId(), field9.getId(),
                    field10.getId());
            record = defaultRepository.create(record);

            commitIndex();

            // Verify only the field from the matched namespace was indexed
            verifyResultCount("dyn1_field1_string:vector", 1);
            verifyResultCount("dyn1_field2_string:circle", 0);

            // Verify name-based match
            verifyResultCount("nameMatch_field3_string:sphere", 1);
            verifyResultCount("nameMatch_field4_long:983", 1);
            verifyResultCount("nameMatch_field5_string_mv:prism", 1);
            verifyResultCount("nameMatch_field5_string_mv:cone", 1);

            // Verify scope-based match
            verifyResultCount("scopeMatch_field6_string:polygon", 0);
            verifyResultCount("scopeMatch_field7_string:polyhedron", 1);

            // Verify type-based match
            verifyResultCount("typeMatch_field8_date:\"2011-04-11T00:00:00Z/DAY\"", 1);

            // Verify multi-value based match
            verifyResultCount("multiValueMatch_field9_string_mv:decagon", 1);

            // Verify hierarchical based match
            verifyResultCount("hierarchicalMatch_field10_hier_literal:\"/triangle/knot\"", 1);
        }

        //
        // Test that index is updated when fields change, without any change to vtags. This verifies
        // that the logic which verifies whether any reindexing needs to be done takes dynamic fields
        // into account.
        //
        {
            log.debug("Begin test V301");
            Record record = defaultRepository.newRecord();
            record.setRecordType(rt.getName());

            record.setField(field11.getName(), "parallelepiped");
            record.setField(field12.getName(), "rectangle");

            expectEvent(CREATE, Table.RECORD.name, record.getId(), 1L, null, field11.getId(), field12.getId());
            record = defaultRepository.create(record);

            commitIndex();

            verifyResultCount("field11_string:parallelepiped", 1);
            verifyResultCount("field12_string:rectangle", 1);

            // Update only the dynamically indexed field
            record.setField(field12.getName(), "square");
            expectEvent(UPDATE, Table.RECORD.name, record.getId(), null, 1L, field12.getId());
            record = defaultRepository.update(record, true, true);
            commitIndex();
            verifyResultCount("field12_string:square", 1);

            // Update only the statically indexed field
            record.setField(field11.getName(), "square");
            expectEvent(UPDATE, Table.RECORD.name, record.getId(), null, 1L, field11.getId());
            record = defaultRepository.update(record, true, true);
            commitIndex();
            verifyResultCount("field11_string:square", 1);
        }

        //
        // Test blobs
        //
        {
            log.debug("Begin test V302");

            Record record = defaultRepository.newRecord();
            record.setRecordType(rt.getName());

            Blob blob1 = createBlob("blob2.pdf", "application/pdf", "blob2.pdf");
            Blob blob2 = createBlob("blob2.pdf", "application/pdf", "blob2.pdf");

            record.setField(field13.getName(), blob1);
            record.setField(field14.getName(), blob2);

            expectEvent(CREATE, Table.RECORD.name, record.getId(), 1L, null, field13.getId(), field14.getId());
            record = defaultRepository.create(record);

            commitIndex();

            // extractContent is not enabled for field13, search on content should not find anything
            verifyResultCount("field13_string:tired", 0);

            // extractContent is enabled for field14
            verifyResultCount("field14_string:tired", 1);
        }

        //
        // Attention: we change the indexerconf here
        //
        changeIndexUpdater("indexerconf_dynfields_continue.xml");

        //
        // Test the fall-through behavior (continue="true") of dynamic fields
        //
        {
            log.debug("Begin test V303");

            Record record = defaultRepository.newRecord();
            record.setRecordType(rt.getName());

            record.setField(field1.getName(), "mega");
            record.setField(field2.getName(), "giga");

            expectEvent(CREATE, Table.RECORD.name, record.getId(), 1L, null, field1.getId(), field2.getId());
            record = defaultRepository.create(record);

            commitIndex();

            verifyResultCount("dyncont_field1_first_string:mega", 1);
            verifyResultCount("dyncont_field2_first_string:giga", 1);

            verifyResultCount("dyncont_field1_second_string:mega", 1);
            verifyResultCount("dyncont_field2_second_string:giga", 1);

            verifyResultCount("dyncont_field1_third_string:mega", 0);
            verifyResultCount("dyncont_field2_third_string:giga", 0);
        }

        //
        // Attention: we change the indexerconf here
        //
        changeIndexUpdater("indexerconf_fulldynamic.xml");

        //
        // Test a 'fully dynamic' mapping
        //
        {
            log.debug("Begin test V304");

            Record record = defaultRepository.newRecord();
            record.setRecordType(rt.getName());

            Blob blob = createBlob("blob2.pdf", "application/pdf", "blob2.pdf");

            record.setField(field1.getName(), "gauss");
            record.setField(field2.getName(), "hilbert");
            record.setField(field4.getName(), new Long(1024));
            record.setField(field14.getName(), blob);

            expectEvent(CREATE, Table.RECORD.name, record.getId(), 1L, null, field1.getId(), field2.getId(), field4.getId(),
                    field14.getId());
            record = defaultRepository.create(record);

            commitIndex();

            verifyResultCount("fulldyn_field1_string:gauss", 1);
            verifyResultCount("fulldyn_field2_string:hilbert", 1);
            verifyResultCount("fulldyn_name_field4_long:1024", 1);
            verifyResultCount("fulldyn_field14_blob:conversations", 1);
        }

        assertEquals("All received messages are correct.", 0, messageVerifier.getFailures());
    }

    @Test
    public void testSystemFields() throws Exception {
        messageVerifier.init();

        //
        // Create schema
        //
        log.debug("Begin test V401");
        ValueType stringValueType = typeManager.getValueType("STRING");
        ValueType linkValueType = typeManager.getValueType("LINK");

        FieldType field1 = typeManager.createFieldType(stringValueType, new QName(NS, "sf_field1"), Scope.VERSIONED);

        FieldType field2 = typeManager.createFieldType(linkValueType, new QName(NS, "sf_field2"), Scope.VERSIONED);

        RecordType supertype1 = typeManager.newRecordType(new QName(NS, "sf_supertype1"));
        supertype1 = typeManager.createRecordType(supertype1);

        RecordType supertype2 = typeManager.newRecordType(new QName(NS, "sf_supertype2"));
        supertype2 = typeManager.createRecordType(supertype2);

        // Create a record type with two versions
        RecordType rt = typeManager.newRecordType(new QName(NS, "sf_rt"));
        rt.addFieldTypeEntry(field1.getId(), false);
        rt.addFieldTypeEntry(field2.getId(), false);
        rt.addSupertype(supertype1.getId());
        rt = typeManager.createRecordType(rt);

        rt.addSupertype(supertype2.getId(), supertype2.getVersion());
        rt = typeManager.updateRecordType(rt);

        RecordType rt2 = typeManager.newRecordType(new QName(NS, "sf_rt2"));
        rt2.addFieldTypeEntry(field1.getId(), false);
        rt2.addFieldTypeEntry(field2.getId(), false);
        rt2 = typeManager.createRecordType(rt2);

        //
        // Change indexer conf
        //
        log.debug("Begin test V402");
        changeIndexUpdater("indexerconf_sysfields.xml");

        //
        // Create content
        //

        // Create a record that uses version 1 of the record type
        log.debug("Begin test V403");
        Record record1 = defaultRepository.newRecord(idGenerator.newRecordId());
        record1.setRecordType(rt.getName(), 1L);
        record1.setField(field1.getName(), "acute");
        expectEvent(CREATE, Table.RECORD.name, record1.getId(), 1L, null, field1.getId());
        record1 = defaultRepository.createOrUpdate(record1);

        // Create a record that uses version 2 of the record type
        log.debug("Begin test V405");
        Record record2 = defaultRepository.newRecord(idGenerator.newRecordId());
        record2.setRecordType(rt.getName(), 2L);
        record2.setField(field1.getName(), "obtuse");
        expectEvent(CREATE, Table.RECORD.name, record2.getId(), 1L, null, field1.getId());
        record2 = defaultRepository.createOrUpdate(record2);

        // Create a record which links to one of the other records
        log.debug("Begin test V406");
        Record record3 = defaultRepository.newRecord(idGenerator.newRecordId());
        record3.setRecordType(rt.getName());
        record3.setField(field2.getName(), new Link(record2.getId()));
        expectEvent(CREATE, Table.RECORD.name, record3.getId(), 1L, null, field2.getId());
        record3 = defaultRepository.createOrUpdate(record3);

        //
        // Test searches
        //
        commitIndex();

        log.debug("Begin test V407");

        verifyResultCount("sf_field1_string:acute", 1);
        verifyResultCount("sf_field1_string:obtuse", 1);

        // recordType
        verifyResultCount("+sf_field1_string:acute +recordType_literal:" +
                qesc("{org.lilyproject.indexer.test}sf_rt"), 1);
        verifyResultCount("+sf_field1_string:obtuse +recordType_literal:" +
                qesc("{org.lilyproject.indexer.test}sf_rt"), 1);

        // recordTypeWithVersion
        verifyResultCount("+sf_field1_string:acute +recordTypeWithVersion_literal:" +
                qesc("{org.lilyproject.indexer.test}sf_rt:1"), 1);
        verifyResultCount("+sf_field1_string:acute +recordTypeWithVersion_literal:" +
                qesc("{org.lilyproject.indexer.test}sf_rt:2"), 0);
        verifyResultCount("+sf_field1_string:obtuse +recordTypeWithVersion_literal:" +
                qesc("{org.lilyproject.indexer.test}sf_rt:2"), 1);

        // recordTypeName
        verifyResultCount("+sf_field1_string:acute +recordTypeName_literal:" + qesc("sf_rt"), 1);

        // recordTypeNamespace
        verifyResultCount("+sf_field1_string:acute +recordTypeNamespace_literal:" +
                qesc("org.lilyproject.indexer.test"), 1);

        // recordTypeVersion
        verifyResultCount("+sf_field1_string:acute +recordTypeVersion_literal:1", 1);
        verifyResultCount("+sf_field1_string:obtuse +recordTypeVersion_literal:2", 1);

        // supertypes
        verifyResultCount("+sf_field1_string:acute +supertypes_literal_mv:" +
                qesc("{org.lilyproject.indexer.test}sf_supertype1"), 1);
        verifyResultCount("+sf_field1_string:acute +supertypes_literal_mv:" +
                qesc("{org.lilyproject.indexer.test}sf_supertype2"), 0);
        verifyResultCount("+sf_field1_string:acute +supertypes_literal_mv:" +
                qesc("{org.lilyproject.indexer.test}sf_rt"), 0);

        verifyResultCount("+sf_field1_string:obtuse +supertypes_literal_mv:" +
                qesc("{org.lilyproject.indexer.test}sf_supertype1"), 1);
        verifyResultCount("+sf_field1_string:obtuse +supertypes_literal_mv:" +
                qesc("{org.lilyproject.indexer.test}sf_supertype2"), 1);

        // supertypesWithVersion
        verifyResultCount("+sf_field1_string:acute +supertypesWithVersion_literal_mv:" +
                qesc("{org.lilyproject.indexer.test}sf_supertype1:1"), 1);

        // supertypeNames
        verifyResultCount("+sf_field1_string:obtuse +supertypeNames_literal_mv:" + qesc("sf_supertype1"), 1);
        verifyResultCount("+sf_field1_string:obtuse +supertypeNames_literal_mv:" + qesc("sf_supertype2"), 1);
        verifyResultCount("+sf_field1_string:obtuse +supertypeNames_literal_mv:" + qesc("sf_supertype_not_existing"), 0);

        // supertypeNamespaces
        verifyResultCount("+sf_field1_string:obtuse +supertypeNamespaces_literal_mv:" +
                qesc("org.lilyproject.indexer.test"), 1);

        // recordTypes (record type + supertypes)
        verifyResultCount("+sf_field1_string:acute +recordTypes_literal_mv:" +
                qesc("{org.lilyproject.indexer.test}sf_supertype1"), 1);
        verifyResultCount("+sf_field1_string:acute +recordTypes_literal_mv:" +
                qesc("{org.lilyproject.indexer.test}sf_supertype2"), 0);
        verifyResultCount("+sf_field1_string:acute +recordTypes_literal_mv:" +
                qesc("{org.lilyproject.indexer.test}sf_rt"), 1);

        // recordTypesWithVersion
        verifyResultCount("+sf_field1_string:obtuse +recordTypesWithVersion_literal_mv:" +
                qesc("{org.lilyproject.indexer.test}sf_supertype1:1"), 1);
        verifyResultCount("+sf_field1_string:obtuse +recordTypesWithVersion_literal_mv:" +
                qesc("{org.lilyproject.indexer.test}sf_supertype2:1"), 1);
        verifyResultCount("+sf_field1_string:obtuse +recordTypesWithVersion_literal_mv:" +
                qesc("{org.lilyproject.indexer.test}sf_rt:2"), 1);

        // recordTypeNames
        verifyResultCount("+sf_field1_string:obtuse +recordTypeNames_literal_mv:" + qesc("sf_supertype1"), 1);
        verifyResultCount("+sf_field1_string:obtuse +recordTypeNames_literal_mv:" + qesc("sf_supertype2"), 1);
        verifyResultCount("+sf_field1_string:obtuse +recordTypeNames_literal_mv:" + qesc("sf_rt"), 1);

        // recordTypeNamespaces
        verifyResultCount("+sf_field1_string:obtuse +recordTypeNamespaces_literal_mv:" +
                qesc("org.lilyproject.indexer.test"), 1);

        // record type via deref
        verifyResultCount("+recordType_deref_literal:" + qesc("{org.lilyproject.indexer.test}sf_rt"), 1);

        // Update record 2, can't verify anything immediately, this is just to check denormalized
        // update of expressions pointing to the fake system fields does not give problems
        log.debug("Begin test V408");
        record2.setField(field1.getName(), "obtuse2");
        expectEvent(UPDATE, Table.RECORD.name, record2.getId(), 2L, null, field1.getId());
        record2 = defaultRepository.createOrUpdate(record2);

        // Change record type of record 2. The denormalized reference of it stored in the index entry
        // of record 3 will not be updated as this is currently not supported.
        log.debug("Begin test V409");
        record2 = defaultRepository.newRecord(record2.getId());
        record2.setRecordType(rt2.getName());
        record2.setField(field1.getName(),
                "obtuse3"); // currently can't only change record type, so touch field as well
        expectEvent(UPDATE, Table.RECORD.name, record2.getId(), 3L, null, true, field1.getId());
        record2 = defaultRepository.update(record2);

        commitIndex();

        // Deref field still contains old record type
        verifyResultCount("+recordType_deref_literal:" + qesc("{org.lilyproject.indexer.test}sf_rt"), 1);

        // Touch record 3 and retest
        record3.setField(field1.getName(), "right");
        expectEvent(UPDATE, Table.RECORD.name, record3.getId(), 2L, null, field1.getId());
        record3 = defaultRepository.update(record3);

        commitIndex();
        verifyResultCount("+recordType_deref_literal:" + qesc("{org.lilyproject.indexer.test}sf_rt2"), 1);

        assertEquals("All received messages are correct.", 0, messageVerifier.getFailures());
    }

    @Test
    public void testComplexFields() throws Exception {
        messageVerifier.init();

        //
        // Create schema
        //
        log.debug("Begin test V501");
        FieldType nestedListsField = typeManager.createFieldType(typeManager.getValueType("LIST<LIST<STRING>>"),
                new QName(NS, "cf_nestedlists"), Scope.NON_VERSIONED);

        FieldType recordField = typeManager.createFieldType(typeManager.getValueType("RECORD"),
                new QName(NS, "cf_record"), Scope.NON_VERSIONED);

        FieldType recordListField = typeManager.createFieldType(typeManager.getValueType("LIST<RECORD>"),
                new QName(NS, "cf_recordlist"), Scope.NON_VERSIONED);

        RecordType cfRecordType = typeManager.recordTypeBuilder()
                .name(new QName(NS, "ComplexFieldsRecordType"))
                .field(nestedListsField.getId(), false)
                .field(recordField.getId(), false)
                .field(recordListField.getId(), false)
                .create();

        //
        // Change indexer conf
        //
        log.debug("Begin test V502");
        changeIndexUpdater("indexerconf_complexfields.xml");

        {
            //
            // Test
            //
            RecordId recordId = idGenerator.newRecordId();
            expectEvent(CREATE, Table.RECORD.name, recordId, nestedListsField.getId(), recordField.getId(), recordListField.getId());

            defaultRepository
                    .recordBuilder()
                    .id(recordId)
                    .recordType(cfRecordType.getName())
                    .field(nestedListsField.getName(),
                            Arrays.asList(
                                    Arrays.asList("dutch", "french", "english"),
                                    Arrays.asList("italian", "greek")
                            ))
                    .field(recordField.getName(),
                            defaultRepository
                                    .recordBuilder()
                                    .recordType(nvRecordType1.getName())
                                    .field(nvfield1.getName(), "german")
                                    .field(nvfield2.getName(), "spanish")
                                    .build())
                    .field(recordListField.getName(),
                            Arrays.asList(
                                    defaultRepository
                                            .recordBuilder()
                                            .recordType(nvRecordType1.getName())
                                            .field(nvfield1.getName(), "swedish")
                                            .field(nvfield2.getName(), "chinese")
                                            .build(),
                                    defaultRepository
                                            .recordBuilder()
                                            .recordType(nvRecordType1.getName())
                                            .field(nvfield1.getName(), "vietnamese")
                                            .field(nvfield2.getName(), "wolof")
                                            .build()
                            )
                    )
                    .create();

            commitIndex();

            verifyResultCount("+cf_nestedlists:italian", 1);
            verifyResultCount("+cf_record:german", 1);
            verifyResultCount("+cf_recordlist:chinese", 1);

            verifyResultCount("+cf_recordlist_field1:swedish", 1);
            verifyResultCount("+cf_recordlist_field1:vietnamese", 1);
            verifyResultCount("+cf_recordlist_field1:chinese", 0);
            verifyResultCount("+cf_recordlist_field1:wolof", 0);

            verifyResultCount("+cf_record_field1:german", 1);
            verifyResultCount("+cf_record_field1:spanish", 0);
        }

        {
            log.debug("Begin test CF503");

            Record beta = defaultRepository.recordBuilder()
                    .recordType(vRecordType1.getName())
                    .field(vfield1.getName(), "whiskey").build();

            Record gamma = defaultRepository.recordBuilder()
                    .recordType(vRecordType1.getName())
                    .field(vfield1.getName(), "wodka").build();

            RecordId alplhaId = idGenerator.newRecordId();
            Record alpha = defaultRepository.recordBuilder().id(alplhaId)
                    .recordType(cfRecordType.getName())
                    .field(recordField.getName(), beta)
                    .field(recordListField.getName(), Lists.newArrayList(beta, gamma)).build();
            expectEvent(CREATE, Table.RECORD.name, alplhaId, recordField.getId(), recordListField.getId());
            alpha = defaultRepository.create(alpha);

            commitIndex();
            verifyFieldValues("+cf_record:whiskey", "cf_shallow_record", "{\"v_field1\":\"whiskey\"}");
            verifyFieldValues("+cf_record:whiskey", "cf_shallow_recordlist", "{\"v_field1\":\"whiskey\"}",
                    "{\"v_field1\":\"wodka\"}");

        }

        assertEquals("All received messages are correct.", 0, messageVerifier.getFailures());
    }

    @Test
    public void testComplexFieldsDerefUpdate() throws Exception {

        messageVerifier.disable();

        final String NS = "org.lilyproject.indexer.test.complexfieldsderef";

        //
        // Create schema
        //
        log.debug("Begin test V601");
        FieldType linkField = typeManager.createFieldType(typeManager.getValueType("LINK"),
                new QName(NS, "link"), Scope.NON_VERSIONED);

        FieldType recordField = typeManager.createFieldType(typeManager.getValueType("RECORD"),
                new QName(NS, "record"), Scope.NON_VERSIONED);

        FieldType record2Field = typeManager.createFieldType(typeManager.getValueType("RECORD"),
                new QName(NS, "record2"), Scope.NON_VERSIONED);

        FieldType stringField = typeManager.createFieldType(typeManager.getValueType("STRING"),
                new QName(NS, "string"), Scope.NON_VERSIONED);

        FieldType recordListField = typeManager.createFieldType(typeManager.getValueType("LIST<RECORD>"),
                new QName(NS, "recordlist"), Scope.NON_VERSIONED);

        RecordType recordType = typeManager.recordTypeBuilder()
                .name(new QName(NS, "RecordType"))
                .field(linkField.getId(), false)
                .field(recordField.getId(), false)
                .field(record2Field.getId(), false)
                .field(stringField.getId(), false)
                .field(recordListField.getId(), false)
                .create();

        //
        // Change indexer conf
        //
        log.debug("Begin test V502");
        changeIndexUpdater("indexerconf_complexfields_deref.xml");

        //
        // Case 1: link field => record field => string field
        //
        {
            log.debug("Begin test V610");

            RecordId recordId = idGenerator.newRecordId();

            defaultRepository
                    .recordBuilder()
                    .recordType(recordType.getName())
                    .field(linkField.getName(),
                            new Link(defaultRepository
                                    .recordBuilder()
                                    .id(recordId)
                                    .recordType(recordType.getName())
                                    .field(recordField.getName(),
                                            defaultRepository
                                                    .recordBuilder()
                                                    .recordType(recordType.getName())
                                                    .field(stringField.getName(), "bordeaux")
                                                    .build())
                                    .create()
                                    .getId()))
                    .create();

            commitIndex();

            verifyResultCount("+cfd_case1:bordeaux", 1);

            // perform update
            log.debug("Begin test V611");

            defaultRepository
                    .recordBuilder()
                    .id(recordId)
                    .field(recordField.getName(),
                            defaultRepository
                                    .recordBuilder()
                                    .recordType(recordType.getName())
                                    .field(stringField.getName(), "bordooo")
                                    .build())
                    .update();

            commitIndex();

            verifyResultCount("+cfd_case1:bordooo", 1);
            verifyResultCount("+cfd_case1:bordeaux", 0);
        }

        //
        // Case 2: link field => record field => link field => string field
        //
        {
            log.debug("Begin test V620");

            RecordId recordId1 = idGenerator.newRecordId();
            RecordId recordId2 = idGenerator.newRecordId();

            defaultRepository
                    .recordBuilder()
                    .recordType(recordType.getName())
                    .field(linkField.getName(),
                            new Link(defaultRepository
                                    .recordBuilder()
                                    .id(recordId1)
                                    .recordType(recordType.getName())
                                    .field(recordField.getName(),
                                            defaultRepository
                                                    .recordBuilder()
                                                    .recordType(recordType.getName())
                                                    .field(linkField.getName(),
                                                            new Link(defaultRepository
                                                                    .recordBuilder()
                                                                    .id(recordId2)
                                                                    .recordType(recordType.getName())
                                                                    .field(stringField.getName(), "beaujolais")
                                                                    .create()
                                                                    .getId()))
                                                    .build())
                                    .create()
                                    .getId()))
                    .create();

            commitIndex();

            verifyResultCount("+cfd_case2:beaujolais", 1);

            // perform update
            log.debug("Begin test V621");

            defaultRepository
                    .recordBuilder()
                    .id(recordId2)
                    .field(stringField.getName(), "booojolais")
                    .update();

            commitIndex();

            verifyResultCount("+cfd_case2:booojolais", 1);
            verifyResultCount("+cfd_case2:beaujolais", 0);
        }

        //
        // Case 3: record field => link field => string field
        //
        {
            log.debug("Begin test V630");

            RecordId recordId = idGenerator.newRecordId();

            defaultRepository
                    .recordBuilder()
                    .recordType(recordType.getName())
                    .field(record2Field.getName(),
                            defaultRepository
                                    .recordBuilder()
                                    .recordType(recordType.getName())
                                    .field(linkField.getName(),
                                            new Link(defaultRepository
                                                    .recordBuilder()
                                                    .id(recordId)
                                                    .recordType(recordType.getName())
                                                    .field(stringField.getName(), "bourgogne")
                                                    .create()
                                                    .getId()))
                                    .build())
                    .create();

            commitIndex();

            verifyResultCount("+cfd_case3:bourgogne", 1);

            // perform an update
            log.debug("Begin test V631");

            defaultRepository
                    .recordBuilder()
                    .id(recordId)
                    .field(stringField.getName(), "boerhonje")
                    .update();

            commitIndex();

            verifyResultCount("+cfd_case3:boerhonje", 1);
            verifyResultCount("+cfd_case3:bourgogne", 0);
        }

        //
        // Case 4: link field => list<record> field => link field => string field
        //
        {
            log.debug("Begin test V640");

            RecordId recordId1 = idGenerator.newRecordId();
            RecordId recordId2 = idGenerator.newRecordId();
            RecordId recordId3 = idGenerator.newRecordId();
            RecordId recordId4 = idGenerator.newRecordId();

            defaultRepository
                    .recordBuilder()
                    .recordType(recordType.getName())
                    .id(recordId1)
                    .field(linkField.getName(),
                            new Link(defaultRepository
                                    .recordBuilder()
                                    .id(recordId2)
                                    .recordType(recordType.getName())
                                    .field(recordListField.getName(),
                                            Arrays.asList(
                                                    defaultRepository
                                                            .recordBuilder()
                                                            .id(recordId3)
                                                            .recordType(recordType.getName())
                                                            .field(linkField.getName(),
                                                                    new Link(defaultRepository
                                                                            .recordBuilder()
                                                                            .id(recordId3)
                                                                            .recordType(recordType.getName())
                                                                            .field(stringField.getName(), "champagne")
                                                                            .create()
                                                                            .getId()))
                                                            .build(),
                                                    defaultRepository
                                                            .recordBuilder()
                                                            .id(recordId4)
                                                            .recordType(recordType.getName())
                                                            .field(linkField.getName(),
                                                                    new Link(defaultRepository
                                                                            .recordBuilder()
                                                                            .id(recordId4)
                                                                            .recordType(recordType.getName())
                                                                            .field(stringField.getName(), "languedoc")
                                                                            .create()
                                                                            .getId()))
                                                            .build()
                                            ))
                                    .create()
                                    .getId()))
                    .create();

            commitIndex();

            verifyResultCount("+cfd_case4:champagne", 1);
            verifyResultCount("+cfd_case4:languedoc", 1);

            // perform an update
            log.debug("Begin test V640");

            defaultRepository
                    .recordBuilder()
                    .id(recordId3)
                    .field(stringField.getName(), "sampanje")
                    .update();

            commitIndex();

            verifyResultCount("+cfd_case4:sampanje", 1);
            verifyResultCount("+cfd_case4:languedoc", 1);
            verifyResultCount("+cfd_case4:champagne", 0);

            // perform another update */
            log.debug("Begin test V641");

            defaultRepository
                    .recordBuilder()
                    .id(recordId2)
                    .recordType(recordType.getName())
                    .field(recordListField.getName(),
                            Arrays.asList(defaultRepository
                                    .recordBuilder()
                                    .recordType(recordType.getName())
                                    .field(linkField.getName(), new Link(recordId3))
                                    .build()))
                    .update();

            commitIndex();

            verifyResultCount("+cfd_case4:sampanje", 1);
            verifyResultCount("+cfd_case4:languedoc", 0);
        }

        //
        // Case 5: link field => record field => record field => string field
        //
        {
            log.debug("Begin test V650");

            RecordId recordId1 = idGenerator.newRecordId();
            RecordId recordId2 = idGenerator.newRecordId();

            defaultRepository
                    .recordBuilder()
                    .id(recordId1)
                    .recordType(recordType.getName())
                    .field(linkField.getName(),
                            new Link(defaultRepository
                                    .recordBuilder()
                                    .id(recordId2)
                                    .recordType(recordType.getName())
                                    .field(recordField.getName(),
                                            defaultRepository
                                                    .recordBuilder()
                                                    .recordType(recordType.getName())
                                                    .field(recordField.getName(),
                                                            defaultRepository
                                                                    .recordBuilder()
                                                                    .recordType(recordType.getName())
                                                                    .field(stringField.getName(), "loire")
                                                                    .build())
                                                    .build())
                                    .create()
                                    .getId()))
                    .create();

            commitIndex();

            verifyResultCount("+cfd_case5:loire", 1);

            // perform an update
            log.debug("Begin test V651");

            defaultRepository
                    .recordBuilder()
                    .id(recordId2)
                    .recordType(recordType.getName())
                    .field(recordField.getName(),
                            defaultRepository
                                    .recordBuilder()
                                    .recordType(recordType.getName())
                                    .field(recordField.getName(),
                                            defaultRepository
                                                    .recordBuilder()
                                                    .recordType(recordType.getName())
                                                    .field(stringField.getName(), "lwaare")
                                                    .build())
                                    .build())
                    .update();

            commitIndex();

            verifyResultCount("+cfd_case5:loire", 0);
            verifyResultCount("+cfd_case5:lwaare", 1);
        }
    }

    /**
     * Tests the correct behavior when a record's state changes so that a different
     * record filter include rule is matched, with different vtags to index.
     */
    @Test
    public void testSwitchBetweenIncludeRules() throws Exception {
        changeIndexUpdater("indexerconf_include_rule_switch.xml");

        messageVerifier.disable();

        // Another include rule will match based on the value of the vfield1 field.

        // First test with vfield1=caseA

        Record record = defaultRepository.newRecord();
        record.setRecordType(vRecordType1.getName());
        record.setField(vfield1.getName(), "caseA");
        record.setField(vfield2.getName(), "guggenheim"); /* theme: NY museums */
        record.setField(liveTag.getName(), 1L);
        record.setField(latestTag.getName(), 1L);
        record = defaultRepository.create(record);

        commitIndex();

        verifyResultCount("+v_field2:guggenheim +lily.vtag:live", 1);
        verifyResultCount("+v_field2:guggenheim +lily.vtag:latest", 1);

        // Now test with vfield1=caseB

        record.setField(vfield1.getName(), "caseB");
        record = defaultRepository.update(record);

        commitIndex();

        verifyResultCount("+v_field2:guggenheim +lily.vtag:live", 0);
        verifyResultCount("+v_field2:guggenheim +lily.vtag:latest", 1);
    }

    /**
     * When a record is only included in an index to trigger updates of denormalized data
     * (that is, when vtags=""), then the IndexUpdater shouldn't do any read operations
     * on the repository.
     */
    @Test
    public void testEmptyVtagsDoesNotDoRepositoryRead() throws Exception {
        changeIndexUpdater("indexerconf_emptyvtags.xml");

        messageVerifier.disable();

        // reset current read count
        indexUpdaterRepositoryMgr.reset();

        TrackingRepository indexUpdaterRepository = (TrackingRepository)indexUpdaterRepositoryMgr.getRepository(Table.RECORD.name);

        Record record = defaultRepository.newRecord();
        record.setRecordType(vRecordType1.getName());
        record.setField(vfield1.getName(), "check");
        record.setField(vfield2.getName(), "met"); /* theme: NY museums */
        record.setField(liveTag.getName(), 1L);
        record.setField(latestTag.getName(), 1L);
        record = defaultRepository.create(record);
        commitIndex();

        assertEquals(0, indexUpdaterRepository.reads());
        verifyResultCount("+v_field2:met +lily.vtag:live", 0);
        verifyResultCount("+v_field2:met +lily.vtag:latest", 0);

        // Check this is also true for updates

        record.setField(vfield2.getName(), "moma");
        record = defaultRepository.update(record);
        commitIndex();

        assertEquals(0, indexUpdaterRepository.reads());
        verifyResultCount("+v_field2:moma +lily.vtag:live", 0);
        verifyResultCount("+v_field2:moma +lily.vtag:latest", 0);

        // And for deletes

        defaultRepository.delete(record.getId());
        commitIndex();

        assertEquals(0, indexUpdaterRepository.reads());
        verifyResultCount("+v_field2:moma +lily.vtag:live", 0);
        verifyResultCount("+v_field2:moma +lily.vtag:latest", 0);

        // But not for records that match a rule with vtags

        record = defaultRepository.newRecord();
        record.setRecordType(vRecordType1.getName());
        record.setField(vfield2.getName(), "met");
        record.setField(liveTag.getName(), 1L);
        record.setField(latestTag.getName(), 1L);
        record = defaultRepository.create(record);
        commitIndex();

        assertEquals(1, indexUpdaterRepository.reads());
        verifyResultCount("+v_field2:met +lily.vtag:live", 1);
        verifyResultCount("+v_field2:met +lily.vtag:latest", 1);
    }

    /**
     * This test might better fit in the indexer-model package
     */
    @Test
    public void testComplexFieldsInvalidConf() throws Exception {
        try {
            changeIndexUpdater("indexerconf_complexfields_invalid1.xml");
            fail("Exception expected");
        } catch (IndexerConfException e) {
            // expected
        }

        try {
            changeIndexUpdater("indexerconf_complexfields_invalid2.xml");
            fail("Exception expected");
        } catch (IndexerConfException e) {
            // expected
        }

        try {
            changeIndexUpdater("indexerconf_complexfields_invalid3.xml");
            fail("Exception expected");
        } catch (IndexerConfException e) {
            // expected
        }
    }

    /**
     * This test might better fit in the indexer-model package
     */
    @Test
    public void testParseComplexConfiguration() throws Exception {
        //
        // Create schema
        //
        FieldType stringField = typeManager.createFieldType(typeManager.getValueType("STRING"),
                new QName(NS, "string"), Scope.NON_VERSIONED);

        typeManager.recordTypeBuilder()
                .name(new QName(NS, "ComplexConfiguration"))
                .field(stringField.getId(), false)
                .create();

        changeIndexUpdater("indexerconf_complex_configuration.xml");

        final IndexFields indexFields = INDEXER_CONF.getIndexFields();
        indexFields.visitAll(new Predicate<MappingNode>() {
            @Override
            public boolean apply(@Nullable MappingNode input) {
                if (input instanceof IndexField) {
                    final IndexField indexField = (IndexField) input;

                    if ("cc_less_variant_spaces".equals(indexField.getName().getTemplate())) {
                        final List<Follow> follows = ((DerefValue) indexField.getValue()).getFollows();
                        assertEquals(1, follows.size());
                        final Set<String> dimensions = ((VariantFollow) follows.get(0)).getDimensions();
                        assertEquals(1, dimensions.size());
                        assertTrue(dimensions.contains("my branch"));
                    } else if ("cc_less_variant_spaces_twice".equals(indexField.getName().getTemplate())) {
                        final List<Follow> follows = ((DerefValue) indexField.getValue()).getFollows();
                        assertEquals(1, follows.size());
                        final Set<String> dimensions = ((VariantFollow) follows.get(0)).getDimensions();
                        assertEquals(2, dimensions.size());
                        assertTrue(dimensions.contains("my branch"));
                        assertTrue(dimensions.contains("some lang"));
                    } else if ("cc_more_variant_spaces".equals(indexField.getName().getTemplate())) {
                        final List<Follow> follows = ((DerefValue) indexField.getValue()).getFollows();
                        assertEquals(1, follows.size());
                        final Map<String, String> dimensions =
                                ((ForwardVariantFollow) follows.get(0)).getDimensions();
                        assertEquals(1, dimensions.size());
                        assertTrue(dimensions.containsKey("my branch"));
                        assertNull(dimensions.get("my branch"));
                    } else if ("cc_more_variant_spaces_twice".equals(indexField.getName().getTemplate())) {
                        final List<Follow> follows = ((DerefValue) indexField.getValue()).getFollows();
                        assertEquals(1, follows.size());
                        final Map<String, String> dimensions =
                                ((ForwardVariantFollow) follows.get(0)).getDimensions();
                        assertEquals(2, dimensions.size());
                        assertTrue(dimensions.containsKey("my branch"));
                        assertNull(dimensions.get("my branch"));
                        assertTrue(dimensions.containsKey("some lang"));
                        assertNull(dimensions.get("some lang"));
                    } else if ("cc_more_variant_spaces_value".equals(indexField.getName().getTemplate())) {
                        final List<Follow> follows = ((DerefValue) indexField.getValue()).getFollows();
                        assertEquals(1, follows.size());
                        final Map<String, String> dimensions =
                                ((ForwardVariantFollow) follows.get(0)).getDimensions();
                        assertEquals(1, dimensions.size());
                        assertTrue(dimensions.containsKey("branch"));
                        assertEquals("some value", dimensions.get("branch"));
                    } else if ("cc_more_variant_spaces_twice_value".equals(indexField.getName().getTemplate())) {
                        final List<Follow> follows = ((DerefValue) indexField.getValue()).getFollows();
                        assertEquals(1, follows.size());
                        final Map<String, String> dimensions =
                                ((ForwardVariantFollow) follows.get(0)).getDimensions();
                        assertEquals(2, dimensions.size());
                        assertTrue(dimensions.containsKey("branch"));
                        assertEquals("some value", dimensions.get("branch"));
                        assertTrue(dimensions.containsKey("lang"));
                        assertEquals("some lang", dimensions.get("lang"));
                    } else if ("cc_more_variant_spaces_key_and_value".equals(indexField.getName().getTemplate())) {
                        final List<Follow> follows = ((DerefValue) indexField.getValue()).getFollows();
                        assertEquals(1, follows.size());
                        final Map<String, String> dimensions =
                                ((ForwardVariantFollow) follows.get(0)).getDimensions();
                        assertEquals(2, dimensions.size());
                        assertTrue(dimensions.containsKey("my branch"));
                        assertEquals("some value", dimensions.get("my branch"));
                        assertTrue(dimensions.containsKey("my lang"));
                        assertEquals("some lang", dimensions.get("my lang"));
                    } else {
                        throw new IllegalStateException("unexpected index field " + indexField.getName().getTemplate());
                    }
                }

                return true; // to make visit continue
            }
        });
    }

    private Blob createBlob(String resource, String mediaType, String fileName) throws Exception {
        byte[] mswordblob = readResource(resource);

        Blob blob = new Blob(mediaType, (long) mswordblob.length, fileName);
        OutputStream os = defaultRepository.getOutputStream(blob);
        try {
            os.write(mswordblob);
        } finally {
            os.close();
        }

        return blob;
    }

    private Blob createBlob(byte[] content, String mediaType, String fileName) throws Exception {
        Blob blob = new Blob(mediaType, (long) content.length, fileName);
        OutputStream os = defaultRepository.getOutputStream(blob);
        try {
            os.write(content);
        } finally {
            os.close();
        }

        return blob;
    }

    private byte[] readResource(String path) throws IOException {
        InputStream mswordblob = getClass().getResourceAsStream(path);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] buffer = new byte[8192];
        int read;
        while ((read = mswordblob.read(buffer)) != -1) {
            bos.write(buffer, 0, read);
        }

        return bos.toByteArray();
    }

    private static String qesc(String input) {
        return ClientUtils.escapeQueryChars(input);
    }

    private void commitIndex() throws Exception {
        commitIndex(Table.RECORD.name);
    }

    private void commitIndex(String tableName) throws Exception {
        // wait for all events that exist at this point in time to be processed
        repoSetup.waitForSepProcessing(tableName);

        // The events that have been processed up to now might themselves have produced new events (reindex events)
        // that now also need to be processed, therefore do another wait.
        repoSetup.waitForSepProcessing();

        solrShardManager.commit(true, true);
    }

    private QueryResponse getQueryResponse(String query) throws SolrClientException, InterruptedException {
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.set("q", query);
        solrQuery.set("rows", 5000);
        return solrShardManager.query(solrQuery);
    }

    private void verifyResultCount(String query, int count) throws SolrClientException, InterruptedException {
        QueryResponse response = getQueryResponse(query);
        if (count != response.getResults().size()) {
            System.out.println("The query result contains a wrong number of documents, here is the result:");
            for (int i = 0; i < response.getResults().size(); i++) {
                SolrDocument result = response.getResults().get(i);
                System.out.println(result.getFirstValue("lily.key"));
            }
        }
        assertEquals("The query result for '" + query + "' contains the wrong number of documents.", count,
                response.getResults().getNumFound());
    }

    private void verifyFieldValues(String query, String fieldName, String... expectedValues)
            throws SolrClientException, InterruptedException {
        QueryResponse response = getQueryResponse(query);
        if (1 != response.getResults().size()) {
            System.out.println("The query result contains a wrong number of documents, here is the result:");
            for (int i = 0; i < response.getResults().size(); i++) {
                SolrDocument result = response.getResults().get(i);
                System.out.println(result.getFirstValue("lily.key"));
            }
        }
        assertEquals(1, response.getResults().getNumFound());

        Assert.assertArrayEquals(expectedValues,
                response.getResults().get(0).getFieldValues(fieldName).toArray(new Object[]{}));
    }

    private void expectEvent(RecordEvent.Type type, String table, RecordId recordId, SchemaId... updatedFields) {
        expectEvent(type, table, recordId, null, null, updatedFields);
    }

    private void expectEvent(RecordEvent.Type type, String table, RecordId recordId, Long versionCreated, Long versionUpdated,
                             SchemaId... updatedFields) {
        expectEvent(type, table, recordId, versionCreated, versionUpdated, false, updatedFields);
    }

    private void expectEvent(RecordEvent.Type type, String table, RecordId recordId, Long versionCreated, Long versionUpdated,
                             boolean recordTypeChanged, SchemaId... updatedFields) {
        RecordEvent event = new RecordEvent();

        event.setType(type);
        event.setTableName(table);

        for (SchemaId updatedField : updatedFields) {
            event.addUpdatedField(updatedField);
        }

        if (versionCreated != null) {
            event.setVersionCreated(versionCreated);
        }

        if (versionUpdated != null) {
            event.setVersionUpdated(versionUpdated);
        }

        if (recordTypeChanged) {
            event.setRecordTypeChanged(recordTypeChanged);
        }

        messageVerifier.addExpectedEvent(recordId, event);
    }

    private static class MessageVerifier implements EventListener {
        private List<Pair<RecordId, RecordEvent>> expectedEvents = Lists.newArrayList();
        private int failures = 0;
        private boolean enabled;

        public int getFailures() {
            return failures;
        }

        public void init() {
            this.enabled = true;
            this.expectedEvents.clear();
            this.failures = 0;
        }

        public void disable() {
            this.enabled = false;
        }

        public void addExpectedEvent(RecordId recordId, RecordEvent recordEvent) {
            this.expectedEvents.add(Pair.create(recordId, recordEvent));
        }

        @Override
        public void processEvent(SepEvent event) {
            if (!enabled) {
                return;
            }

            // In case of failures we print out "load" messages, the main junit thread is expected to
            // test that the failures variable is 0.

            RecordId recordId = defaultRepository.getIdGenerator().fromBytes(event.getRow());

            try {
                RecordEvent recordEvent = new RecordEvent(event.getPayload(), idGenerator);

                if (recordEvent.getType().equals(RecordEvent.Type.INDEX)) {
                    log.debug("Ignoring incoming re-index event for message verification");
                    return;
                }

                if (expectedEvents.isEmpty()) {
                    System.err.println("No events are expected, but we just got event " + recordEvent.toJson() + " on " + recordId);
                    failures++;
                    return;
                }

                Pair<RecordId, RecordEvent> expectedPair = expectedEvents.remove(0);
                RecordId expectedId = expectedPair.getV1();
                RecordEvent expectedEvent = expectedPair.getV2();

                if (expectedEvent == null) {
                    failures++;
                    printSomethingLoad();
                    System.err.println("Did not expect a message, but got:");
                    System.err.println(recordId);
                    System.err.println(recordEvent.toJson());
                } else {
                    if (!recordEvent.equals(expectedEvent) ||
                            !(recordId.equals(expectedId) ||
                                    (expectedId == null && expectedEvent.getType() == CREATE))) {
                        failures++;
                        printSomethingLoad();
                        System.err.println("Expected message:");
                        System.err.println(expectedId);
                        System.err.println(expectedEvent.toJson());
                        System.err.println("Received message:");
                        System.err.println(recordId);
                        System.err.println(recordEvent.toJson());
                    } else {
                        log.debug("Received message ok.");
                    }
                }
            } catch (IOException e) {
                failures++;
                e.printStackTrace();
            }
        }

        private void printSomethingLoad() {
            for (int i = 0; i < 10; i++) {
                System.err.println("!!");
            }
        }
    }

    /**
     * An arbitrary, non-indexing, MQ listener.
     */
    private static class OtherListener implements EventListener {
        private int msgCount;

        @Override
        public void processEvent(SepEvent event)  {
            msgCount++;
        }

        public int getMsgCount() {
            return msgCount;
        }

        public void reset() {
            msgCount = 0;
        }
    }

    private static class CompositeEventListener implements EventListener {
        private List<EventListener> eventListeners;

        CompositeEventListener(EventListener...eventListeners) {
            this.eventListeners = Lists.newArrayList(eventListeners);
        }

        @Override
        public void processEvent(SepEvent event) {
            for (EventListener eventListener : eventListeners) {
                eventListener.processEvent(event);
            }
        }


    }

    private static class TrackingRepositoryManager implements RepositoryManager {

        private RepositoryManager delegate;
        private Map<String,TrackingRepository> repositoryCache = Maps.newHashMap();

        TrackingRepositoryManager(RepositoryManager delegate) {
            this.delegate = delegate;
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        @Override
        public RecordFactory getRecordFactory() {
            return delegate.getRecordFactory();
        }

        @Override
        public IdGenerator getIdGenerator() {
            return delegate.getIdGenerator();
        }

        @Override
        public TypeManager getTypeManager() {
            return delegate.getTypeManager();
        }

        @Override
        public Repository getDefaultRepository() throws IOException, InterruptedException {
            throw new RuntimeException("Default repository should not be used by indexer");
        }

        @Override
        public synchronized Repository getRepository(String tableName) throws IOException, InterruptedException {
            if (!repositoryCache.containsKey(tableName)) {
                Repository repository = delegate.getRepository(tableName);
                TrackingRepository trackingRepository = new TrackingRepository();
                trackingRepository.setDelegate(repository);
                repositoryCache.put(tableName, trackingRepository);
            }
            return repositoryCache.get(tableName);
        }

        public void reset() {
            for (TrackingRepository repo : repositoryCache.values()) {
                repo.reads();
            }
        }
    }

    private static class TrackingRepository extends BaseRepositoryDecorator {
        private int readCount;

        @Override
        public IdRecord readWithIds(RecordId recordId, Long version, List<SchemaId> fieldIds)
                throws RepositoryException, InterruptedException {
            readCount++;
            return super.readWithIds(recordId, version, fieldIds);
        }

        public int reads() {
            int result = readCount;
            readCount = 0;
            return result;
        }
    }
}
