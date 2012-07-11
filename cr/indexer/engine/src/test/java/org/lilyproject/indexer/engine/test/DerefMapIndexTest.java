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

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.thirdparty.guava.common.collect.Maps;
import org.apache.hadoop.thirdparty.guava.common.collect.Sets;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilyproject.hadooptestfw.TestHelper;
import org.lilyproject.hbaseindex.IndexManager;
import org.lilyproject.indexer.engine.DerefMap;
import org.lilyproject.indexer.engine.DerefMap.DependantRecordIdsIterator;
import org.lilyproject.indexer.engine.DerefMapHbaseImpl;
import org.lilyproject.indexer.engine.IndexLocker;
import org.lilyproject.indexer.engine.IndexUpdater;
import org.lilyproject.indexer.engine.IndexUpdaterMetrics;
import org.lilyproject.indexer.engine.Indexer;
import org.lilyproject.indexer.engine.IndexerMetrics;
import org.lilyproject.indexer.engine.SolrClientException;
import org.lilyproject.indexer.engine.SolrShardManagerImpl;
import org.lilyproject.indexer.model.indexerconf.IndexerConf;
import org.lilyproject.indexer.model.indexerconf.IndexerConfBuilder;
import org.lilyproject.linkindex.LinkIndex;
import org.lilyproject.linkindex.LinkIndexUpdater;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.Link;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.api.ValueType;
import org.lilyproject.repotestfw.RepositorySetup;
import org.lilyproject.rowlog.api.RowLogConfigurationManager;
import org.lilyproject.rowlog.api.RowLogException;
import org.lilyproject.rowlog.api.RowLogMessage;
import org.lilyproject.rowlog.api.RowLogMessageListener;
import org.lilyproject.rowlog.api.RowLogMessageListenerMapping;
import org.lilyproject.rowlog.api.RowLogSubscription;
import org.lilyproject.solrtestfw.SolrTestingUtility;
import org.lilyproject.util.repo.RecordEvent;
import org.lilyproject.util.repo.VersionTag;

import static org.junit.Assert.assertEquals;
import static org.lilyproject.util.repo.RecordEvent.Type.CREATE;

/**
 * Tests if the indexer has the desired effect on the derefmap
 */
public class DerefMapIndexTest {
    private final static RepositorySetup repoSetup = new RepositorySetup();
    private static IndexerConf INDEXER_CONF;
    private static SolrTestingUtility SOLR_TEST_UTIL;
    private static Repository repository;
    private static TypeManager typeManager;
    private static IdGenerator idGenerator;
    private static SolrShardManagerImpl solrShardManager;
    private static LinkIndex linkIndex;
    private static DerefMap derefMap;

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
    private static FieldType vLinkField1;

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

    private static Log log = LogFactory.getLog(DerefMapIndexTest.class);

    private static MessageVerifier messageVerifier = new MessageVerifier();
    private static RecordType nvRecordType1;
    private static RecordType vRecordType1;
    private static RecordType lastRecordType;

    private static Map<String, FieldType> fields = Maps.newHashMap();
    private Map<String, Integer> matchResultCounts = Maps.newHashMap();

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        SOLR_TEST_UTIL = new SolrTestingUtility();
        SOLR_TEST_UTIL.setSchemaData(IOUtils.toByteArray(DerefMapIndexTest.class.getResourceAsStream("schema1.xml")));

        TestHelper.setupLogging("org.lilyproject.indexer", "org.lilyproject.linkindex",
                "org.lilyproject.rowlog.impl.RowLogImpl");

        SOLR_TEST_UTIL.start();

        repoSetup.setupCore();
        repoSetup.setupRepository(true);
        repoSetup.setupMessageQueue(false, true);

        repository = repoSetup.getRepository();
        typeManager = repoSetup.getTypeManager();
        idGenerator = repository.getIdGenerator();

        IndexManager indexManager = new IndexManager(repoSetup.getHadoopConf());

        linkIndex = new LinkIndex(indexManager, repository);

        // Field types should exist before the indexer conf is loaded
        setupSchema();

        RowLogConfigurationManager rowLogConfMgr = repoSetup.getRowLogConfManager();
        rowLogConfMgr.addSubscription("WAL", "LinkIndexUpdater", RowLogSubscription.Type.VM, 1);
        rowLogConfMgr.addSubscription("WAL", "MessageVerifier", RowLogSubscription.Type.VM, 2);

        repoSetup.waitForSubscription(repoSetup.getWal(), "LinkIndexUpdater");
        repoSetup.waitForSubscription(repoSetup.getWal(), "MessageVerifier");

        rowLogConfMgr.addSubscription("MQ", "IndexUpdater", RowLogSubscription.Type.VM, 1);

        repoSetup.waitForSubscription(repoSetup.getMq(), "IndexUpdater");

        solrShardManager = SolrShardManagerImpl.createForOneShard(SOLR_TEST_UTIL.getUri());

        RowLogMessageListenerMapping.INSTANCE.put("LinkIndexUpdater", new LinkIndexUpdater(repository, linkIndex));
        RowLogMessageListenerMapping.INSTANCE.put("MessageVerifier", messageVerifier);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        repoSetup.stop();

        if (SOLR_TEST_UTIL != null)
            SOLR_TEST_UTIL.stop();
    }

    public static void changeIndexUpdater(String confName) throws Exception {
        INDEXER_CONF = IndexerConfBuilder.build(DerefMapIndexTest.class.getResourceAsStream(confName), repository);
        IndexLocker indexLocker = new IndexLocker(repoSetup.getZk(), true);
        derefMap = DerefMapHbaseImpl.create("test", repoSetup.getHadoopConf(), repository.getIdGenerator());
        Indexer indexer = new Indexer("test", INDEXER_CONF, repository, solrShardManager, indexLocker,
                new IndexerMetrics("test"), derefMap);

        RowLogMessageListenerMapping.INSTANCE.put("IndexUpdater", new IndexUpdater(indexer, repository, linkIndex,
                indexLocker, repoSetup.getMq(), new IndexUpdaterMetrics("test"), derefMap));
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
        nvRecordType1.addFieldTypeEntry(nvfield1.getId(), false);
        nvRecordType1.addFieldTypeEntry(nvfield2.getId(), false);
        nvRecordType1.addFieldTypeEntry(liveTag.getId(), false);
        nvRecordType1.addFieldTypeEntry(latestTag.getId(), false);
        nvRecordType1.addFieldTypeEntry(previewTag.getId(), false);
        nvRecordType1.addFieldTypeEntry(nvLinkField1.getId(), false);
        nvRecordType1.addFieldTypeEntry(nvLinkField2.getId(), false);
        nvRecordType1 = typeManager.createRecordType(nvRecordType1);

        //
        // Schema types for the versioned test
        //
        QName vfield1Name = new QName(NS2, "v_field1");
        vfield1 = typeManager.newFieldType(stringValueType, vfield1Name, Scope.VERSIONED);
        vfield1 = typeManager.createFieldType(vfield1);

        QName vlinkField1Name = new QName(NS2, "v_linkfield1");
        vLinkField1 = typeManager.newFieldType(linkValueType, vlinkField1Name, Scope.VERSIONED);
        vLinkField1 = typeManager.createFieldType(vLinkField1);

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
        vRecordType1.addFieldTypeEntry(vfield1.getId(), false);
        vRecordType1.addFieldTypeEntry(liveTag.getId(), false);
        vRecordType1.addFieldTypeEntry(latestTag.getId(), false);
        vRecordType1.addFieldTypeEntry(previewTag.getId(), false);
        vRecordType1.addFieldTypeEntry(vLinkField1.getId(), false);
        vRecordType1.addFieldTypeEntry(nvLinkField2.getId(), false);
        vRecordType1.addFieldTypeEntry(vStringMvField.getId(), false);
        vRecordType1.addFieldTypeEntry(vLongField.getId(), false);
        vRecordType1.addFieldTypeEntry(vBlobField.getId(), false);
        vRecordType1.addFieldTypeEntry(vBlobMvHierField.getId(), false);
        vRecordType1.addFieldTypeEntry(vBlobNestedField.getId(), false);
        vRecordType1.addFieldTypeEntry(vDateTimeField.getId(), false);
        vRecordType1.addFieldTypeEntry(vDateField.getId(), false);
        vRecordType1.addFieldTypeEntry(vIntHierField.getId(), false);
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

        // versioned fields
        RecordType matchNs1AlphaRecordType = typeManager.newRecordType(new QName(NS, "Alpha"));
        RecordType matchNs1BetaRecordType = typeManager.newRecordType(new QName(NS, "Beta"));
        RecordType matchNs2AlphaRecordType = typeManager.newRecordType(new QName(NS2, "Alpha"));
        RecordType matchNs2BetaRecordType = typeManager.newRecordType(new QName(NS2, "Beta"));
        for (int i = 1; i <= 8; i++) {
            FieldType ns1Field = typeManager
                    .createFieldType(typeManager.newFieldType("STRING", new QName(NS, "match" + i), Scope.VERSIONED));
            fields.put("ns:match" + i, ns1Field);

            matchNs1AlphaRecordType.addFieldTypeEntry(field("ns:match" + i).getId(), false);
            matchNs1BetaRecordType.addFieldTypeEntry(field("ns:match" + i).getId(), false);
            matchNs2AlphaRecordType.addFieldTypeEntry(field("ns:match" + i).getId(), false);
            matchNs2BetaRecordType.addFieldTypeEntry(field("ns:match" + i).getId(), false);

        }

        // non-versioned fields
        for (int i = 1; i <= 8; i++) {
            FieldType ns1Field = typeManager.createFieldType(
                    typeManager.newFieldType("STRING", new QName(NS, "nvmatch" + i), Scope.NON_VERSIONED));
            fields.put("ns:nvmatch" + i, ns1Field);

            matchNs1AlphaRecordType.addFieldTypeEntry(field("ns:nvmatch" + i).getId(), false);
            matchNs1BetaRecordType.addFieldTypeEntry(field("ns:nvmatch" + i).getId(), false);
            matchNs2AlphaRecordType.addFieldTypeEntry(field("ns:nvmatch" + i).getId(), false);
            matchNs2BetaRecordType.addFieldTypeEntry(field("ns:nvmatch" + i).getId(), false);

        }

        typeManager.createRecordType(matchNs1AlphaRecordType);
        typeManager.createRecordType(matchNs1BetaRecordType);
        typeManager.createRecordType(matchNs2AlphaRecordType);
        typeManager.createRecordType(matchNs2BetaRecordType);

    }

    private static FieldType field(String name) {
        return fields.get(name);
    }


    private void commitIndex() throws Exception {
        repoSetup.processMQ();
        solrShardManager.commit(true, true);
    }

    private QueryResponse getQueryResponse(String query) throws SolrClientException, InterruptedException {
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.set("q", query);
        solrQuery.set("rows", 5000);
        QueryResponse response = solrShardManager.query(solrQuery);
        return response;
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
        assertEquals(count, response.getResults().getNumFound());
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

        Assert.assertArrayEquals((Object[]) expectedValues,
                response.getResults().get(0).getFieldValues(fieldName).toArray(new Object[]{}));
    }

    private void expectEvent(RecordEvent.Type type, RecordId recordId, SchemaId... updatedFields) {
        expectEvent(type, recordId, null, null, updatedFields);
    }

    private void expectEvent(RecordEvent.Type type, RecordId recordId, Long versionCreated, Long versionUpdated,
                             SchemaId... updatedFields) {
        expectEvent(type, recordId, versionCreated, versionUpdated, false, updatedFields);
    }

    private void expectEvent(RecordEvent.Type type, RecordId recordId, Long versionCreated, Long versionUpdated,
                             boolean recordTypeChanged, SchemaId... updatedFields) {
        RecordEvent event = new RecordEvent();

        event.setType(type);

        for (SchemaId updatedField : updatedFields) {
            event.addUpdatedField(updatedField);
        }

        if (versionCreated != null)
            event.setVersionCreated(versionCreated);

        if (versionUpdated != null)
            event.setVersionUpdated(versionUpdated);

        if (recordTypeChanged)
            event.setRecordTypeChanged(recordTypeChanged);

        messageVerifier.setExpectedEvent(recordId, event);
    }

    private static class MessageVerifier implements RowLogMessageListener {
        private RecordId expectedId;
        private RecordEvent expectedEvent;
        private int failures = 0;
        private boolean enabled;

        public int getFailures() {
            return failures;
        }

        public void init() {
            this.enabled = true;
            this.expectedId = null;
            this.expectedEvent = null;
            this.failures = 0;
        }

        public void disable() {
            this.enabled = false;
        }

        public void setExpectedEvent(RecordId recordId, RecordEvent recordEvent) {
            this.expectedId = recordId;
            this.expectedEvent = recordEvent;
        }

        @Override
        public boolean processMessage(RowLogMessage message) {
            if (!enabled)
                return true;

            // In case of failures we print out "load" messages, the main junit thread is expected to
            // test that the failures variable is 0.

            RecordId recordId = repository.getIdGenerator().fromBytes(message.getRowKey());
            try {
                RecordEvent event = new RecordEvent(message.getPayload(), idGenerator);
                if (expectedEvent == null) {
                    failures++;
                    printSomethingLoad();
                    System.err.println("Did not expect a message, but got:");
                    System.err.println(recordId);
                    System.err.println(event.toJson());
                } else {
                    if (!event.equals(expectedEvent) ||
                            !(recordId.equals(expectedId) ||
                                    (expectedId == null && expectedEvent.getType() == CREATE))) {
                        failures++;
                        printSomethingLoad();
                        System.err.println("Expected message:");
                        System.err.println(expectedId);
                        System.err.println(expectedEvent.toJson());
                        System.err.println("Received message:");
                        System.err.println(recordId);
                        System.err.println(event.toJson());
                    } else {
                        log.debug("Received message ok.");
                    }
                }
            } catch (RowLogException e) {
                failures++;
                e.printStackTrace();
            } catch (IOException e) {
                failures++;
                e.printStackTrace();
            } finally {
                expectedId = null;
                expectedEvent = null;
            }
            return true;
        }

        private void printSomethingLoad() {
            for (int i = 0; i < 10; i++) {
                System.err.println("!!");
            }
        }
    }

    @Test
    public void testDerefMap() throws Exception {
        // <field value="n:nvlink"/>
        // deref-00-1
        //   n:nvstring1 = blub
        //   n:nvlink1 = deref-00-2
        // deref-00-2
        //   ...
        // DerefMap:
        // record | dependency | variants | field
        // (empty)
        // 00-1 points to 00-2, but doesn't use any fields of 00-2
        repository.recordBuilder().id("deref-00-1")
                .recordType(new QName(NS, "NVRecordType1"))
                .field(nvfield1.getName(), "blub1")
                .field(nvLinkField1.getName(), new Link(id("deref-00-2")))
                .create();
        assertEquals(dependencySet(id("deref-00-2"), nvfield2.getId()), idSet());

        // <field value="n:nvlink=>n:nvstring1"/>
        // 02-1
        //   n:nvstring1 = blub1
        //   n:nvlink1 = 02-2
        // 02-2
        //   ...
        // DerefMap:
        // record | dependency | variants | field
        // R1     | R2         |          | n:nvstring1
        // 02-2 does not have n:nvstring1, but it should be in the index anyway, in case that field is added
        repository.recordBuilder().id("deref-02-1")
                .recordType(new QName(NS, "NVRecordType1"))
                .field(nvfield1.getName(), "blub1")
                .field(nvLinkField1.getName(), new Link(id("deref-02-2")))
                .create();
        assertEquals(dependencySet(id("deref-02-2"), nvfield1.getId()), idSet(id("deref-02-1")));
        assertEquals(dependencySet(id("deref-02-2"), nvfield2.getId()), idSet()); // we're not interested in nvfield2

        // <field value="+prop1=>n:nvlink1=>+prop2=>n:nvlink2"/>
        // 03-1
        //   ...
        // 03-1.prop1=x                                  A.prop1=y (create, has n:nvlink1)
        //   n:nvlink1 = 03-2
        // 03-2.prop2=y
        //   n:nvlink2 = 03-3
        // DerefMap:
        // record | dependency   | variants | field
        // 03-1   | 03-1         | prop1    | n:nvlink1
        // 03-1   | 03-1.prop1=x |          | n:nvlink1 # not needed because of previous line
        // 03-1   | 03-2.prop2=y | prop2    | n:nvlink2
        // 03-1   | 03-2.prop2=y |          | n:nvlink2 # not needed because of previous line
        repository.recordBuilder().id("deref-03-1", Collections.singletonMap("prop1", "x"))
                .recordType(new QName(NS, "NVRecordType1"))
                .field(nvLinkField1.getName(), idGenerator.newRecordId("deref-03-2"))
                .create();
        repository.recordBuilder().id("deref-03-2", Collections.singletonMap("prop2", "y"))
                .recordType(new QName(NS, "NVRecordType1"))
                .field(nvLinkField1.getName(), idGenerator.newRecordId("deref-03-3"))
                .create();
        assertEquals(dependencySet(id("deref-03-2", "prop2", "y"), nvLinkField2.getId()), idSet(id("03-1")));

        // Expression: +prop1,+prop2=x=>n:nvlink1=>+prop3=>n:nvlink2
        // 04-1
        //   ...
        // 04-1.prop1=x
        //   n:nvlink1 = 04-2
        // 04-2
        //   ...
        // 04-2.prop2=y
        //   n:nvlink2 = 04-3
        // DerefMap:
        // record | dependency   | variants | field
        // 04-1   | 04-1.prop2=x | prop1    | n:nvlink1
        // 04-1   | 04-2         | prop2    | n:nvlink2

        // <forEach match="+prop1">
        //   <forEach matbarch="+prop2">
        //     <field value="n:nvstring1"/>
        // 05-1
        //   ...
        // 05-1.prop1=x,prop2=z
        //    n:nvstring1=ppp
        //   ...
        // 05-2.prop2=y
        //   n:nvlink2 = 05-3
        // DerefMap:
        // record | dependency | variants | field
        // 05-1     | 05-1         | prop1    | n:nvlink1
        // 05-1     | 05-1.prop1=x | prop2    | n:nvlink2
        // 05-1     | 05-2.prop2=y |          | n:nvlink2


        //
    }

    private RecordId id(String id, String... props) {
        if (props != null && props.length % 2 == 1) {
            throw new IllegalArgumentException("Bad number of arguments");
        }
        Map<String, String> variantProps = Maps.newHashMap();
        for (int i = 0; i < props.length; i += 2) {
            variantProps.put(props[i], props[i + 1]);
        }

        return idGenerator.newRecordId(id, variantProps);
    }

    private Set<RecordId> dependencySet(RecordId id, SchemaId fieldId) throws Exception {
        DependantRecordIdsIterator reverse = derefMap.findDependantsOf(id, fieldId);
        Set<RecordId> result = Sets.newHashSet();
        while (reverse.hasNext()) {
            result.add(reverse.next());
        }
        return result;
    }

    private Set<RecordId> idSet(RecordId... ids) throws Exception {
        return Sets.newHashSet(ids);

    }

}
