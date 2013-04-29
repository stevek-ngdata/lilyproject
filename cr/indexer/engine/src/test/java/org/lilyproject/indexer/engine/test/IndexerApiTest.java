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
package org.lilyproject.indexer.engine.test;

import com.google.common.collect.Sets;
import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.lilyproject.hadooptestfw.TestHelper;
import org.lilyproject.indexer.IndexerException;
import org.lilyproject.indexer.engine.ClassicSolrShardManager;
import org.lilyproject.indexer.engine.IndexLocker;
import org.lilyproject.indexer.engine.Indexer;
import org.lilyproject.indexer.engine.IndexerApiImpl;
import org.lilyproject.indexer.engine.IndexerMetrics;
import org.lilyproject.indexer.engine.IndexerRegistry;
import org.lilyproject.indexer.engine.SolrClientException;
import org.lilyproject.indexer.model.indexerconf.IndexerConf;
import org.lilyproject.indexer.model.indexerconf.IndexerConfBuilder;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.RepositoryManager;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repotestfw.RepositorySetup;
import org.lilyproject.solrtestfw.SolrDefinition;
import org.lilyproject.solrtestfw.SolrTestingUtility;
import org.lilyproject.util.hbase.LilyHBaseSchema.Table;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class IndexerApiTest {

    private final static RepositorySetup repoSetup = new RepositorySetup();

    private static final String NS = "org.lilyproject.indexer.test";

    private IndexerConf INDEXER_CONF;
    private SolrTestingUtility SOLR_TEST_UTIL;
    private Repository repository;
    private RepositoryManager repositoryManager;
    private TypeManager typeManager;
    private ClassicSolrShardManager solrShardManager;

    private final IndexerRegistry indexerRegistry = new IndexerRegistry();
    private IndexerApiImpl indexerApi;

    private RecordType matchingRecordType;
    private RecordType otherRecordType;

    @Before
    public void setUp() throws Exception {
        SOLR_TEST_UTIL = new SolrTestingUtility();
        SOLR_TEST_UTIL.setSolrDefinition(
                new SolrDefinition(IOUtils.toByteArray(IndexerTest.class.getResourceAsStream("schema1.xml"))));

        TestHelper.setupLogging("org.lilyproject.indexer");

        SOLR_TEST_UTIL.start();

        repoSetup.setupCore();
        repoSetup.setupRepository();

        repositoryManager = repoSetup.getRepositoryManager();
        repository = (Repository)repositoryManager.getDefaultTable();
        typeManager = repoSetup.getTypeManager();

        setupSchema();

        solrShardManager = ClassicSolrShardManager.createForOneShard(SOLR_TEST_UTIL.getUri());

        indexerApi = new IndexerApiImpl(repositoryManager, indexerRegistry);
    }

    @After
    public void tearDown() throws Exception {
        repoSetup.stop();

        if (SOLR_TEST_UTIL != null) {
            SOLR_TEST_UTIL.stop();
        }
    }

    public void changeIndexUpdater(String confName) throws Exception {
        INDEXER_CONF = IndexerConfBuilder.build(IndexerTest.class.getResourceAsStream(confName),
                (Repository)repoSetup.getRepositoryManager().getPublicRepository());
        IndexLocker indexLocker = new IndexLocker(repoSetup.getZk(), false);
        Indexer indexer =
                new Indexer("test", INDEXER_CONF, repositoryManager, solrShardManager, indexLocker, new IndexerMetrics("test"),
                        null);
        indexerRegistry.register(indexer);
    }

    @Test
    public void explicitIndexing() throws Exception {
        changeIndexUpdater("indexerconf_synchronous.xml");

        final Record record = repository.recordBuilder()
                .id(repository.getIdGenerator().newRecordId("index-explicitly"))
                .recordType(matchingRecordType.getName())
                .field(new QName(NS, "nv_field1"), "value")
                .create();

        // nothing will be indexed yet, because there are no index updaters running
        commitIndex();
        verifyResultCount("nv_field1:value", 0);

        indexerApi.index(Table.RECORD.name, record.getId());

        // now we triggered indexing
        commitIndex();
        verifyResultCount("nv_field1:value", 1);
    }

    @Test
    public void explicitIndexingWrongRecordType() throws Exception {
        changeIndexUpdater("indexerconf_synchronous.xml");

        final Record record = repository.recordBuilder()
                .id(repository.getIdGenerator().newRecordId("wrong-type"))
                .recordType(otherRecordType.getName())
                .field(new QName(NS, "nv_field1"), "value")
                .create();

        // nothing will be indexed yet, because there are no index updaters running
        commitIndex();
        verifyResultCount("nv_field1:value", 0);

        indexerApi.index(Table.RECORD.name, record.getId());

        // still nothing will be indexed, because the record type doesn't match
        commitIndex();
        verifyResultCount("nv_field1:value", 0);
    }

    @Test(expected = IndexerException.class)
    public void explicitIndexingWrongIndex() throws Exception {
        changeIndexUpdater("indexerconf_synchronous.xml");

        final Record record = repository.recordBuilder()
                .id(repository.getIdGenerator().newRecordId("wrong-index"))
                .recordType(matchingRecordType.getName())
                .field(new QName(NS, "nv_field1"), "value")
                .create();

        // nothing will be indexed yet, because there are no index updaters running
        commitIndex();
        verifyResultCount("nv_field1:value", 0);

        indexerApi.indexOn(Table.RECORD.name, record.getId(), Sets.newHashSet("this-index-does-not-exist"));
    }

    private void setupSchema() throws RepositoryException, InterruptedException {
        QName fieldName = new QName(NS, "nv_field1");
        FieldType field1 = typeManager.newFieldType(typeManager.getValueType("STRING"), fieldName, Scope.NON_VERSIONED);
        field1 = typeManager.createFieldType(field1);

        matchingRecordType = typeManager.newRecordType(new QName(NS, "RecordType1"));
        matchingRecordType.addFieldTypeEntry(field1.getId(), true);
        matchingRecordType = typeManager.createRecordType(matchingRecordType);

        otherRecordType = typeManager.newRecordType(new QName(NS, "OtherRecordType"));
        otherRecordType.addFieldTypeEntry(field1.getId(), true);
        otherRecordType = typeManager.createRecordType(otherRecordType);
    }

    private void commitIndex() throws Exception {
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

}
