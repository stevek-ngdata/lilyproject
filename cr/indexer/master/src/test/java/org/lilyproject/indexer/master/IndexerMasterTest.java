package org.lilyproject.indexer.master;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Collections;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilyproject.indexer.derefmap.DerefMapHbaseImpl;
import org.lilyproject.indexer.model.api.IndexConcurrentModificationException;
import org.lilyproject.indexer.model.api.IndexDefinition;
import org.lilyproject.indexer.model.api.IndexModelException;
import org.lilyproject.indexer.model.api.IndexNotFoundException;
import org.lilyproject.indexer.model.api.IndexUpdateException;
import org.lilyproject.indexer.model.api.IndexValidityException;
import org.lilyproject.indexer.model.impl.IndexerModelImpl;
import org.lilyproject.indexer.model.util.IndexesInfo;
import org.lilyproject.indexer.model.util.IndexesInfoImpl;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repotestfw.RepositorySetup;
import org.lilyproject.util.hbase.HBaseAdminFactory;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.zookeeper.ZkConnectException;
import org.lilyproject.util.zookeeper.ZkLockException;

public class IndexerMasterTest {
    private static final QName BOOK_RECORD_TYPE = new QName("org.lilyproject.test", "Book");
    private static final QName AUTHOR_RECORD_TYPE = new QName("org.lilyproject.test", "Author");
    private static final QName BOOK_TO_AUTHOR_LINK = new QName("org.lilyproject.test", "authorLink");
    private static final QName NAME = new QName("org.lilyproject.test", "name");

    private final static RepositorySetup repoSetup = new RepositorySetup();
    private Repository repository;
    private HBaseAdmin hBaseAdmin;
    private IndexesInfo indexesInfo;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        repoSetup.setupCore();
        repoSetup.setupRepository(true);
    }

    @Before
    public void setUp() throws Exception {
        repository = repoSetup.getRepository();
        hBaseAdmin = HBaseAdminFactory.get(repoSetup.getHadoopConf());

        final IndexerModelImpl model = new IndexerModelImpl(repoSetup.getZk());
        indexesInfo = new IndexesInfoImpl(model, repository);
    }

    @After
    public void tearDown() throws IOException {
        repository.close();
        hBaseAdmin.close();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        Closer.close(repoSetup);
    }

    @Test
    public void testDisableAndEnableIndexerDereferenceMap() throws Exception {
        createSchema();
        final String indexName = addIndex();

        verifyExistenceOfDerefMap(indexName, true);

        setDerefMapEnabled(indexName, false);
        verifyExistenceOfDerefMap(indexName, false);

        setDerefMapEnabled(indexName, true);
        verifyExistenceOfDerefMap(indexName, true);
    }

    private boolean verifyExistenceOfDerefMap(String indexName, boolean shouldExist)
            throws IOException, InterruptedException {
        final int MAX_TRIES = 100;
        final int SLEEP_BETWEEN_TRIES = 500;

        for (int tries = 0; tries < MAX_TRIES; tries++) {
            boolean tablesExist = checkHTableExistence(DerefMapHbaseImpl.backwardIndexName(indexName), hBaseAdmin) &&
                    checkHTableExistence(DerefMapHbaseImpl.forwardIndexName(indexName), hBaseAdmin);
            if (tablesExist == shouldExist)
                return true; // tables existence as expected
            else
                Thread.sleep(SLEEP_BETWEEN_TRIES);
        }

        return false; // condition not met after trying long enough
    }

    private void setDerefMapEnabled(String indexName, boolean enabled)
            throws IOException, InterruptedException, KeeperException, ZkLockException, IndexNotFoundException,
            IndexModelException, IndexConcurrentModificationException, IndexUpdateException, IndexValidityException,
            ZkConnectException {

        final IndexerModelImpl model = new IndexerModelImpl(repoSetup.getZk());

        final String lock = model.lockIndex(indexName);
        try {
            IndexDefinition index = model.getMutableIndex(indexName);
            index.setEnableDerefMap(enabled);
            model.updateIndex(index, lock);
        } finally {
            model.unlockIndex(lock, false);
        }
    }

    private boolean checkHTableExistence(String tableName, HBaseAdmin hBaseAdmin) throws IOException {
        try {
            hBaseAdmin.getTableDescriptor(Bytes.toBytes(tableName));
            return true;
        } catch (TableNotFoundException e) {
            return false;
        }
    }

    /**
     * Creates a simple schema with two record types and a link between them.
     *
     * @throws RepositoryException
     * @throws InterruptedException
     */
    private void createSchema() throws RepositoryException, InterruptedException {
        TypeManager typeManager = repository.getTypeManager();

        FieldType linkFieldType = typeManager.createFieldType(typeManager.newFieldType(typeManager.getValueType("LINK"),
                BOOK_TO_AUTHOR_LINK, Scope.NON_VERSIONED));
        FieldType nameFieldType =
                typeManager.createFieldType(typeManager.newFieldType(typeManager.getValueType("STRING"),
                        NAME, Scope.NON_VERSIONED));

        final RecordType bookRecordType =
                typeManager.recordTypeBuilder().name(BOOK_RECORD_TYPE).field(linkFieldType.getId(), false).build();
        typeManager.createRecordType(bookRecordType);

        final RecordType authorRecordType =
                typeManager.recordTypeBuilder().name(AUTHOR_RECORD_TYPE).field(nameFieldType.getId(), false).build();
        typeManager.createRecordType(authorRecordType);
    }

    /**
     * Creates a simple index for the schema of this test. The index uses a dereference expression on the link field in
     * the schema.
     *
     * @return the name of the index
     * @throws Exception
     */
    private String addIndex() throws Exception {
        final String indexName = "books";

        final IndexerModelImpl model = new IndexerModelImpl(repoSetup.getZk());

        final IndexDefinition indexDef = model.newIndex(indexName);
        indexDef.setConfiguration(
                IOUtils.toByteArray(IndexerMasterTest.class.getResourceAsStream("test_indexer_conf.xml")));
        indexDef.setSolrShards(Collections.singletonMap("shard1", "http://somewhere/"));
        model.addIndex(indexDef);
        waitForIndexesInfoUpdate(1);

        return indexName;
    }

    protected void waitForIndexesInfoUpdate(int expectedCount) throws InterruptedException {
        // IndexesInfo will be updated asynchronously: wait for that to happen
        long now = System.currentTimeMillis();
        while (indexesInfo.getIndexInfos().size() != expectedCount) {
            if (System.currentTimeMillis() - now > 10000) {
                fail("IndexesInfo was not updated within the expected timeout.");
            }
            Thread.sleep(20);
        }
    }

}
