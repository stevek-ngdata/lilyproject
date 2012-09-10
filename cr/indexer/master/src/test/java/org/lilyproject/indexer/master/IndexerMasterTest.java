package org.lilyproject.indexer.master;

import java.io.IOException;

import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilyproject.client.LilyClient;
import org.lilyproject.indexer.derefmap.DerefMapHbaseImpl;
import org.lilyproject.indexer.model.api.IndexConcurrentModificationException;
import org.lilyproject.indexer.model.api.IndexDefinition;
import org.lilyproject.indexer.model.api.IndexModelException;
import org.lilyproject.indexer.model.api.IndexNotFoundException;
import org.lilyproject.indexer.model.api.IndexUpdateException;
import org.lilyproject.indexer.model.api.IndexValidityException;
import org.lilyproject.indexer.model.impl.IndexerModelImpl;
import org.lilyproject.lilyservertestfw.LilyProxy;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.util.hbase.HBaseAdminFactory;
import org.lilyproject.util.zookeeper.StateWatchingZooKeeper;
import org.lilyproject.util.zookeeper.ZkLockException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IndexerMasterTest {
    private static final QName BOOK_RECORD_TYPE = new QName("org.lilyproject.test", "Book");
    private static final QName AUTHOR_RECORD_TYPE = new QName("org.lilyproject.test", "Author");
    private static final QName BOOK_TO_AUTHOR_LINK = new QName("org.lilyproject.test", "authorLink");
    private static final QName NAME = new QName("org.lilyproject.test", "name");

    private static LilyProxy lilyProxy;
    private static LilyClient lilyClient;
    private Repository repository;
    private HBaseAdmin hBaseAdmin;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        lilyProxy = new LilyProxy();
        lilyProxy.start();
        lilyClient = lilyProxy.getLilyServerProxy().getClient();
    }

    @Before
    public void setUp() throws ZooKeeperConnectionException, MasterNotRunningException {
        repository = lilyClient.getRepository();
        hBaseAdmin = HBaseAdminFactory.get(lilyProxy.getHBaseProxy().getConf());
    }

    @After
    public void tearDown() throws IOException {
        hBaseAdmin.close();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        lilyProxy.stop();
    }

    @Test
    public void testDisableAndEnableIndexerDereferenceMap() throws Exception {
        createSchema();
        final String indexName = addIndex();

        waitUntilIndexUpdateProcessed();
        assertTrue(verifyExistenceOfDerefMap(indexName));

        setDerefMapEnabled(indexName, false);
        waitUntilIndexUpdateProcessed();
        assertFalse(verifyExistenceOfDerefMap(indexName));

        setDerefMapEnabled(indexName, true);
        waitUntilIndexUpdateProcessed();
        assertTrue(verifyExistenceOfDerefMap(indexName));
    }

    private void waitUntilIndexUpdateProcessed() throws InterruptedException {
        // TODO: is there a more clever way to do this?
        Thread.sleep(2500);
    }

    private boolean verifyExistenceOfDerefMap(String indexName) throws IOException {
        return checkHTableExistence(DerefMapHbaseImpl.backwardIndexName(indexName), hBaseAdmin) &&
                checkHTableExistence(DerefMapHbaseImpl.forwardIndexName(indexName), hBaseAdmin);
    }

    private void setDerefMapEnabled(String indexName, boolean enabled)
            throws IOException, InterruptedException, KeeperException, ZkLockException, IndexNotFoundException,
            IndexModelException, IndexConcurrentModificationException, IndexUpdateException, IndexValidityException {
        // TODO: ok to use localhost hardcoded here?
        final StateWatchingZooKeeper zk = new StateWatchingZooKeeper("localhost:2181", 10000);

        final IndexerModelImpl model = new IndexerModelImpl(zk);

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

    private void createSchema() throws RepositoryException, InterruptedException {
        TypeManager typeManager = repository.getTypeManager();

        FieldType linkFieldType = typeManager.createFieldType(typeManager.newFieldType(typeManager.getValueType("LINK"),
                BOOK_TO_AUTHOR_LINK, Scope.NON_VERSIONED));
        FieldType nameFieldType =
                typeManager.createFieldType(typeManager.newFieldType(typeManager.getValueType("STRING"),
                        NAME, Scope.NON_VERSIONED));

        RecordType bookRecordType = typeManager.newRecordType(BOOK_RECORD_TYPE);
        bookRecordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(linkFieldType.getId(), false));
        typeManager.createRecordType(bookRecordType);

        final RecordType authorRecordType = typeManager.newRecordType(AUTHOR_RECORD_TYPE);
        authorRecordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(nameFieldType.getId(), false));
        typeManager.createRecordType(authorRecordType);
    }

    private String addIndex() throws Exception {
        final String indexName = "books";
        lilyProxy.getLilyServerProxy()
                .addIndexFromResource(indexName, "org/lilyproject/indexer/master/test_indexer_conf.xml", 60000L);
        return indexName;
    }

}
