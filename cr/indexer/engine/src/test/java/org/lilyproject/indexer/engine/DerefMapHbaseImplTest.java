package org.lilyproject.indexer.engine;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.HashMultimap;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilyproject.hadooptestfw.TestHelper;
import org.lilyproject.hbaseindex.IndexManager;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repotestfw.RepositorySetup;
import org.lilyproject.util.io.Closer;

import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Jan Van Besien
 */
public class DerefMapHbaseImplTest {
    private final static RepositorySetup repoSetup = new RepositorySetup();

    private static Repository repository;
    private static IdGenerator ids;
    private static DerefMapHbaseImpl derefMap;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging("org.lilyproject.indexer", "org.lilyproject.rowlog.impl.RowLogImpl");

        repoSetup.setupCore();
        repoSetup.setupRepository(true);

        repository = repoSetup.getRepository();
        ids = repository.getIdGenerator();

        IndexManager indexManager = new IndexManager(repoSetup.getHadoopConf());

        derefMap = new DerefMapHbaseImpl("test", indexManager, ids);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        Closer.close(repoSetup);
    }

    // TODO: this tests the deref map directly but we might also need a test which checks if it gets updated correctly due
    // to normal indexer behavior

    @Test
    public void emptyDependencies() throws Exception {
        SchemaId dummyVtag = ids.getSchemaId(UUID.randomUUID());
        SchemaId dummyField = ids.getSchemaId(UUID.randomUUID());
        final RecordId id1 = ids.newRecordId("id1");

        final HashMultimap<DerefMap.Entry, SchemaId> empty = HashMultimap.create();
        derefMap.updateDependencies(id1, dummyVtag, empty);

        final Set<DerefMap.Entry> found = derefMap.findDependencies(id1, dummyVtag);
        assertEquals(empty.keySet(), found);

        final DerefMap.DependantRecordIdsIterator dependants =
                derefMap.findDependantsOf(new DerefMap.Entry(id1, dummyVtag), dummyField);
        assertFalse(dependants.hasNext());
    }

    @Test
    public void oneDependency() throws Exception {
        SchemaId dummyVtag = ids.getSchemaId(UUID.randomUUID());
        SchemaId dependingField = ids.getSchemaId(UUID.randomUUID());
        SchemaId anotherField = ids.getSchemaId(UUID.randomUUID());
        final RecordId dependant = ids.newRecordId("dependant");
        final RecordId depending = ids.newRecordId("depending");
        final RecordId dependingAfterUpdate = ids.newRecordId("dependingAfterUpdate");

        // the dependant depends on the dependingField of the depending
        final HashMultimap<DerefMap.Entry, SchemaId> dependencies = HashMultimap.create();
        dependencies.put(new DerefMap.Entry(depending, dummyVtag), dependingField);
        derefMap.updateDependencies(dependant, dummyVtag, dependencies);

        final Set<DerefMap.Entry> found = derefMap.findDependencies(dependant, dummyVtag);
        assertEquals(dependencies.keySet(), found);

        // check that the dependant is found as only dependency of the depending via the dependingField
        final DerefMap.DependantRecordIdsIterator dependants =
                derefMap.findDependantsOf(new DerefMap.Entry(depending, dummyVtag), dependingField);
        assertTrue(dependants.hasNext());
        assertEquals(dependant, dependants.next());
        assertFalse(dependants.hasNext());

        // check that nothing is found as depending on the dependant
        assertFalse(derefMap.findDependantsOf(new DerefMap.Entry(dependant, dummyVtag), dependingField).hasNext());

        // check that nothing is found as depending on the depending via another field than the dependingField
        assertFalse(derefMap.findDependantsOf(new DerefMap.Entry(dependant, dummyVtag), anotherField).hasNext());

        // now update the dependency to be from the dependant to the dependingAfterUpdate (via the same field)
        final HashMultimap<DerefMap.Entry, SchemaId> updatedDependencies = HashMultimap.create();
        updatedDependencies.put(new DerefMap.Entry(dependingAfterUpdate, dummyVtag), dependingField);
        derefMap.updateDependencies(dependant, dummyVtag, updatedDependencies);

        final Set<DerefMap.Entry> foundAfterUpdate = derefMap.findDependencies(dependant, dummyVtag);
        assertEquals(updatedDependencies.keySet(), foundAfterUpdate);

        // check that the dependant is found as only dependency of the dependingAfterUpdate via the dependingField
        final DerefMap.DependantRecordIdsIterator dependantsAfterUpdate =
                derefMap.findDependantsOf(new DerefMap.Entry(dependingAfterUpdate, dummyVtag), dependingField);
        assertTrue(dependantsAfterUpdate.hasNext());
        assertEquals(dependant, dependantsAfterUpdate.next());
        assertFalse(dependantsAfterUpdate.hasNext());

        // check that nothing is found any longer as depending on the previous depending (from before the update)
        assertFalse(derefMap.findDependantsOf(new DerefMap.Entry(depending, dummyVtag), dependingField).hasNext());
    }

    // TODO: test multiple dependencies, various updates, ...

    @Test
    public void serializeFields() throws Exception {
        final Set<SchemaId> fields = new HashSet<SchemaId>();
        fields.add(ids.getSchemaId(UUID.randomUUID()));
        fields.add(ids.getSchemaId(UUID.randomUUID()));

        final Set<SchemaId> deserialized = derefMap.deserializeFields(derefMap.serializeFields(fields));

        assertEquals(fields, deserialized);
    }

    @Test
    public void serializeEntries() throws Exception {
        final Set<DerefMap.Entry> entries = new HashSet<DerefMap.Entry>();
        entries.add(new DerefMap.Entry(ids.newRecordId("id1"), ids.getSchemaId(UUID.randomUUID())));
        entries.add(new DerefMap.Entry(ids.newRecordId("id2"), ids.getSchemaId(UUID.randomUUID())));

        final Set<DerefMap.Entry> deserialized = derefMap.deserializeEntries(derefMap.serializeEntries(entries));

        assertEquals(entries, deserialized);
    }

}
