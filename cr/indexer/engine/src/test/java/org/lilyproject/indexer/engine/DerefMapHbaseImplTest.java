package org.lilyproject.indexer.engine;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.lilyproject.indexer.model.indexerconf.DerefMap;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import junit.framework.Assert;
import org.apache.hadoop.thirdparty.guava.common.collect.ImmutableSet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilyproject.hadooptestfw.TestHelper;
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

        derefMap = (DerefMapHbaseImpl) DerefMapHbaseImpl.create("test", repoSetup.getHadoopConf(), ids);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        Closer.close(repoSetup);
    }

    // TODO: this tests the deref map directly but we might also need a test which checks if it gets updated correctly due
    // to normal indexer behavior

    @Test
    public void emptyDependencies() throws Exception {
        final SchemaId dummyVtag = ids.getSchemaId(UUID.randomUUID());
        final SchemaId dummyField = ids.getSchemaId(UUID.randomUUID());
        final RecordId id1 = ids.newRecordId("id1");

        final HashMultimap<DerefMap.Entry, SchemaId> empty = HashMultimap.create();
        derefMap.updateDependencies(id1, dummyVtag, empty);

        // consistency check
        final Set<DerefMap.DependingRecord> found = derefMap.findDependencies(id1, dummyVtag);
        assertTrue(found.isEmpty());

        final DerefMap.DependingRecord dependingRecord = new DerefMap.DependingRecord(id1, dummyVtag);
        final DerefMap.DependantRecordIdsIterator dependants = derefMap.findDependantsOf(dependingRecord, dummyField);
        assertFalse(dependants.hasNext());
    }

    @Test
    public void oneDependency() throws Exception {
        final SchemaId dummyVtag = ids.getSchemaId(UUID.randomUUID());
        final SchemaId dependingField = ids.getSchemaId(UUID.randomUUID());
        final SchemaId anotherField = ids.getSchemaId(UUID.randomUUID());
        final RecordId dependant = ids.newRecordId("dependant");
        final RecordId depending = ids.newRecordId("depending");
        final RecordId dependingAfterUpdate = ids.newRecordId("dependingAfterUpdate");

        // the dependant depends on the dependingField of the depending
        final HashMultimap<DerefMap.Entry, SchemaId> dependencies = HashMultimap.create();
        dependencies.put(new DerefMap.Entry(new DerefMap.DependingRecord(depending, dummyVtag)), dependingField);
        derefMap.updateDependencies(dependant, dummyVtag, dependencies);

        // consistency check
        final Set<DerefMap.DependingRecord> found = derefMap.findDependencies(dependant, dummyVtag);
        assertEquals(1, found.size());
        assertEquals(depending, found.iterator().next().getRecordId());

        // check that the dependant is found as only dependant of the depending via the dependingField
        final DerefMap.DependantRecordIdsIterator dependants =
                derefMap.findDependantsOf(new DerefMap.DependingRecord(depending, dummyVtag), dependingField);
        assertTrue(dependants.hasNext());
        assertEquals(dependant, dependants.next());
        assertFalse(dependants.hasNext());

        // check that nothing is found as depending on the dependant
        assertFalse(derefMap.findDependantsOf(new DerefMap.DependingRecord(dependant, dummyVtag), dependingField)
                .hasNext());

        // check that nothing is found as depending on the depending via another field than the dependingField
        assertFalse(
                derefMap.findDependantsOf(new DerefMap.DependingRecord(dependant, dummyVtag), anotherField).hasNext());

        // now update the dependency to be from the dependant to the dependingAfterUpdate (via the same field)
        final HashMultimap<DerefMap.Entry, SchemaId> updatedDependencies = HashMultimap.create();
        updatedDependencies
                .put(new DerefMap.Entry(new DerefMap.DependingRecord(dependingAfterUpdate, dummyVtag)), dependingField);
        derefMap.updateDependencies(dependant, dummyVtag, updatedDependencies);

        // consistency check
        final Set<DerefMap.DependingRecord> foundAfterUpdate = derefMap.findDependencies(dependant, dummyVtag);
        assertEquals(1, foundAfterUpdate.size());
        assertEquals(dependingAfterUpdate, foundAfterUpdate.iterator().next().getRecordId());

        // check that the dependant is found as only dependant of the dependingAfterUpdate via the dependingField
        final DerefMap.DependantRecordIdsIterator dependantsAfterUpdate =
                derefMap.findDependantsOf(
                        new DerefMap.DependingRecord(dependingAfterUpdate, dummyVtag), dependingField);
        assertTrue(dependantsAfterUpdate.hasNext());
        assertEquals(dependant, dependantsAfterUpdate.next());
        assertFalse(dependantsAfterUpdate.hasNext());

        // check that nothing is found any longer as depending on the previous depending (from before the update)
        assertFalse(derefMap.findDependantsOf(new DerefMap.DependingRecord(depending, dummyVtag), dependingField)
                .hasNext());
    }

    @Test
    public void oneDependencyWithMoreDimensionedVariants() throws Exception {
        final SchemaId dummyVtag = ids.getSchemaId(UUID.randomUUID());
        final SchemaId dependingField = ids.getSchemaId(UUID.randomUUID());
        final RecordId dependant = ids.newRecordId("master", ImmutableMap.of("bar", "x"));
        final RecordId depending = ids.newRecordId("master", ImmutableMap.of("bar", "x", "foo", "y"));

        // the dependant depends on the dependingField of the depending via a "+foo" dereferencing rule
        final HashMultimap<DerefMap.Entry, SchemaId> dependencies = HashMultimap.create();
        dependencies.put(new DerefMap.Entry(new DerefMap.DependingRecord(depending, dummyVtag), ImmutableSet.of("foo")),
                dependingField);
        derefMap.updateDependencies(dependant, dummyVtag, dependencies);

        // consistency check
        final Set<DerefMap.DependingRecord> found = derefMap.findDependencies(dependant, dummyVtag);
        assertEquals(1, found.size());
        assertEquals(depending.getMaster(), found.iterator().next().getRecordId());

        // check that the dependant is found as only dependant of the depending via the dependingField
        DerefMap.DependantRecordIdsIterator dependants =
                derefMap.findDependantsOf(new DerefMap.DependingRecord(depending, dummyVtag), dependingField);
        assertTrue(dependants.hasNext());
        assertEquals(dependant, dependants.next());
        assertFalse(dependants.hasNext());

        // check that other records (which would in reality not yet exist at index time) that match the "+foo" rule
        // are returned as dependants of our dependant (such that in reality reindexation of the dependant happens)

        final RecordId shouldTriggerOurDependant =
                ids.newRecordId("master", ImmutableMap.of("bar", "x", "foo", "another-value"));
        dependants = derefMap.findDependantsOf(new DerefMap.DependingRecord(shouldTriggerOurDependant, dummyVtag),
                dependingField);
        assertTrue(dependants.hasNext());
        assertEquals(dependant, dependants.next());
        assertFalse(dependants.hasNext());

        // doesn't have the foo property
        final RecordId shouldNotTriggerOurDependant1 = ids.newRecordId("master", ImmutableMap.of("bar", "x"));
        assertFalse(derefMap.findDependantsOf(new DerefMap.DependingRecord(shouldNotTriggerOurDependant1, dummyVtag),
                dependingField).hasNext());

        // doesn't have the bar property
        final RecordId shouldNotTriggerOurDependant2 = ids.newRecordId("master", ImmutableMap.of("foo", "x"));
        assertFalse(derefMap.findDependantsOf(new DerefMap.DependingRecord(shouldNotTriggerOurDependant2, dummyVtag),
                dependingField).hasNext());

        // wrong value for the bar property
        final RecordId shouldNotTriggerOurDependant3 =
                ids.newRecordId("master", ImmutableMap.of("bar", "y", "foo", "another-value"));
        assertFalse(derefMap.findDependantsOf(new DerefMap.DependingRecord(shouldNotTriggerOurDependant3, dummyVtag),
                dependingField).hasNext());

        // additional unmatched property
        final RecordId shouldNotTriggerOurDependant4 =
                ids.newRecordId("master", ImmutableMap.of("bar", "x", "foo", "another-value", "baz", "z"));
        assertFalse(derefMap.findDependantsOf(new DerefMap.DependingRecord(shouldNotTriggerOurDependant4, dummyVtag),
                dependingField).hasNext());

        // another master
        final RecordId shouldNotTriggerOurDependant5 =
                ids.newRecordId("another-master", ImmutableMap.of("bar", "x", "foo", "another-value"));
        assertFalse(derefMap.findDependantsOf(new DerefMap.DependingRecord(shouldNotTriggerOurDependant5, dummyVtag),
                dependingField).hasNext());

        // wrong properties
        final RecordId shouldNotTriggerOurDependant6 = ids.newRecordId("master", ImmutableMap.of("a", "b", "c", "d"));
        assertFalse(derefMap.findDependantsOf(new DerefMap.DependingRecord(shouldNotTriggerOurDependant6, dummyVtag),
                dependingField).hasNext());

        // no properties
        final RecordId shouldNotTriggerOurDependant7 = ids.newRecordId("master", ImmutableMap.<String, String>of());
        assertFalse(derefMap.findDependantsOf(new DerefMap.DependingRecord(shouldNotTriggerOurDependant7, dummyVtag),
                dependingField).hasNext());
    }

    @Test
    public void multipleDependencies() throws Exception {
        final SchemaId dummyVtag = ids.getSchemaId(UUID.randomUUID());
        final SchemaId dependingField = ids.getSchemaId(UUID.randomUUID());
        final RecordId dependant = ids.newRecordId("dependant");
        final RecordId depending1 = ids.newRecordId("depending1");
        final RecordId depending2 = ids.newRecordId("depending2");

        // the dependant depends on the dependingField of the depending1 and depending2
        final HashMultimap<DerefMap.Entry, SchemaId> dependencies = HashMultimap.create();
        dependencies.put(new DerefMap.Entry(new DerefMap.DependingRecord(depending1, dummyVtag)), dependingField);
        dependencies.put(new DerefMap.Entry(new DerefMap.DependingRecord(depending2, dummyVtag)), dependingField);
        derefMap.updateDependencies(dependant, dummyVtag, dependencies);

        // consistency check
        final Set<DerefMap.DependingRecord> found = derefMap.findDependencies(dependant, dummyVtag);
        assertEquals(2, found.size());

        // check that the dependant is found as only dependant of the depending1 via the dependingField
        final DerefMap.DependantRecordIdsIterator dependantsOf1 =
                derefMap.findDependantsOf(new DerefMap.DependingRecord(depending1, dummyVtag), dependingField);
        assertTrue(dependantsOf1.hasNext());
        assertEquals(dependant, dependantsOf1.next());
        assertFalse(dependantsOf1.hasNext());

        // check that the dependant is also found as only dependant of the depending2 via the dependingField
        final DerefMap.DependantRecordIdsIterator dependantsOf2 =
                derefMap.findDependantsOf(new DerefMap.DependingRecord(depending2, dummyVtag), dependingField);
        assertTrue(dependantsOf2.hasNext());
        assertEquals(dependant, dependantsOf2.next());
        assertFalse(dependantsOf2.hasNext());
    }

    @Test
    public void multipleDependants() throws Exception {
        final SchemaId dummyVtag = ids.getSchemaId(UUID.randomUUID());
        final SchemaId dependingField = ids.getSchemaId(UUID.randomUUID());
        final RecordId dependant1 = ids.newRecordId("dependant1");
        final RecordId dependant2 = ids.newRecordId("dependant2");
        final RecordId depending = ids.newRecordId("depending");

        // the dependant1 and dependant2 depend on the dependingField of the depending
        final HashMultimap<DerefMap.Entry, SchemaId> dependencies = HashMultimap.create();
        dependencies.put(new DerefMap.Entry(new DerefMap.DependingRecord(depending, dummyVtag)), dependingField);
        derefMap.updateDependencies(dependant1, dummyVtag, dependencies);
        derefMap.updateDependencies(dependant2, dummyVtag, dependencies);

        // consistency check dependant1
        final Set<DerefMap.DependingRecord> dependenciesOf1 = derefMap.findDependencies(dependant1, dummyVtag);
        assertEquals(1, dependenciesOf1.size());
        assertEquals(depending.getMaster(), dependenciesOf1.iterator().next().getRecordId());

        // consistency check dependant2
        final Set<DerefMap.DependingRecord> dependenciesOf2 = derefMap.findDependencies(dependant1, dummyVtag);
        assertEquals(1, dependenciesOf2.size());
        assertEquals(depending.getMaster(), dependenciesOf2.iterator().next().getRecordId());

        // check that both dependant1 and dependant2 are found as dependants of the depending
        final DerefMap.DependantRecordIdsIterator dependants =
                derefMap.findDependantsOf(new DerefMap.DependingRecord(depending, dummyVtag), dependingField);
        assertTrue(dependants.hasNext());
        final RecordId firstFoundDependant = dependants.next();
        assertTrue(dependants.hasNext());
        final RecordId secondFoundDependant = dependants.next();
        assertFalse(dependants.hasNext());

        // check that the two found dependants are dependant1 and dependant2 (order doesn't matter)
        assertTrue((dependant1.equals(firstFoundDependant) && dependant2.equals(secondFoundDependant)) ||
                (dependant2.equals(firstFoundDependant) && dependant1.equals(secondFoundDependant)));
    }

    @Test
    public void serializeFields() throws Exception {
        final Set<SchemaId> fields = new HashSet<SchemaId>();
        fields.add(ids.getSchemaId(UUID.randomUUID()));
        fields.add(ids.getSchemaId(UUID.randomUUID()));

        final Set<SchemaId> deserialized = derefMap.deserializeFields(derefMap.serializeFields(fields));

        assertEquals(fields, deserialized);
    }

    @Test
    public void serializeEntriesForward() throws Exception {
        final Set<DerefMap.DependingRecord> dependingRecords = new HashSet<DerefMap.DependingRecord>();
        dependingRecords.add(
                new DerefMap.DependingRecord(ids.newRecordId("id1"), ids.getSchemaId(UUID.randomUUID())));
        dependingRecords.add(
                new DerefMap.DependingRecord(ids.newRecordId("id2"), ids.getSchemaId(UUID.randomUUID())));

        final Set<DerefMap.DependingRecord> deserialized = derefMap.deserializeDependingRecordsForward(
                derefMap.serializeDependingRecordsForward(dependingRecords));

        assertEquals(dependingRecords, deserialized);
    }

    @Test
    public void serializeVariantPropertiesPattern() throws Exception {
        final HashMap<String, String> pattern = new HashMap<String, String>();
        pattern.put("foo", null);
        pattern.put("bar", "x");

        final DerefMapHbaseImpl.VariantPropertiesPattern variantPropertiesPattern =
                new DerefMapHbaseImpl.VariantPropertiesPattern(pattern);

        final DerefMapHbaseImpl.VariantPropertiesPattern deserialized =
                derefMap.deserializeVariantPropertiesPattern(
                        derefMap.serializeVariantPropertiesPattern(variantPropertiesPattern));

        Assert.assertEquals(variantPropertiesPattern, deserialized);
    }

}
