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
        final Set<DerefMap.Dependency> found = derefMap.findDependencies(id1, dummyVtag);
        assertTrue(found.isEmpty());

        final DerefMap.Dependency dependency = new DerefMap.Dependency(id1, dummyVtag);
        final DerefMap.DependantRecordIdsIterator dependants = derefMap.findDependantsOf(dependency, dummyField);
        assertFalse(dependants.hasNext());
    }

    @Test
    public void oneDependency() throws Exception {
        final SchemaId dummyVtag = ids.getSchemaId(UUID.randomUUID());
        final SchemaId dependencyField = ids.getSchemaId(UUID.randomUUID());
        final SchemaId anotherField = ids.getSchemaId(UUID.randomUUID());
        final RecordId dependant = ids.newRecordId("dependant");
        final RecordId dependency = ids.newRecordId("dependency");
        final RecordId dependencyAfterUpdate = ids.newRecordId("dependencyAfterUpdate");

        // the dependant depends on the dependencyField of the dependency
        final HashMultimap<DerefMap.Entry, SchemaId> dependencies = HashMultimap.create();
        dependencies.put(new DerefMap.Entry(new DerefMap.Dependency(dependency, dummyVtag)), dependencyField);
        derefMap.updateDependencies(dependant, dummyVtag, dependencies);

        // consistency check
        final Set<DerefMap.Dependency> found = derefMap.findDependencies(dependant, dummyVtag);
        assertEquals(1, found.size());
        assertEquals(dependency, found.iterator().next().getRecordId());

        // check that the dependant is found as only dependant of the dependency via the dependencyField
        final DerefMap.DependantRecordIdsIterator dependants =
                derefMap.findDependantsOf(new DerefMap.Dependency(dependency, dummyVtag), dependencyField);
        assertTrue(dependants.hasNext());
        assertEquals(dependant, dependants.next());
        assertFalse(dependants.hasNext());

        // check that nothing is found as dependency of the dependant
        assertFalse(derefMap.findDependantsOf(new DerefMap.Dependency(dependant, dummyVtag), dependencyField)
                .hasNext());

        // check that nothing is found as dependency of the dependency via another field than the dependencyField
        assertFalse(
                derefMap.findDependantsOf(new DerefMap.Dependency(dependant, dummyVtag), anotherField).hasNext());

        // now update the dependency to be from the dependant to the dependencyAfterUpdate (via the same field)
        final HashMultimap<DerefMap.Entry, SchemaId> updatedDependencies = HashMultimap.create();
        updatedDependencies
                .put(new DerefMap.Entry(new DerefMap.Dependency(dependencyAfterUpdate, dummyVtag)), dependencyField);
        derefMap.updateDependencies(dependant, dummyVtag, updatedDependencies);

        // consistency check
        final Set<DerefMap.Dependency> foundAfterUpdate = derefMap.findDependencies(dependant, dummyVtag);
        assertEquals(1, foundAfterUpdate.size());
        assertEquals(dependencyAfterUpdate, foundAfterUpdate.iterator().next().getRecordId());

        // check that the dependant is found as only dependant of the dependencyAfterUpdate via the dependencyField
        final DerefMap.DependantRecordIdsIterator dependantsAfterUpdate =
                derefMap.findDependantsOf(
                        new DerefMap.Dependency(dependencyAfterUpdate, dummyVtag), dependencyField);
        assertTrue(dependantsAfterUpdate.hasNext());
        assertEquals(dependant, dependantsAfterUpdate.next());
        assertFalse(dependantsAfterUpdate.hasNext());

        // check that nothing is found any longer as dependency on the previous dependency (from before the update)
        assertFalse(derefMap.findDependantsOf(new DerefMap.Dependency(dependency, dummyVtag), dependencyField)
                .hasNext());
    }

    @Test
    public void oneDependencyWithMoreDimensionedVariants() throws Exception {
        final SchemaId dummyVtag = ids.getSchemaId(UUID.randomUUID());
        final SchemaId dependencyField = ids.getSchemaId(UUID.randomUUID());
        final RecordId dependant = ids.newRecordId("master", ImmutableMap.of("bar", "x"));
        final RecordId dependency = ids.newRecordId("master", ImmutableMap.of("bar", "x", "foo", "y"));

        // the dependant depends on the dependencyField of the dependency via a "+foo" dereferencing rule
        final HashMultimap<DerefMap.Entry, SchemaId> dependencies = HashMultimap.create();
        dependencies.put(new DerefMap.Entry(new DerefMap.Dependency(dependency, dummyVtag), ImmutableSet.of("foo")),
                dependencyField);
        derefMap.updateDependencies(dependant, dummyVtag, dependencies);

        // consistency check
        final Set<DerefMap.Dependency> found = derefMap.findDependencies(dependant, dummyVtag);
        assertEquals(1, found.size());
        assertEquals(dependency.getMaster(), found.iterator().next().getRecordId());

        // check that the dependant is found as only dependant of the dependency via the dependencyField
        DerefMap.DependantRecordIdsIterator dependants =
                derefMap.findDependantsOf(new DerefMap.Dependency(dependency, dummyVtag), dependencyField);
        assertTrue(dependants.hasNext());
        assertEquals(dependant, dependants.next());
        assertFalse(dependants.hasNext());

        // check that other records (which would in reality not yet exist at index time) that match the "+foo" rule
        // are returned as dependants of our dependant (such that in reality reindexation of the dependant happens)

        final RecordId shouldTriggerOurDependant =
                ids.newRecordId("master", ImmutableMap.of("bar", "x", "foo", "another-value"));
        dependants = derefMap.findDependantsOf(new DerefMap.Dependency(shouldTriggerOurDependant, dummyVtag),
                dependencyField);
        assertTrue(dependants.hasNext());
        assertEquals(dependant, dependants.next());
        assertFalse(dependants.hasNext());

        // doesn't have the foo property
        final RecordId shouldNotTriggerOurDependant1 = ids.newRecordId("master", ImmutableMap.of("bar", "x"));
        assertFalse(derefMap.findDependantsOf(new DerefMap.Dependency(shouldNotTriggerOurDependant1, dummyVtag),
                dependencyField).hasNext());

        // doesn't have the bar property
        final RecordId shouldNotTriggerOurDependant2 = ids.newRecordId("master", ImmutableMap.of("foo", "x"));
        assertFalse(derefMap.findDependantsOf(new DerefMap.Dependency(shouldNotTriggerOurDependant2, dummyVtag),
                dependencyField).hasNext());

        // wrong value for the bar property
        final RecordId shouldNotTriggerOurDependant3 =
                ids.newRecordId("master", ImmutableMap.of("bar", "y", "foo", "another-value"));
        assertFalse(derefMap.findDependantsOf(new DerefMap.Dependency(shouldNotTriggerOurDependant3, dummyVtag),
                dependencyField).hasNext());

        // additional unmatched property
        final RecordId shouldNotTriggerOurDependant4 =
                ids.newRecordId("master", ImmutableMap.of("bar", "x", "foo", "another-value", "baz", "z"));
        assertFalse(derefMap.findDependantsOf(new DerefMap.Dependency(shouldNotTriggerOurDependant4, dummyVtag),
                dependencyField).hasNext());

        // another master
        final RecordId shouldNotTriggerOurDependant5 =
                ids.newRecordId("another-master", ImmutableMap.of("bar", "x", "foo", "another-value"));
        assertFalse(derefMap.findDependantsOf(new DerefMap.Dependency(shouldNotTriggerOurDependant5, dummyVtag),
                dependencyField).hasNext());

        // wrong properties
        final RecordId shouldNotTriggerOurDependant6 = ids.newRecordId("master", ImmutableMap.of("a", "b", "c", "d"));
        assertFalse(derefMap.findDependantsOf(new DerefMap.Dependency(shouldNotTriggerOurDependant6, dummyVtag),
                dependencyField).hasNext());

        // no properties
        final RecordId shouldNotTriggerOurDependant7 = ids.newRecordId("master", ImmutableMap.<String, String>of());
        assertFalse(derefMap.findDependantsOf(new DerefMap.Dependency(shouldNotTriggerOurDependant7, dummyVtag),
                dependencyField).hasNext());
    }

    @Test
    public void multipleDependencies() throws Exception {
        final SchemaId dummyVtag = ids.getSchemaId(UUID.randomUUID());
        final SchemaId dependencyField = ids.getSchemaId(UUID.randomUUID());
        final RecordId dependant = ids.newRecordId("dependant");
        final RecordId dependency1 = ids.newRecordId("dependency1");
        final RecordId dependency2 = ids.newRecordId("dependency2");

        // the dependant depends on the dependencyField of the dependency1 and dependency2
        final HashMultimap<DerefMap.Entry, SchemaId> dependencies = HashMultimap.create();
        dependencies.put(new DerefMap.Entry(new DerefMap.Dependency(dependency1, dummyVtag)), dependencyField);
        dependencies.put(new DerefMap.Entry(new DerefMap.Dependency(dependency2, dummyVtag)), dependencyField);
        derefMap.updateDependencies(dependant, dummyVtag, dependencies);

        // consistency check
        final Set<DerefMap.Dependency> found = derefMap.findDependencies(dependant, dummyVtag);
        assertEquals(2, found.size());

        // check that the dependant is found as only dependant of the dependency1 via the dependencyField
        final DerefMap.DependantRecordIdsIterator dependantsOf1 =
                derefMap.findDependantsOf(new DerefMap.Dependency(dependency1, dummyVtag), dependencyField);
        assertTrue(dependantsOf1.hasNext());
        assertEquals(dependant, dependantsOf1.next());
        assertFalse(dependantsOf1.hasNext());

        // check that the dependant is also found as only dependant of the dependency2 via the dependencyField
        final DerefMap.DependantRecordIdsIterator dependantsOf2 =
                derefMap.findDependantsOf(new DerefMap.Dependency(dependency2, dummyVtag), dependencyField);
        assertTrue(dependantsOf2.hasNext());
        assertEquals(dependant, dependantsOf2.next());
        assertFalse(dependantsOf2.hasNext());
    }

    @Test
    public void multipleDependants() throws Exception {
        final SchemaId dummyVtag = ids.getSchemaId(UUID.randomUUID());
        final SchemaId dependencyField = ids.getSchemaId(UUID.randomUUID());
        final RecordId dependant1 = ids.newRecordId("dependant1");
        final RecordId dependant2 = ids.newRecordId("dependant2");
        final RecordId dependency = ids.newRecordId("dependency");

        // the dependant1 and dependant2 depend on the dependencyField of the dependency
        final HashMultimap<DerefMap.Entry, SchemaId> dependencies = HashMultimap.create();
        dependencies.put(new DerefMap.Entry(new DerefMap.Dependency(dependency, dummyVtag)), dependencyField);
        derefMap.updateDependencies(dependant1, dummyVtag, dependencies);
        derefMap.updateDependencies(dependant2, dummyVtag, dependencies);

        // consistency check dependant1
        final Set<DerefMap.Dependency> dependenciesOf1 = derefMap.findDependencies(dependant1, dummyVtag);
        assertEquals(1, dependenciesOf1.size());
        assertEquals(dependency.getMaster(), dependenciesOf1.iterator().next().getRecordId());

        // consistency check dependant2
        final Set<DerefMap.Dependency> dependenciesOf2 = derefMap.findDependencies(dependant1, dummyVtag);
        assertEquals(1, dependenciesOf2.size());
        assertEquals(dependency.getMaster(), dependenciesOf2.iterator().next().getRecordId());

        // check that both dependant1 and dependant2 are found as dependants of the dependency
        final DerefMap.DependantRecordIdsIterator dependants =
                derefMap.findDependantsOf(new DerefMap.Dependency(dependency, dummyVtag), dependencyField);
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
        final Set<DerefMap.Dependency> dependencies = new HashSet<DerefMap.Dependency>();
        dependencies.add(
                new DerefMap.Dependency(ids.newRecordId("id1"), ids.getSchemaId(UUID.randomUUID())));
        dependencies.add(
                new DerefMap.Dependency(ids.newRecordId("id2"), ids.getSchemaId(UUID.randomUUID())));

        final Set<DerefMap.Dependency> deserialized = derefMap.deserializeDependenciesForward(
                derefMap.serializeDependenciesForward(dependencies));

        assertEquals(dependencies, deserialized);
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
