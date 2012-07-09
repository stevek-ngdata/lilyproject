package org.lilyproject.indexer.engine;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;
import junit.framework.Assert;
import org.apache.hadoop.thirdparty.guava.common.collect.ImmutableSet;
import org.apache.hadoop.thirdparty.guava.common.collect.Sets;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
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
import static org.junit.Assert.fail;

/**
 * This tests the functionality of the {@link DerefMapHbaseImpl}. Note there is also a {@link DerefMapIndexTest} which
 * tests it in real indexing scenario's.
 *
 * @author Jan Van Besien
 */
public class DerefMapBasicTest {
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

    @Test
    public void emptyDependencies() throws Exception {
        final SchemaId dummyVtag = ids.getSchemaId(UUID.randomUUID());
        final SchemaId dummyField = ids.getSchemaId(UUID.randomUUID());
        final RecordId id1 = ids.newRecordId("id1");

        final HashMap<DerefMap.DependencyEntry, Set<SchemaId>> empty =
                new HashMap<DerefMap.DependencyEntry, Set<SchemaId>>();
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
        final HashMap<DerefMap.DependencyEntry, Set<SchemaId>> dependencies =
                new HashMap<DerefMap.DependencyEntry, Set<SchemaId>>();
        dependencies.put(new DerefMap.DependencyEntry(new DerefMap.Dependency(dependency, dummyVtag)),
                Sets.newHashSet(dependencyField));
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
        assertFalse(derefMap.findDependantsOf(new DerefMap.Dependency(dependant, dummyVtag), anotherField).hasNext());

        // now update the dependency to be from the dependant to the dependencyAfterUpdate (via the same field)
        final HashMap<DerefMap.DependencyEntry, Set<SchemaId>> updatedDependencies =
                new HashMap<DerefMap.DependencyEntry, Set<SchemaId>>();
        updatedDependencies
                .put(new DerefMap.DependencyEntry(new DerefMap.Dependency(dependencyAfterUpdate, dummyVtag)),
                        Sets.newHashSet(dependencyField));
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
        final RecordId dependant = ids.newRecordId("master", ImmutableMap.of("bar", "x"));
        final RecordId dependency = ids.newRecordId("master", ImmutableMap.of("bar", "x", "foo", "y"));

        // the dependant depends on the dependencyField of the dependency via a "+foo" dereferencing rule
        final HashMap<DerefMap.DependencyEntry, Set<SchemaId>> dependencies =
                new HashMap<DerefMap.DependencyEntry, Set<SchemaId>>();
        dependencies.put(new DerefMap.DependencyEntry(new DerefMap.Dependency(dependency, dummyVtag),
                ImmutableSet.of("foo")), Sets.<SchemaId>newHashSet());
        derefMap.updateDependencies(dependant, dummyVtag, dependencies);

        // consistency check
        final Set<DerefMap.Dependency> found = derefMap.findDependencies(dependant, dummyVtag);
        assertEquals(1, found.size());
        assertEquals(dependency.getMaster(), found.iterator().next().getRecordId());

        // check that the dependant is found as only dependant of the dependency (without specifying a field)
        DerefMap.DependantRecordIdsIterator dependants =
                derefMap.findDependantsOf(new DerefMap.Dependency(dependency, dummyVtag), null);
        assertTrue(dependants.hasNext());
        assertEquals(dependant, dependants.next());
        assertFalse(dependants.hasNext());

        // check that other records (which would in reality not yet exist at index time) that match the "+foo" rule
        // are returned as dependants of our dependant (such that in reality reindexation of the dependant happens)

        final RecordId shouldTriggerOurDependant =
                ids.newRecordId("master", ImmutableMap.of("bar", "x", "foo", "another-value"));
        dependants = derefMap.findDependantsOf(new DerefMap.Dependency(shouldTriggerOurDependant, dummyVtag),
                null);
        assertTrue(dependants.hasNext());
        assertEquals(dependant, dependants.next());
        assertFalse(dependants.hasNext());

        // doesn't have the foo property
        final RecordId shouldNotTriggerOurDependant1 = ids.newRecordId("master", ImmutableMap.of("bar", "x"));
        assertFalse(derefMap.findDependantsOf(new DerefMap.Dependency(shouldNotTriggerOurDependant1, dummyVtag),
                null).hasNext());

        // doesn't have the bar property
        final RecordId shouldNotTriggerOurDependant2 = ids.newRecordId("master", ImmutableMap.of("foo", "x"));
        assertFalse(derefMap.findDependantsOf(new DerefMap.Dependency(shouldNotTriggerOurDependant2, dummyVtag),
                null).hasNext());

        // wrong value for the bar property
        final RecordId shouldNotTriggerOurDependant3 =
                ids.newRecordId("master", ImmutableMap.of("bar", "y", "foo", "another-value"));
        assertFalse(derefMap.findDependantsOf(new DerefMap.Dependency(shouldNotTriggerOurDependant3, dummyVtag),
                null).hasNext());

        // additional unmatched property
        final RecordId shouldNotTriggerOurDependant4 =
                ids.newRecordId("master", ImmutableMap.of("bar", "x", "foo", "another-value", "baz", "z"));
        assertFalse(derefMap.findDependantsOf(new DerefMap.Dependency(shouldNotTriggerOurDependant4, dummyVtag),
                null).hasNext());

        // another master
        final RecordId shouldNotTriggerOurDependant5 =
                ids.newRecordId("another-master", ImmutableMap.of("bar", "x", "foo", "another-value"));
        assertFalse(derefMap.findDependantsOf(new DerefMap.Dependency(shouldNotTriggerOurDependant5, dummyVtag),
                null).hasNext());

        // wrong properties
        final RecordId shouldNotTriggerOurDependant6 = ids.newRecordId("master", ImmutableMap.of("a", "b", "c", "d"));
        assertFalse(derefMap.findDependantsOf(new DerefMap.Dependency(shouldNotTriggerOurDependant6, dummyVtag),
                null).hasNext());

        // no properties
        final RecordId shouldNotTriggerOurDependant7 = ids.newRecordId("master", ImmutableMap.<String, String>of());
        assertFalse(derefMap.findDependantsOf(new DerefMap.Dependency(shouldNotTriggerOurDependant7, dummyVtag),
                null).hasNext());
    }

    /**
     * Simulates what would happen in case of a dereference expression like n:link1=>n:link2=>n:field.
     */
    @Test
    public void chainOfDependencies() throws Exception {
        final SchemaId dummyVtag = ids.getSchemaId(UUID.randomUUID());
        final SchemaId linkField1 = ids.getSchemaId(UUID.randomUUID());
        final SchemaId linkField2 = ids.getSchemaId(UUID.randomUUID());
        final SchemaId field = ids.getSchemaId(UUID.randomUUID());
        final RecordId dependant = ids.newRecordId("dependant");
        final RecordId dependency1 = ids.newRecordId("dependency1");
        final RecordId dependency2 = ids.newRecordId("dependency2");

        // scenario: dependant has linkField1 -> dependency1 which has linkField2 -> dependency2 which has field "field"
        final Map<DerefMap.DependencyEntry, Set<SchemaId>> dependencies =
                new HashMap<DerefMap.DependencyEntry, Set<SchemaId>>();
        // 1) dependant depends on dependency1 from which it uses linkField2
        dependencies.put(new DerefMap.DependencyEntry(new DerefMap.Dependency(dependency1, dummyVtag)),
                Sets.newHashSet(linkField2));
        // 2) dependant depends on dependency2 from which it uses field
        dependencies.put(new DerefMap.DependencyEntry(new DerefMap.Dependency(dependency2, dummyVtag)),
                Sets.newHashSet(field));
        derefMap.updateDependencies(dependant, dummyVtag, dependencies);

        // consistency check
        final Set<DerefMap.Dependency> found = derefMap.findDependencies(dependant, dummyVtag);
        assertEquals(2, found.size());

        // check that the dependant is found as only dependant of the dependencies via the corresponding fields
        DerefMap.DependantRecordIdsIterator viaDependency1AndLinkField1 =
                derefMap.findDependantsOf(new DerefMap.Dependency(dependency1, dummyVtag), linkField1);
        assertFalse(viaDependency1AndLinkField1.hasNext());

        DerefMap.DependantRecordIdsIterator viaDependency2AndLinkField1 =
                derefMap.findDependantsOf(new DerefMap.Dependency(dependency2, dummyVtag), linkField1);
        assertFalse(viaDependency2AndLinkField1.hasNext());

        DerefMap.DependantRecordIdsIterator viaDependency1AndLinkField2 =
                derefMap.findDependantsOf(new DerefMap.Dependency(dependency1, dummyVtag), linkField2);
        assertTrue(viaDependency1AndLinkField2.hasNext());
        assertEquals(dependant, viaDependency1AndLinkField2.next());
        assertFalse(viaDependency1AndLinkField2.hasNext());

        DerefMap.DependantRecordIdsIterator viaDependency2AndLinkField2 =
                derefMap.findDependantsOf(new DerefMap.Dependency(dependency2, dummyVtag), linkField2);
        assertFalse(viaDependency2AndLinkField2.hasNext());

        DerefMap.DependantRecordIdsIterator viaDependency2AndField =
                derefMap.findDependantsOf(new DerefMap.Dependency(dependency2, dummyVtag), field);
        assertTrue(viaDependency2AndField.hasNext());
        assertEquals(dependant, viaDependency2AndField.next());
        assertFalse(viaDependency2AndField.hasNext());

        DerefMap.DependantRecordIdsIterator viaDependency1WithoutSpecifyingField =
                derefMap.findDependantsOf(new DerefMap.Dependency(dependency1, dummyVtag), null);
        assertTrue(viaDependency1WithoutSpecifyingField.hasNext());
        assertEquals(dependant, viaDependency1WithoutSpecifyingField.next());
        assertFalse(viaDependency1WithoutSpecifyingField.hasNext());

        DerefMap.DependantRecordIdsIterator viaDependency2WithoutSpecifyingField =
                derefMap.findDependantsOf(new DerefMap.Dependency(dependency2, dummyVtag), null);
        assertTrue(viaDependency2WithoutSpecifyingField.hasNext());
        assertEquals(dependant, viaDependency2WithoutSpecifyingField.next());
        assertFalse(viaDependency2WithoutSpecifyingField.hasNext());
    }

    /**
     * Simulates what would happen in case of a dereference expression like n:link1=>n:link2=>n:link3.
     */
    @Test
    public void chainOfDependenciesWhichDoesNotEndInField() throws Exception {
        final SchemaId dummyVtag = ids.getSchemaId(UUID.randomUUID());
        final SchemaId linkField1 = ids.getSchemaId(UUID.randomUUID());
        final SchemaId linkField2 = ids.getSchemaId(UUID.randomUUID());
        final SchemaId linkField3 = ids.getSchemaId(UUID.randomUUID());
        final RecordId dependant = ids.newRecordId("dependant");
        final RecordId dependency1 = ids.newRecordId("dependency1");
        final RecordId dependency2 = ids.newRecordId("dependency2");
        final RecordId dependency3 = ids.newRecordId("dependency3");

        // scenario: dependant has linkField1 -> dependency1 which has linkField2 -> dependency2 which has linkField3 -> dependency3
        final Map<DerefMap.DependencyEntry, Set<SchemaId>> dependencies =
                new HashMap<DerefMap.DependencyEntry, Set<SchemaId>>();
        // 1) dependant depends on dependency1 from which it uses linkField2
        dependencies.put(new DerefMap.DependencyEntry(new DerefMap.Dependency(dependency1, dummyVtag)),
                Sets.newHashSet(linkField2));
        // 2) dependant depends on dependency2 from which it uses linkField3 which points to dependency3
        dependencies.put(new DerefMap.DependencyEntry(new DerefMap.Dependency(dependency2, dummyVtag)),
                Sets.newHashSet(linkField3));
        // 3) dependant depends on the whole record dependency3 (via linkField3)
        dependencies.put(new DerefMap.DependencyEntry(new DerefMap.Dependency(dependency3, dummyVtag)),
                Sets.<SchemaId>newHashSet());
        derefMap.updateDependencies(dependant, dummyVtag, dependencies);

        // consistency check
        final Set<DerefMap.Dependency> found = derefMap.findDependencies(dependant, dummyVtag);
        assertEquals(3, found.size());

        // check that the dependant is found as only dependant of the dependencies via the corresponding fields
        DerefMap.DependantRecordIdsIterator viaDependency1AndLinkField2 =
                derefMap.findDependantsOf(new DerefMap.Dependency(dependency1, dummyVtag), linkField2);
        assertTrue(viaDependency1AndLinkField2.hasNext());
        assertEquals(dependant, viaDependency1AndLinkField2.next());
        assertFalse(viaDependency1AndLinkField2.hasNext());

        DerefMap.DependantRecordIdsIterator viaDependency2AndLinkField3 =
                derefMap.findDependantsOf(new DerefMap.Dependency(dependency2, dummyVtag), linkField3);
        assertTrue(viaDependency2AndLinkField3.hasNext());
        assertEquals(dependant, viaDependency2AndLinkField3.next());
        assertFalse(viaDependency2AndLinkField3.hasNext());

        DerefMap.DependantRecordIdsIterator viaDependency3 =
                derefMap.findDependantsOf(new DerefMap.Dependency(dependency3, dummyVtag), null);
        assertTrue(viaDependency3.hasNext());
        assertEquals(dependant, viaDependency3.next());
        assertFalse(viaDependency3.hasNext());
    }

    /**
     * Simulates what would happen in case of a dereference expression like +prop1=>n:link1=>+prop2=>n:field.
     */
    @Ignore
    @Test
    public void chainOfDependenciesIncludingMoreDimensionedVariantProperties() throws Exception {
        fail("todo");
    }

    @Test
    public void multipleDependencies() throws Exception {
        final SchemaId dummyVtag = ids.getSchemaId(UUID.randomUUID());
        final SchemaId dependencyField = ids.getSchemaId(UUID.randomUUID());
        final RecordId dependant = ids.newRecordId("dependant");
        final RecordId dependency1 = ids.newRecordId("dependency1");
        final RecordId dependency2 = ids.newRecordId("dependency2");

        // the dependant depends on the dependencyField of the dependency1 and dependency2
        final HashMap<DerefMap.DependencyEntry, Set<SchemaId>> dependencies =
                new HashMap<DerefMap.DependencyEntry, Set<SchemaId>>();
        dependencies
                .put(new DerefMap.DependencyEntry(new DerefMap.Dependency(dependency1, dummyVtag)),
                        Sets.newHashSet(dependencyField));
        dependencies
                .put(new DerefMap.DependencyEntry(new DerefMap.Dependency(dependency2, dummyVtag)),
                        Sets.newHashSet(dependencyField));
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
        final HashMap<DerefMap.DependencyEntry, Set<SchemaId>> dependencies =
                new HashMap<DerefMap.DependencyEntry, Set<SchemaId>>();
        dependencies.put(new DerefMap.DependencyEntry(new DerefMap.Dependency(dependency, dummyVtag)),
                Sets.newHashSet(dependencyField));
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
