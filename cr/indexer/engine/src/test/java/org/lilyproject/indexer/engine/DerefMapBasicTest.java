package org.lilyproject.indexer.engine;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import junit.framework.Assert;
import org.apache.hadoop.thirdparty.guava.common.collect.Sets;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilyproject.hadooptestfw.TestHelper;
import org.lilyproject.indexer.engine.DerefMap.DependencyEntry;
import org.lilyproject.indexer.engine.test.DerefMapIndexTest;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repotestfw.RepositorySetup;
import org.lilyproject.util.io.Closer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
        final Set<DerefMap.DependencyEntry> found = derefMap.findDependencies(id1, dummyVtag);
        assertTrue(found.isEmpty());

        final DerefMap.DependantRecordIdsIterator dependants = derefMap.findDependantsOf(id1, dummyField);
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
        dependencies.put(new DerefMap.DependencyEntry(dependency), Sets.newHashSet(dependencyField));
        derefMap.updateDependencies(dependant, dummyVtag, dependencies);

        // consistency check
        final Set<DerefMap.DependencyEntry> found = derefMap.findDependencies(dependant, dummyVtag);
        assertEquals(1, found.size());
        assertEquals(dependency, found.iterator().next().getDependency());

        // check that the dependant is found as only dependant of the dependency via the dependencyField
        final DerefMap.DependantRecordIdsIterator dependants =
                derefMap.findDependantsOf(dependency, dependencyField);
        assertTrue(dependants.hasNext());
        assertEquals(dependant, dependants.next());
        assertFalse(dependants.hasNext());

        // check that nothing is found as dependency of the dependant
        assertFalse(derefMap.findDependantsOf(dependant, dependencyField).hasNext());

        // check that nothing is found as dependency of the dependency via another field than the dependencyField
        assertFalse(derefMap.findDependantsOf(dependency, anotherField).hasNext());

        // now update the dependency to be from the dependant to the dependencyAfterUpdate (via the same field)
        final HashMap<DerefMap.DependencyEntry, Set<SchemaId>> updatedDependencies =
                new HashMap<DerefMap.DependencyEntry, Set<SchemaId>>();
        updatedDependencies
                .put(new DerefMap.DependencyEntry(dependencyAfterUpdate), Sets.newHashSet(dependencyField));
        derefMap.updateDependencies(dependant, dummyVtag, updatedDependencies);

        // consistency check
        final Set<DerefMap.DependencyEntry> foundAfterUpdate = derefMap.findDependencies(dependant, dummyVtag);
        assertEquals(1, foundAfterUpdate.size());
        assertEquals(dependencyAfterUpdate, foundAfterUpdate.iterator().next().getDependency());

        // check that the dependant is found as only dependant of the dependencyAfterUpdate via the dependencyField
        final DerefMap.DependantRecordIdsIterator dependantsAfterUpdate =
                derefMap.findDependantsOf(dependencyAfterUpdate, dependencyField);
        assertTrue(dependantsAfterUpdate.hasNext());
        assertEquals(dependant, dependantsAfterUpdate.next());
        assertFalse(dependantsAfterUpdate.hasNext());

        // check that nothing is found any longer as dependency on the previous dependency (from before the update)
        assertFalse(derefMap.findDependantsOf(dependency, dependencyField).hasNext());
    }

    @Test
    public void oneDependencyWithMoreDimensionedVariants() throws Exception {
        final SchemaId dummyVtag = ids.getSchemaId(UUID.randomUUID());
        final RecordId dependant = ids.newRecordId("master", ImmutableMap.of("bar", "x"));
        final RecordId dependency = ids.newRecordId("master", ImmutableMap.of("bar", "x", "foo", "y"));

        // the dependant depends on the dependencyField of the dependency via a "+foo" dereferencing rule
        final HashMap<DerefMap.DependencyEntry, Set<SchemaId>> dependencies =
                new HashMap<DerefMap.DependencyEntry, Set<SchemaId>>();
        dependencies.put(new DerefMap.DependencyEntry(dependency,
                ImmutableSet.of("foo")), Sets.<SchemaId>newHashSet());
        derefMap.updateDependencies(dependant, dummyVtag, dependencies);

        // consistency check
        final Set<DerefMap.DependencyEntry> found = derefMap.findDependencies(dependant, dummyVtag);
        assertEquals(1, found.size());
        final DerefMap.DependencyEntry foundDependencyEntry = found.iterator().next();
        assertEquals(ids.newRecordId("master", ImmutableMap.of("bar", "x")), foundDependencyEntry.getDependency());
        assertEquals(Sets.newHashSet("foo"), foundDependencyEntry.getMoreDimensionedVariants());

        // check that the dependant is found as only dependant of the dependency (without specifying a field)
        DerefMap.DependantRecordIdsIterator dependants =
                derefMap.findDependantsOf(dependency, null);
        assertTrue(dependants.hasNext());
        assertEquals(dependant, dependants.next());
        assertFalse(dependants.hasNext());

        // check that other records (which would in reality not yet exist at index time) that match the "+foo" rule
        // are returned as dependants of our dependant (such that in reality reindexation of the dependant happens)

        final RecordId shouldTriggerOurDependant =
                ids.newRecordId("master", ImmutableMap.of("bar", "x", "foo", "another-value"));
        dependants = derefMap.findDependantsOf(shouldTriggerOurDependant, null);
        assertTrue(dependants.hasNext());
        assertEquals(dependant, dependants.next());
        assertFalse(dependants.hasNext());

        // doesn't have the foo property
        final RecordId shouldNotTriggerOurDependant1 = ids.newRecordId("master", ImmutableMap.of("bar", "x"));
        assertFalse(derefMap.findDependantsOf(shouldNotTriggerOurDependant1, null).hasNext());

        // doesn't have the bar property
        final RecordId shouldNotTriggerOurDependant2 = ids.newRecordId("master", ImmutableMap.of("foo", "x"));
        assertFalse(derefMap.findDependantsOf(shouldNotTriggerOurDependant2, null).hasNext());

        // wrong value for the bar property
        final RecordId shouldNotTriggerOurDependant3 =
                ids.newRecordId("master", ImmutableMap.of("bar", "y", "foo", "another-value"));
        assertFalse(derefMap.findDependantsOf(shouldNotTriggerOurDependant3, null).hasNext());

        // additional unmatched property
        final RecordId shouldNotTriggerOurDependant4 =
                ids.newRecordId("master", ImmutableMap.of("bar", "x", "foo", "another-value", "baz", "z"));
        assertFalse(derefMap.findDependantsOf(shouldNotTriggerOurDependant4, null).hasNext());

        // another master
        final RecordId shouldNotTriggerOurDependant5 =
                ids.newRecordId("another-master", ImmutableMap.of("bar", "x", "foo", "another-value"));
        assertFalse(derefMap.findDependantsOf(shouldNotTriggerOurDependant5, null).hasNext());

        // wrong properties
        final RecordId shouldNotTriggerOurDependant6 = ids.newRecordId("master", ImmutableMap.of("a", "b", "c", "d"));
        assertFalse(derefMap.findDependantsOf(shouldNotTriggerOurDependant6, null).hasNext());

        // no properties
        final RecordId shouldNotTriggerOurDependant7 = ids.newRecordId("master", ImmutableMap.<String, String>of());
        assertFalse(derefMap.findDependantsOf(shouldNotTriggerOurDependant7, null).hasNext());
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
        dependencies.put(new DerefMap.DependencyEntry(dependency1), Sets.newHashSet(linkField2));
        // 2) dependant depends on dependency2 from which it uses field
        dependencies.put(new DerefMap.DependencyEntry(dependency2), Sets.newHashSet(field));
        derefMap.updateDependencies(dependant, dummyVtag, dependencies);

        // consistency check
        final Set<DerefMap.DependencyEntry> found = derefMap.findDependencies(dependant, dummyVtag);
        assertEquals(2, found.size());

        // check that the dependant is found as only dependant of the dependencies via the corresponding fields
        DerefMap.DependantRecordIdsIterator viaDependency1AndLinkField1 =
                derefMap.findDependantsOf(dependency1, linkField1);
        assertFalse(viaDependency1AndLinkField1.hasNext());

        DerefMap.DependantRecordIdsIterator viaDependency2AndLinkField1 =
                derefMap.findDependantsOf(dependency2, linkField1);
        assertFalse(viaDependency2AndLinkField1.hasNext());

        DerefMap.DependantRecordIdsIterator viaDependency1AndLinkField2 =
                derefMap.findDependantsOf(dependency1, linkField2);
        assertTrue(viaDependency1AndLinkField2.hasNext());
        assertEquals(dependant, viaDependency1AndLinkField2.next());
        assertFalse(viaDependency1AndLinkField2.hasNext());

        DerefMap.DependantRecordIdsIterator viaDependency2AndLinkField2 =
                derefMap.findDependantsOf(dependency2, linkField2);
        assertFalse(viaDependency2AndLinkField2.hasNext());

        DerefMap.DependantRecordIdsIterator viaDependency2AndField =
                derefMap.findDependantsOf(dependency2, field);
        assertTrue(viaDependency2AndField.hasNext());
        assertEquals(dependant, viaDependency2AndField.next());
        assertFalse(viaDependency2AndField.hasNext());

        DerefMap.DependantRecordIdsIterator viaDependency1WithoutSpecifyingField =
                derefMap.findDependantsOf(dependency1, null);
        assertTrue(viaDependency1WithoutSpecifyingField.hasNext());
        assertEquals(dependant, viaDependency1WithoutSpecifyingField.next());
        assertFalse(viaDependency1WithoutSpecifyingField.hasNext());

        DerefMap.DependantRecordIdsIterator viaDependency2WithoutSpecifyingField =
                derefMap.findDependantsOf(dependency2, null);
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
        dependencies.put(new DerefMap.DependencyEntry(dependency1),
                Sets.newHashSet(linkField2));
        // 2) dependant depends on dependency2 from which it uses linkField3 which points to dependency3
        dependencies.put(new DerefMap.DependencyEntry(dependency2),
                Sets.newHashSet(linkField3));
        derefMap.updateDependencies(dependant, dummyVtag, dependencies);

        // consistency check
        final Set<DerefMap.DependencyEntry> found = derefMap.findDependencies(dependant, dummyVtag);
        assertEquals(2, found.size());

        // check that the dependant is found as only dependant of the dependencies via the corresponding fields
        DerefMap.DependantRecordIdsIterator viaDependency1AndLinkField2 =
                derefMap.findDependantsOf(dependency1, linkField2);
        assertTrue(viaDependency1AndLinkField2.hasNext());
        assertEquals(dependant, viaDependency1AndLinkField2.next());
        assertFalse(viaDependency1AndLinkField2.hasNext());

        DerefMap.DependantRecordIdsIterator viaDependency2AndLinkField3 =
                derefMap.findDependantsOf(dependency2, linkField3);
        assertTrue(viaDependency2AndLinkField3.hasNext());
        assertEquals(dependant, viaDependency2AndLinkField3.next());
        assertFalse(viaDependency2AndLinkField3.hasNext());
    }

    /**
     * Simulates what would happen in case of a dereference expression like +prop1=>n:link1=>+prop2=>n:field.
     */
    @Test
    public void chainOfDependenciesIncludingMoreDimensionedVariantProperties() throws Exception {
        final SchemaId dummyVtag = ids.getSchemaId(UUID.randomUUID());
        final SchemaId linkField1 = ids.getSchemaId(UUID.randomUUID());
        final SchemaId field = ids.getSchemaId(UUID.randomUUID());
        final RecordId dependant = ids.newRecordId("dependant");
        final RecordId dependantWithProp1 = ids.newRecordId("dependant", ImmutableMap.of("prop1", "x"));
        final RecordId dependency1 = ids.newRecordId("dependency1");
        final RecordId dependency1WithProp2 = ids.newRecordId("dependency1", ImmutableMap.of("prop2", "y"));

        // scenario: dependant depends on all dependant +prop1 records. One such a record (dependantWithProp1) exists
        // with a linkField1 pointing to dependency1. Via the +prop2 rule, we end up with all dependency1 + prop2
        // records, of which there is one instance (dependency1WithProp2). Of this instance, we use the field "field".
        final Map<DerefMap.DependencyEntry, Set<SchemaId>> dependencies =
                new HashMap<DerefMap.DependencyEntry, Set<SchemaId>>();
        // 1) dependant depends on all similar with prop1, of which it uses linkField1
        dependencies
                .put(new DerefMap.DependencyEntry(dependant, ImmutableSet.of("prop1")), Sets.newHashSet(linkField1));
        // 2) dependant depends on dependency1WithProp2 (and all similar with prop2) from which it uses field "field"
        dependencies.put(new DerefMap.DependencyEntry(dependency1WithProp2, ImmutableSet.of("prop2")),
                Sets.newHashSet(field));
        derefMap.updateDependencies(dependant, dummyVtag, dependencies);

        // consistency check
        final Set<DerefMap.DependencyEntry> found = derefMap.findDependencies(dependant, dummyVtag);
        assertEquals(2, found.size());

        // check that the dependant is found as only dependant of the dependencies via the corresponding fields (in a few scenarios)

        // scenario1: as if a dependency1 with prop2=value is being created
        final RecordId someRecordLikeDependency1WithProp2 =
                ids.newRecordId("dependency1", ImmutableMap.of("prop2", "value"));
        DerefMap.DependantRecordIdsIterator scenario1 =
                derefMap.findDependantsOf(someRecordLikeDependency1WithProp2, field);
        assertTrue(scenario1.hasNext());
        assertEquals(dependant, scenario1.next());
        assertFalse(scenario1.hasNext());

        // scenario2: as if a new record like dependant is created with prop1=value
        final RecordId someRecordLikeDependantWithProp1 =
                ids.newRecordId("dependant", ImmutableMap.of("prop1", "value"));
        DerefMap.DependantRecordIdsIterator scenario2 =
                derefMap.findDependantsOf(someRecordLikeDependantWithProp1, linkField1);
        assertTrue(scenario2.hasNext());
        assertEquals(dependant, scenario2.next());
        assertFalse(scenario2.hasNext());
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
        dependencies.put(new DerefMap.DependencyEntry(dependency1), Sets.newHashSet(dependencyField));
        dependencies.put(new DerefMap.DependencyEntry(dependency2), Sets.newHashSet(dependencyField));
        derefMap.updateDependencies(dependant, dummyVtag, dependencies);

        // consistency check
        final Set<DerefMap.DependencyEntry> found = derefMap.findDependencies(dependant, dummyVtag);
        assertEquals(2, found.size());

        // check that the dependant is found as only dependant of the dependency1 via the dependencyField
        final DerefMap.DependantRecordIdsIterator dependantsOf1 =
                derefMap.findDependantsOf(dependency1, dependencyField);
        assertTrue(dependantsOf1.hasNext());
        assertEquals(dependant, dependantsOf1.next());
        assertFalse(dependantsOf1.hasNext());

        // check that the dependant is also found as only dependant of the dependency2 via the dependencyField
        final DerefMap.DependantRecordIdsIterator dependantsOf2 =
                derefMap.findDependantsOf(dependency2, dependencyField);
        assertTrue(dependantsOf2.hasNext());
        assertEquals(dependant, dependantsOf2.next());
        assertFalse(dependantsOf2.hasNext());
    }

    @Test
    public void multipleVariantsDependingOnMaster() throws Exception {
        final SchemaId dummyVtag = ids.getSchemaId(UUID.randomUUID());
        final SchemaId dependencyField = ids.getSchemaId(UUID.randomUUID());

        final RecordId master = ids.newRecordId("myrecord");
        final RecordId v1variant = ids.newRecordId("myrecord", Collections.singletonMap("v1", "x"));
        final RecordId v1v2variant = ids.newRecordId("myrecord", map("v1", "x", "v2", "y"));

        final HashMap<DerefMap.DependencyEntry, Set<SchemaId>> dependencies =
                new HashMap<DerefMap.DependencyEntry, Set<SchemaId>>();
        dependencies.put(new DerefMap.DependencyEntry(master), Sets.newHashSet(dependencyField));
        derefMap.updateDependencies(v1variant, dummyVtag, dependencies);
        derefMap.updateDependencies(v1v2variant, dummyVtag, dependencies);

        Set<RecordId> recordIds = asRecordIds(derefMap.findDependantsOf(master, null));
        assertEquals(recordIds, Sets.newHashSet(v1variant, v1v2variant));
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
        dependencies.put(new DerefMap.DependencyEntry(dependency),
                Sets.newHashSet(dependencyField));
        derefMap.updateDependencies(dependant1, dummyVtag, dependencies);
        derefMap.updateDependencies(dependant2, dummyVtag, dependencies);

        // consistency check dependant1
        final Set<DerefMap.DependencyEntry> dependenciesOf1 = derefMap.findDependencies(dependant1, dummyVtag);
        assertEquals(1, dependenciesOf1.size());
        assertEquals(dependency.getMaster(), dependenciesOf1.iterator().next().getDependency());

        // consistency check dependant2
        final Set<DerefMap.DependencyEntry> dependenciesOf2 = derefMap.findDependencies(dependant1, dummyVtag);
        assertEquals(1, dependenciesOf2.size());
        assertEquals(dependency.getMaster(), dependenciesOf2.iterator().next().getDependency());

        // check that both dependant1 and dependant2 are found as dependants of the dependency
        final DerefMap.DependantRecordIdsIterator dependants =
                derefMap.findDependantsOf(dependency, dependencyField);
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
        final Set<DerefMap.DependencyEntry> dependencies = new HashSet<DerefMap.DependencyEntry>();
        dependencies.add(new DerefMap.DependencyEntry(ids.newRecordId("id1"), new HashSet<String>()));
        dependencies
                .add(new DerefMap.DependencyEntry(ids.newRecordId("id2", ImmutableMap.of("bar", "x")), Sets.newHashSet(
                        "foo")));

        final Set<DerefMap.DependencyEntry> deserialized = derefMap.deserializeDependenciesForward(
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

    @Test
    public void resultIndependentOfOrder() throws Exception {
        for (int i = 0; i < 10; i++) {
            final SchemaId dummyVtag = ids.getSchemaId(UUID.randomUUID());
            final SchemaId dependencyField = ids.getSchemaId(UUID.randomUUID());

            final RecordId master = ids.newRecordId();
            final RecordId var1 = ids.newRecordId(master, Collections.singletonMap("prop1", "x"));
            final RecordId var2 = ids.newRecordId(master, map("prop1", "x", "prop2", "y"));

            Set<SchemaId> fields = Sets.newHashSet(dependencyField);

            Map<DerefMap.DependencyEntry, Set<SchemaId>> dependencies = Maps.newHashMap();
            dependencies.put(new DerefMap.DependencyEntry(master), fields);
            dependencies.put(new DerefMap.DependencyEntry(var2), fields);

            derefMap.updateDependencies(var1, dummyVtag, dependencies);

            Set<RecordId> recordIds = asRecordIds(derefMap.findDependantsOf(var2, null));
            assertEquals("Iteration " + i, Sets.newHashSet(var1), recordIds);
        }
    }

    @Test
    public void twoVTagsDependingOnOneRecord() throws Exception {
        final SchemaId tag1 = ids.getSchemaId(UUID.randomUUID());
        final SchemaId tag2 = ids.getSchemaId(UUID.randomUUID());
        final SchemaId field = ids.getSchemaId(UUID.randomUUID());
        final Set<SchemaId> fields = Sets.newHashSet(field);

        final RecordId a = ids.newRecordId();
        final RecordId b = ids.newRecordId();

        derefMap.updateDependencies(a, tag1, Collections.singletonMap(new DependencyEntry(b), fields));

        // TODO: query on tag1
        assertEquals(Sets.newHashSet(a), asRecordIds(derefMap.findDependantsOf(b, field)));

        derefMap.updateDependencies(a, tag2, Collections.singletonMap(new DependencyEntry(b), fields));

        // TODO: query on tag2
        assertEquals(Sets.newHashSet(a), asRecordIds(derefMap.findDependantsOf(b, field)));
    }

    private Set<RecordId> asRecordIds(DerefMap.DependantRecordIdsIterator iter) throws IOException {
        Set<RecordId> result = Sets.newHashSet();
        while (iter.hasNext()) {
            result.add(iter.next());
        }
        return result;
    }

    private Map<String, String> map(String... keyOrValue) {
        Map<String, String> map = Maps.newHashMap();
        for (int i = 0; i < keyOrValue.length; i += 2) {
            map.put(keyOrValue[i], keyOrValue[i + 1]);
        }
        return map;
    }

}
