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
package org.lilyproject.indexer.derefmap;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilyproject.hadooptestfw.TestHelper;
import org.lilyproject.repository.api.AbsoluteRecordId;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.LRepository;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.impl.id.AbsoluteRecordIdImpl;
import org.lilyproject.repotestfw.RepositorySetup;
import org.lilyproject.util.hbase.LilyHBaseSchema.Table;
import org.lilyproject.util.io.Closer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * This tests the functionality of the {@link DerefMapHbaseImpl}. Note there is also a DerefMapIndexTest which
 * tests it in real indexing scenario's.
 *
 *
 */
public class DerefMapBasicTest {
    private final static RepositorySetup repoSetup = new RepositorySetup();

    private static IdGenerator ids;
    private static DerefMapHbaseImpl derefMap;

    private static int nextIdPrefix = 0;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging("org.lilyproject.indexer");

        repoSetup.setupCore();
        repoSetup.setupRepository();

        LRepository repository = repoSetup.getRepositoryManager().getPublicRepository();
        ids = repository.getIdGenerator();

        derefMap = (DerefMapHbaseImpl) DerefMapHbaseImpl.create("test", repoSetup.getHadoopConf(), null, ids);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        Closer.close(repoSetup);
    }

    private static AbsoluteRecordId absId(RecordId recordId) {
        return new AbsoluteRecordIdImpl(Table.RECORD.name, recordId);
    }

    @Test
    public void emptyDependencies() throws Exception {
        String idPrefix = newIdPrefix();

        final SchemaId dummyVtag = ids.getSchemaId(UUID.randomUUID());
        final SchemaId dummyField = ids.getSchemaId(UUID.randomUUID());
        final RecordId id1 = ids.newRecordId(idPrefix + "id1");

        final HashMap<DependencyEntry, Set<SchemaId>> empty =
                new HashMap<DependencyEntry, Set<SchemaId>>();
        derefMap.updateDependants(absId(id1), dummyVtag, empty);

        // consistency check
        final Set<DependencyEntry> found = derefMap.findDependencies(absId(id1), dummyVtag);
        assertTrue(found.isEmpty());

        final DependantRecordIdsIterator dependants = derefMap.findDependantsOf(absId(id1), dummyField, dummyVtag);
        assertFalse(dependants.hasNext());
    }

    @Test
    public void oneDependency() throws Exception {
        String idPrefix = newIdPrefix();

        final SchemaId dummyVtag = ids.getSchemaId(UUID.randomUUID());
        final SchemaId dependencyField = ids.getSchemaId(UUID.randomUUID());
        final SchemaId anotherField = ids.getSchemaId(UUID.randomUUID());
        final RecordId dependant = ids.newRecordId(idPrefix + idPrefix + "dependant");
        final RecordId dependency = ids.newRecordId(idPrefix + idPrefix + "dependency");
        final RecordId dependencyAfterUpdate = ids.newRecordId(idPrefix + idPrefix + "dependencyAfterUpdate");

        // the dependant depends on the dependencyField of the dependency
        final HashMap<DependencyEntry, Set<SchemaId>> dependencies =
                new HashMap<DependencyEntry, Set<SchemaId>>();
        dependencies.put(new DependencyEntry(absId(dependency)), Sets.newHashSet(dependencyField));
        derefMap.updateDependants(absId(dependant), dummyVtag, dependencies);

        // consistency check
        final Set<DependencyEntry> found = derefMap.findDependencies(absId(dependant), dummyVtag);
        assertEquals(1, found.size());
        assertEquals(absId(dependency), found.iterator().next().getDependency());

        // check that the dependant is found as only dependant of the dependency via the dependencyField
        DependantRecordIdsIterator dependants =
                derefMap.findDependantsOf(absId(dependency), dependencyField, dummyVtag);
        assertTrue(dependants.hasNext());
        assertEquals(absId(dependant), dependants.next());
        assertFalse(dependants.hasNext());

        // idem but with a set of fields among which the dependencyField
        dependants = derefMap.findDependantsOf(absId(dependency), Sets.newHashSet(dependencyField, anotherField), dummyVtag);
        assertTrue(dependants.hasNext());
        assertEquals(absId(dependant), dependants.next());
        assertFalse(dependants.hasNext());

        // check that nothing is found as dependency of the dependant
        assertFalse(derefMap.findDependantsOf(absId(dependant), dependencyField, dummyVtag).hasNext());

        // check that nothing is found as dependency of the dependency via another field than the dependencyField
        assertFalse(derefMap.findDependantsOf(absId(dependency), anotherField, dummyVtag).hasNext());

        // now update the dependency to be from the dependant to the dependencyAfterUpdate (via the same field)
        final HashMap<DependencyEntry, Set<SchemaId>> updatedDependencies =
                new HashMap<DependencyEntry, Set<SchemaId>>();
        updatedDependencies
                .put(new DependencyEntry(absId(dependencyAfterUpdate)), Sets.newHashSet(dependencyField));
        derefMap.updateDependants(absId(dependant), dummyVtag, updatedDependencies);

        // consistency check
        final Set<DependencyEntry> foundAfterUpdate = derefMap.findDependencies(absId(dependant), dummyVtag);
        assertEquals(1, foundAfterUpdate.size());
        assertEquals(absId(dependencyAfterUpdate), foundAfterUpdate.iterator().next().getDependency());

        // check that the dependant is found as only dependant of the dependencyAfterUpdate via the dependencyField
        final DependantRecordIdsIterator dependantsAfterUpdate =
                derefMap.findDependantsOf(absId(dependencyAfterUpdate), dependencyField, dummyVtag);
        assertTrue(dependantsAfterUpdate.hasNext());
        assertEquals(absId(dependant), dependantsAfterUpdate.next());
        assertFalse(dependantsAfterUpdate.hasNext());

        // check that nothing is found any longer as dependency on the previous dependency (from before the update)
        assertFalse(derefMap.findDependantsOf(absId(dependency), dependencyField, dummyVtag).hasNext());
    }

    @Test
    public void oneDependencyWithMoreDimensionedVariants() throws Exception {
        String idPrefix = newIdPrefix();

        final SchemaId dummyVtag = ids.getSchemaId(UUID.randomUUID());
        final RecordId dependant = ids.newRecordId(idPrefix + "master", ImmutableMap.of("bar", "x"));
        final RecordId dependency = ids.newRecordId(idPrefix + "master", ImmutableMap.of("bar", "x", "foo", "y"));

        // the dependant depends on the dependencyField of the dependency via a "+foo" dereferencing rule
        final HashMap<DependencyEntry, Set<SchemaId>> dependencies =
                new HashMap<DependencyEntry, Set<SchemaId>>();
        dependencies.put(new DependencyEntry(absId(dependency),
                ImmutableSet.of("foo")), Sets.<SchemaId>newHashSet());
        derefMap.updateDependants(absId(dependant), dummyVtag, dependencies);

        // consistency check
        final Set<DependencyEntry> found = derefMap.findDependencies(absId(dependant), dummyVtag);
        assertEquals(1, found.size());
        final DependencyEntry foundDependencyEntry = found.iterator().next();
        assertEquals(absId(ids.newRecordId(idPrefix + "master", ImmutableMap.of("bar", "x"))), foundDependencyEntry.getDependency());
        assertEquals(Sets.newHashSet("foo"), foundDependencyEntry.getMoreDimensionedVariants());

        // check that the dependant is found as only dependant of the dependency (without specifying a field)
        DependantRecordIdsIterator dependants = derefMap.findDependantsOf(absId(dependency));
        assertTrue(dependants.hasNext());
        assertEquals(absId(dependant), dependants.next());
        assertFalse(dependants.hasNext());

        // check that other records (which would in reality not yet exist at index time) that match the "+foo" rule
        // are returned as dependants of our dependant (such that in reality reindexation of the dependant happens)

        final RecordId shouldTriggerOurDependant =
                ids.newRecordId(idPrefix + "master", ImmutableMap.of("bar", "x", "foo", "another-value"));
        dependants = derefMap.findDependantsOf(absId(shouldTriggerOurDependant));
        assertTrue(dependants.hasNext());
        assertEquals(absId(dependant), dependants.next());
        assertFalse(dependants.hasNext());

        // doesn't have the foo property
        final RecordId shouldNotTriggerOurDependant1 = ids.newRecordId(idPrefix + "master", ImmutableMap.of("bar", "x"));
        assertFalse(derefMap.findDependantsOf(absId(shouldNotTriggerOurDependant1)).hasNext());

        // doesn't have the bar property
        final RecordId shouldNotTriggerOurDependant2 = ids.newRecordId(idPrefix + "master", ImmutableMap.of("foo", "x"));
        assertFalse(derefMap.findDependantsOf(absId(shouldNotTriggerOurDependant2)).hasNext());

        // wrong value for the bar property
        final RecordId shouldNotTriggerOurDependant3 =
                ids.newRecordId(idPrefix + "master", ImmutableMap.of("bar", "y", "foo", "another-value"));
        assertFalse(derefMap.findDependantsOf(absId(shouldNotTriggerOurDependant3)).hasNext());

        // additional unmatched property
        final RecordId shouldNotTriggerOurDependant4 =
                ids.newRecordId(idPrefix + "master", ImmutableMap.of("bar", "x", "foo", "another-value", "baz", "z"));
        assertFalse(derefMap.findDependantsOf(absId(shouldNotTriggerOurDependant4)).hasNext());

        // another master
        final RecordId shouldNotTriggerOurDependant5 =
                ids.newRecordId(idPrefix + "another-master", ImmutableMap.of("bar", "x", "foo", "another-value"));
        assertFalse(derefMap.findDependantsOf(absId(shouldNotTriggerOurDependant5)).hasNext());

        // wrong properties
        final RecordId shouldNotTriggerOurDependant6 = ids.newRecordId(idPrefix + "master", ImmutableMap.of("a", "b", "c", "d"));
        assertFalse(derefMap.findDependantsOf(absId(shouldNotTriggerOurDependant6)).hasNext());

        // no properties
        final RecordId shouldNotTriggerOurDependant7 = ids.newRecordId(idPrefix + "master", ImmutableMap.<String, String>of());
        assertFalse(derefMap.findDependantsOf(absId(shouldNotTriggerOurDependant7)).hasNext());
    }

    /**
     * Simulates what would happen in case of a dereference expression like n:link1=>n:link2=>n:field.
     */
    @Test
    public void chainOfDependencies() throws Exception {
        String idPrefix = newIdPrefix();

        final SchemaId dummyVtag = ids.getSchemaId(UUID.randomUUID());
        final SchemaId linkField1 = ids.getSchemaId(UUID.randomUUID());
        final SchemaId linkField2 = ids.getSchemaId(UUID.randomUUID());
        final SchemaId field = ids.getSchemaId(UUID.randomUUID());
        final RecordId dependant = ids.newRecordId(idPrefix + "dependant");
        final RecordId dependency1 = ids.newRecordId(idPrefix + "dependency1");
        final RecordId dependency2 = ids.newRecordId(idPrefix + "dependency2");

        // scenario: dependant has linkField1 -> dependency1 which has linkField2 -> dependency2 which has field "field"
        final Map<DependencyEntry, Set<SchemaId>> dependencies =
                new HashMap<DependencyEntry, Set<SchemaId>>();
        // 1) dependant depends on dependency1 from which it uses linkField2
        dependencies.put(new DependencyEntry(absId(dependency1)), Sets.newHashSet(linkField2));
        // 2) dependant depends on dependency2 from which it uses field
        dependencies.put(new DependencyEntry(absId(dependency2)), Sets.newHashSet(field));
        derefMap.updateDependants(absId(dependant), dummyVtag, dependencies);

        // consistency check
        final Set<DependencyEntry> found = derefMap.findDependencies(absId(dependant), dummyVtag);
        assertEquals(2, found.size());

        // check that the dependant is found as only dependant of the dependencies via the corresponding fields
        DependantRecordIdsIterator viaDependency1AndLinkField1 =
                derefMap.findDependantsOf(absId(dependency1), linkField1, dummyVtag);
        assertFalse(viaDependency1AndLinkField1.hasNext());

        DependantRecordIdsIterator viaDependency2AndLinkField1 =
                derefMap.findDependantsOf(absId(dependency2), linkField1, dummyVtag);
        assertFalse(viaDependency2AndLinkField1.hasNext());

        DependantRecordIdsIterator viaDependency1AndLinkField2 =
                derefMap.findDependantsOf(absId(dependency1), linkField2, dummyVtag);
        assertTrue(viaDependency1AndLinkField2.hasNext());
        assertEquals(absId(dependant), viaDependency1AndLinkField2.next());
        assertFalse(viaDependency1AndLinkField2.hasNext());

        DependantRecordIdsIterator viaDependency2AndLinkField2 =
                derefMap.findDependantsOf(absId(dependency2), linkField2, dummyVtag);
        assertFalse(viaDependency2AndLinkField2.hasNext());

        DependantRecordIdsIterator viaDependency2AndField =
                derefMap.findDependantsOf(absId(dependency2), field, dummyVtag);
        assertTrue(viaDependency2AndField.hasNext());
        assertEquals(absId(dependant), viaDependency2AndField.next());
        assertFalse(viaDependency2AndField.hasNext());

        DependantRecordIdsIterator viaDependency1WithoutSpecifyingField =
                derefMap.findDependantsOf(absId(dependency1));
        assertTrue(viaDependency1WithoutSpecifyingField.hasNext());
        assertEquals(absId(dependant), viaDependency1WithoutSpecifyingField.next());
        assertFalse(viaDependency1WithoutSpecifyingField.hasNext());

        DependantRecordIdsIterator viaDependency2WithoutSpecifyingField =
                derefMap.findDependantsOf(absId(dependency2));
        assertTrue(viaDependency2WithoutSpecifyingField.hasNext());
        assertEquals(absId(dependant), viaDependency2WithoutSpecifyingField.next());
        assertFalse(viaDependency2WithoutSpecifyingField.hasNext());
    }

    /**
     * Simulates what would happen in case of a dereference expression like n:link1=>n:link2=>n:link3.
     */
    @Test
    public void chainOfDependenciesWhichDoesNotEndInField() throws Exception {
        String idPrefix = newIdPrefix();

        final SchemaId dummyVtag = ids.getSchemaId(UUID.randomUUID());
        final SchemaId linkField1 = ids.getSchemaId(UUID.randomUUID());
        final SchemaId linkField2 = ids.getSchemaId(UUID.randomUUID());
        final SchemaId linkField3 = ids.getSchemaId(UUID.randomUUID());
        final RecordId dependant = ids.newRecordId(idPrefix + "dependant");
        final RecordId dependency1 = ids.newRecordId(idPrefix + "dependency1");
        final RecordId dependency2 = ids.newRecordId(idPrefix + "dependency2");
        final RecordId dependency3 = ids.newRecordId(idPrefix + "dependency3");

        // scenario: dependant has linkField1 -> dependency1 which has linkField2 -> dependency2 which has linkField3 -> dependency3
        final Map<DependencyEntry, Set<SchemaId>> dependencies =
                new HashMap<DependencyEntry, Set<SchemaId>>();
        // 1) dependant depends on dependency1 from which it uses linkField2
        dependencies.put(new DependencyEntry(absId(dependency1)),
                Sets.newHashSet(linkField2));
        // 2) dependant depends on dependency2 from which it uses linkField3 which points to dependency3
        dependencies.put(new DependencyEntry(absId(dependency2)),
                Sets.newHashSet(linkField3));
        derefMap.updateDependants(absId(dependant), dummyVtag, dependencies);

        // consistency check
        final Set<DependencyEntry> found = derefMap.findDependencies(absId(dependant), dummyVtag);
        assertEquals(2, found.size());

        // check that the dependant is found as only dependant of the dependencies via the corresponding fields
        DependantRecordIdsIterator viaDependency1AndLinkField2 =
                derefMap.findDependantsOf(absId(dependency1), linkField2, dummyVtag);
        assertTrue(viaDependency1AndLinkField2.hasNext());
        assertEquals(absId(dependant), viaDependency1AndLinkField2.next());
        assertFalse(viaDependency1AndLinkField2.hasNext());

        DependantRecordIdsIterator viaDependency2AndLinkField3 =
                derefMap.findDependantsOf(absId(dependency2), linkField3, dummyVtag);
        assertTrue(viaDependency2AndLinkField3.hasNext());
        assertEquals(absId(dependant), viaDependency2AndLinkField3.next());
        assertFalse(viaDependency2AndLinkField3.hasNext());
    }

    /**
     * Simulates what would happen in case of a dereference expression like +prop1=>n:link1=>+prop2=>n:field.
     */
    @Test
    public void chainOfDependenciesIncludingMoreDimensionedVariantProperties() throws Exception {
        String idPrefix = newIdPrefix();

        final SchemaId dummyVtag = ids.getSchemaId(UUID.randomUUID());
        final SchemaId linkField1 = ids.getSchemaId(UUID.randomUUID());
        final SchemaId field = ids.getSchemaId(UUID.randomUUID());
        final RecordId dependant = ids.newRecordId(idPrefix + "dependant");
        final RecordId dependantWithProp1 = ids.newRecordId(idPrefix + "dependant", ImmutableMap.of("prop1", "x"));
        final RecordId dependency1 = ids.newRecordId(idPrefix + "dependency1");
        final RecordId dependency1WithProp2 = ids.newRecordId(idPrefix + "dependency1", ImmutableMap.of("prop2", "y"));

        // scenario: dependant depends on all dependant +prop1 records. One such a record (dependantWithProp1) exists
        // with a linkField1 pointing to dependency1. Via the +prop2 rule, we end up with all dependency1 + prop2
        // records, of which there is one instance (dependency1WithProp2). Of this instance, we use the field "field".
        final Map<DependencyEntry, Set<SchemaId>> dependencies =
                new HashMap<DependencyEntry, Set<SchemaId>>();
        // 1) dependant depends on all similar with prop1, of which it uses linkField1
        dependencies
                .put(new DependencyEntry(absId(dependant), ImmutableSet.of("prop1")), Sets.newHashSet(linkField1));
        // 2) dependant depends on dependency1WithProp2 (and all similar with prop2) from which it uses field "field"
        dependencies.put(new DependencyEntry(absId(dependency1WithProp2), ImmutableSet.of("prop2")),
                Sets.newHashSet(field));
        derefMap.updateDependants(absId(dependant), dummyVtag, dependencies);

        // consistency check
        final Set<DependencyEntry> found = derefMap.findDependencies(absId(dependant), dummyVtag);
        assertEquals(2, found.size());

        // check that the dependant is found as only dependant of the dependencies via the corresponding fields (in a few scenarios)

        // scenario1: as if a dependency1 with prop2=value is being created
        final RecordId someRecordLikeDependency1WithProp2 =
                ids.newRecordId(idPrefix + "dependency1", ImmutableMap.of("prop2", "value"));
        DependantRecordIdsIterator scenario1 =
                derefMap.findDependantsOf(absId(someRecordLikeDependency1WithProp2), field, dummyVtag);
        assertTrue(scenario1.hasNext());
        assertEquals(absId(dependant), scenario1.next());
        assertFalse(scenario1.hasNext());

        // scenario2: as if a new record like dependant is created with prop1=value
        final RecordId someRecordLikeDependantWithProp1 =
                ids.newRecordId(idPrefix + "dependant", ImmutableMap.of("prop1", "value"));
        DependantRecordIdsIterator scenario2 =
                derefMap.findDependantsOf(absId(someRecordLikeDependantWithProp1), linkField1, dummyVtag);
        assertTrue(scenario2.hasNext());
        assertEquals(absId(dependant), scenario2.next());
        assertFalse(scenario2.hasNext());
    }

    @Test
    public void multipleDependencies() throws Exception {
        String idPrefix = newIdPrefix();

        final SchemaId dummyVtag = ids.getSchemaId(UUID.randomUUID());
        final SchemaId dependencyField = ids.getSchemaId(UUID.randomUUID());
        final RecordId dependant = ids.newRecordId(idPrefix + "dependant");
        final RecordId dependency1 = ids.newRecordId(idPrefix + "dependency1");
        final RecordId dependency2 = ids.newRecordId(idPrefix + "dependency2");

        // the dependant depends on the dependencyField of the dependency1 and dependency2
        final HashMap<DependencyEntry, Set<SchemaId>> dependencies =
                new HashMap<DependencyEntry, Set<SchemaId>>();
        dependencies.put(new DependencyEntry(absId(dependency1)), Sets.newHashSet(dependencyField));
        dependencies.put(new DependencyEntry(absId(dependency2)), Sets.newHashSet(dependencyField));
        derefMap.updateDependants(absId(dependant), dummyVtag, dependencies);

        // consistency check
        final Set<DependencyEntry> found = derefMap.findDependencies(absId(dependant), dummyVtag);
        assertEquals(2, found.size());

        // check that the dependant is found as only dependant of the dependency1 via the dependencyField
        final DependantRecordIdsIterator dependantsOf1 =
                derefMap.findDependantsOf(absId(dependency1), dependencyField, dummyVtag);
        assertTrue(dependantsOf1.hasNext());
        assertEquals(absId(dependant), dependantsOf1.next());
        assertFalse(dependantsOf1.hasNext());

        // check that the dependant is also found as only dependant of the dependency2 via the dependencyField
        final DependantRecordIdsIterator dependantsOf2 =
                derefMap.findDependantsOf(absId(dependency2), dependencyField, dummyVtag);
        assertTrue(dependantsOf2.hasNext());
        assertEquals(absId(dependant), dependantsOf2.next());
        assertFalse(dependantsOf2.hasNext());
    }

    @Test
    public void multipleVariantsDependingOnMaster() throws Exception {
        String idPrefix = newIdPrefix();

        final SchemaId dummyVtag = ids.getSchemaId(UUID.randomUUID());
        final SchemaId dependencyField = ids.getSchemaId(UUID.randomUUID());

        final RecordId master = ids.newRecordId(idPrefix + "myrecord");
        final RecordId v1variant = ids.newRecordId(idPrefix + "myrecord", Collections.singletonMap("v1", "x"));
        final RecordId v1v2variant = ids.newRecordId(idPrefix + "myrecord", map("v1", "x", "v2", "y"));

        final HashMap<DependencyEntry, Set<SchemaId>> dependencies =
                new HashMap<DependencyEntry, Set<SchemaId>>();
        dependencies.put(new DependencyEntry(absId(master)), Sets.newHashSet(dependencyField));
        derefMap.updateDependants(absId(v1variant), dummyVtag, dependencies);
        derefMap.updateDependants(absId(v1v2variant), dummyVtag, dependencies);

        Set<AbsoluteRecordId> recordIds = asRecordIds(derefMap.findDependantsOf(absId(master)));
        assertEquals(recordIds, Sets.newHashSet(absId(v1variant), absId(v1v2variant)));
    }

    @Test
    public void multipleDependants() throws Exception {
        String idPrefix = newIdPrefix();

        final SchemaId dummyVtag = ids.getSchemaId(UUID.randomUUID());
        final SchemaId dependencyField = ids.getSchemaId(UUID.randomUUID());
        final RecordId dependant1 = ids.newRecordId(idPrefix + "dependant1");
        final RecordId dependant2 = ids.newRecordId(idPrefix + "dependant2");
        final RecordId dependency = ids.newRecordId(idPrefix + "dependency");

        // the dependant1 and dependant2 depend on the dependencyField of the dependency
        final HashMap<DependencyEntry, Set<SchemaId>> dependencies =
                new HashMap<DependencyEntry, Set<SchemaId>>();
        dependencies.put(new DependencyEntry(absId(dependency)),
                Sets.newHashSet(dependencyField));
        derefMap.updateDependants(absId(dependant1), dummyVtag, dependencies);
        derefMap.updateDependants(absId(dependant2), dummyVtag, dependencies);

        // consistency check dependant1
        final Set<DependencyEntry> dependenciesOf1 = derefMap.findDependencies(absId(dependant1), dummyVtag);
        assertEquals(1, dependenciesOf1.size());
        assertEquals(absId(dependency.getMaster()), dependenciesOf1.iterator().next().getDependency());

        // consistency check dependant2
        final Set<DependencyEntry> dependenciesOf2 = derefMap.findDependencies(absId(dependant1), dummyVtag);
        assertEquals(1, dependenciesOf2.size());
        assertEquals(absId(dependency.getMaster()), dependenciesOf2.iterator().next().getDependency());

        // check that both dependant1 and dependant2 are found as dependants of the dependency
        final DependantRecordIdsIterator dependants =
                derefMap.findDependantsOf(absId(dependency), dependencyField, dummyVtag);
        assertTrue(dependants.hasNext());
        final AbsoluteRecordId firstFoundDependant = dependants.next();
        assertTrue(dependants.hasNext());
        final AbsoluteRecordId secondFoundDependant = dependants.next();
        assertFalse(dependants.hasNext());

        // check that the two found dependants are dependant1 and dependant2 (order doesn't matter)
        assertTrue((absId(dependant1).equals(firstFoundDependant) && absId(dependant2).equals(secondFoundDependant)) ||
                (absId(dependant2).equals(firstFoundDependant) && absId(dependant1).equals(secondFoundDependant)));
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

            Map<DependencyEntry, Set<SchemaId>> dependencies = Maps.newHashMap();
            dependencies.put(new DependencyEntry(absId(master)), fields);
            dependencies.put(new DependencyEntry(absId(var2)), fields);

            derefMap.updateDependants(absId(var1), dummyVtag, dependencies);

            Set<AbsoluteRecordId> recordIds = asRecordIds(derefMap.findDependantsOf(absId(var2)));
            assertEquals("Iteration " + i, Sets.newHashSet(absId(var1)), recordIds);
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

        derefMap.updateDependants(absId(a), tag1,
                Collections.singletonMap(new DependencyEntry(absId(b)), fields));

        assertEquals(Sets.newHashSet(absId(a)), asRecordIds(derefMap.findDependantsOf(absId(b), field, tag1)));
        assertEquals(Sets.newHashSet(), asRecordIds(derefMap.findDependantsOf(absId(b), field, tag2)));

        derefMap.updateDependants(absId(a), tag2,
                Collections.singletonMap(new DependencyEntry(absId(b)), fields));

        assertEquals(Sets.newHashSet(absId(a)), asRecordIds(derefMap.findDependantsOf(
                                                absId(b), field, tag1)));
        assertEquals(Sets.newHashSet(absId(a)), asRecordIds(derefMap.findDependantsOf(
                                                absId(b), field, tag2)));
    }

    private Set<AbsoluteRecordId> asRecordIds(DependantRecordIdsIterator iter) throws IOException {
        Set<AbsoluteRecordId> result = Sets.newHashSet();
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

    private String newIdPrefix() {
        return String.format("TEST%3d", nextIdPrefix++);
    }

}
