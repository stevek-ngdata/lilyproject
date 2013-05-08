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
package org.lilyproject.linkindex.test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.Sets;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilyproject.hadooptestfw.TestHelper;
import org.lilyproject.hbaseindex.IndexManager;
import org.lilyproject.linkindex.FieldedLink;
import org.lilyproject.linkindex.LinkIndex;
import org.lilyproject.linkindex.LinkIndexUpdater;
import org.lilyproject.repository.api.AbsoluteRecordId;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.HierarchyPath;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.Link;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.impl.id.SchemaIdImpl;
import org.lilyproject.repotestfw.RepositorySetup;
import org.lilyproject.util.hbase.LilyHBaseSchema.Table;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.repo.VersionTag;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LinkIndexTest {

    private static final String TABLE_A = "tableA";
    private static final String TABLE_B = "tableB";
    private static final String TABLE_C = "tableC";

    private final static RepositorySetup repoSetup = new RepositorySetup();

    private static TypeManager typeManager;
    private static Repository repository;
    private static IdGenerator ids;
    private static LinkIndex linkIndex;

    private SchemaId field1 = new SchemaIdImpl(UUID.randomUUID());

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging("org.lilyproject.linkindex");

        repoSetup.setupCore();
        repoSetup.setupRepository();

        typeManager = repoSetup.getTypeManager();
        repository = (Repository)repoSetup.getRepositoryManager().getDefaultTable();
        ids = repository.getIdGenerator();

        IndexManager indexManager = new IndexManager(repoSetup.getHadoopConf());

        linkIndex = new LinkIndex(indexManager, repoSetup.getRepositoryManager());

        repoSetup.getSepModel().addSubscription("LinkIndexUpdater");
        repoSetup.getTableManager().createTable(TABLE_A);
        repoSetup.getTableManager().createTable(TABLE_B);
        repoSetup.getTableManager().createTable(TABLE_C);

        repoSetup.startSepEventSlave("LinkIndexUpdater", new LinkIndexUpdater(repoSetup.getRepositoryManager(), linkIndex));
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        Closer.close(repoSetup);
    }

    @Test
    public void testLinkIndex() throws Exception {
        SchemaId liveTag = repository.getIdGenerator().getSchemaId(UUID.randomUUID());

        Set<FieldedLink> links1 = new HashSet<FieldedLink>();
        links1.add(new FieldedLink(createAbsoluteId("id1"), field1));
        links1.add(new FieldedLink(createAbsoluteId("id2"), field1));

        Set<FieldedLink> links2 = new HashSet<FieldedLink>();
        links2.add(new FieldedLink(createAbsoluteId("id3"), field1));
        links2.add(new FieldedLink(createAbsoluteId("id4"), field1));

        linkIndex.updateLinks(ids.newRecordId("idA"), liveTag, links1);
        linkIndex.updateLinks(ids.newRecordId("idB"), liveTag, links1);
        linkIndex.updateLinks(ids.newRecordId("idC"), liveTag, links2);

        // Test forward link retrieval
        Set<FieldedLink> links = linkIndex.getFieldedForwardLinks(ids.newRecordId("idA"), liveTag);
        assertTrue(links.contains(new FieldedLink(createAbsoluteId("id1"), field1)));
        assertTrue(links.contains(new FieldedLink(createAbsoluteId("id2"), field1)));
        assertEquals(2, links.size());

        // Test backward link retrieval
        Set<RecordId> referrers = linkIndex.getReferrers(ids.newRecordId("id1"), liveTag);
        assertTrue(referrers.contains(ids.newRecordId("idA")));
        assertTrue(referrers.contains(ids.newRecordId("idB")));
        assertEquals(2, referrers.size());

        // Update the links for record idA and re-check
        links1.add(new FieldedLink(createAbsoluteId("id2a"), field1));
        linkIndex.updateLinks(ids.newRecordId("idA"), liveTag, links1);

        links = linkIndex.getFieldedForwardLinks(ids.newRecordId("idA"), liveTag);
        assertTrue(links.contains(new FieldedLink(createAbsoluteId("id1"), field1)));
        assertTrue(links.contains(new FieldedLink(createAbsoluteId("id2"), field1)));
        assertTrue(links.contains(new FieldedLink(createAbsoluteId("id2a"), field1)));
        assertEquals(3, links.size());

        referrers = linkIndex.getReferrers(ids.newRecordId("id1"), liveTag);
        assertTrue(referrers.contains(ids.newRecordId("idA")));
        assertTrue(referrers.contains(ids.newRecordId("idB")));
        assertEquals(2, referrers.size());

        referrers = linkIndex.getReferrers(ids.newRecordId("id2a"), liveTag);
        assertTrue(referrers.contains(ids.newRecordId("idA")));
        assertEquals(1, referrers.size());
    }

    @Test
    public void testLinkIndex_AcrossTables() throws Exception {
        SchemaId liveTag = repository.getIdGenerator().getSchemaId(UUID.randomUUID());

        Set<FieldedLink> links1 = new HashSet<FieldedLink>();
        links1.add(new FieldedLink(ids.newAbsoluteRecordId(TABLE_A, "id1"), field1));
        links1.add(new FieldedLink(ids.newAbsoluteRecordId(TABLE_A, "id2"), field1));

        Set<FieldedLink> links2 = new HashSet<FieldedLink>();
        links2.add(new FieldedLink(ids.newAbsoluteRecordId(TABLE_B, "id3"), field1));
        links2.add(new FieldedLink(ids.newAbsoluteRecordId(TABLE_B, "id4"), field1));

        linkIndex.updateLinks(ids.newAbsoluteRecordId(TABLE_A, "idA"), liveTag, links1);
        linkIndex.updateLinks(ids.newAbsoluteRecordId(TABLE_B, "idB"), liveTag, links1);
        linkIndex.updateLinks(ids.newAbsoluteRecordId(TABLE_C, "idC"), liveTag, links2);

        // Test forward link retrieval
        Set<FieldedLink> links = linkIndex.getFieldedForwardLinks(ids.newAbsoluteRecordId(TABLE_A, "idA"), liveTag);
        assertEquals(2, links.size());
        assertEquals(
                Sets.newHashSet(
                        new FieldedLink(ids.newAbsoluteRecordId(TABLE_A, "id1"), field1),
                        new FieldedLink(ids.newAbsoluteRecordId(TABLE_A, "id2"), field1)),
                links);

        // Test backward link retrieval - non-absolute ids
        Set<AbsoluteRecordId> referrers = linkIndex.getAbsoluteReferrers(ids.newAbsoluteRecordId(TABLE_A, "id1"), liveTag);
        assertEquals(2, referrers.size());
        assertEquals(
                Sets.newHashSet(
                        ids.newAbsoluteRecordId(TABLE_A, "idA"),
                        ids.newAbsoluteRecordId(TABLE_B, "idB")),
                referrers);

        // Test backward link retrieval - absolute ids
        Set<AbsoluteRecordId> absoluteReferrers = linkIndex.getAbsoluteReferrers(ids.newAbsoluteRecordId(TABLE_A, "id1"), liveTag);
        assertTrue(absoluteReferrers.contains(ids.newAbsoluteRecordId(TABLE_A, "idA")));
        assertTrue(absoluteReferrers.contains(ids.newAbsoluteRecordId(TABLE_B, "idB")));
        assertEquals(2, absoluteReferrers.size());
    }

    @Test
    public void testLinkIndexWithShortRecordIds() throws Exception {
        final RecordId id1 = ids.newRecordId("id1");
        final RecordId id2 = ids.newRecordId("id2");
        final RecordId id3 = ids.newRecordId("id3");
        final RecordId id4 = ids.newRecordId("id4");

        testLinkIndexRetrievalWithProvidedIds(id1, id2, id3, id4);
    }

    @Test
    public void testLinkIndexWithLongRecordIds() throws Exception {
        final RecordId id1 = ids.newRecordId("thisIsARecordIdWhichIsMuchLongerThanTenBytes1");
        final RecordId id2 = ids.newRecordId("thisIsARecordIdWhichIsMuchLongerThanTenBytes2");
        final RecordId id3 = ids.newRecordId("thisIsARecordIdWhichIsMuchLongerThanTenBytes3");
        final RecordId id4 = ids.newRecordId("thisIsARecordIdWhichIsMuchLongerThanTenBytes4");

        testLinkIndexRetrievalWithProvidedIds(id1, id2, id3, id4);
    }

    private void testLinkIndexRetrievalWithProvidedIds(RecordId id1, RecordId id2, RecordId id3, RecordId id4) throws Exception {
        SchemaId liveTag = repository.getIdGenerator().getSchemaId(UUID.randomUUID());

        Set<FieldedLink> links1 = new HashSet<FieldedLink>();
        links1.add(new FieldedLink(createAbsoluteId(id1), field1));
        links1.add(new FieldedLink(createAbsoluteId(id2), field1));

        Set<FieldedLink> links2 = new HashSet<FieldedLink>();
        links2.add(new FieldedLink(createAbsoluteId(id3), field1));
        links2.add(new FieldedLink(createAbsoluteId(id4), field1));

        linkIndex.updateLinks(ids.newRecordId("idA"), liveTag, links1);
        linkIndex.updateLinks(ids.newRecordId("idB"), liveTag, links2);

        // Test forward link retrieval
        Set<FieldedLink> linksFromA = linkIndex.getFieldedForwardLinks(ids.newRecordId("idA"), liveTag);
        assertTrue(linksFromA.contains(new FieldedLink(createAbsoluteId(id1), field1)));
        assertTrue(linksFromA.contains(new FieldedLink(createAbsoluteId(id2), field1)));
        assertEquals(2, linksFromA.size());

        Set<FieldedLink> linksFromB = linkIndex.getFieldedForwardLinks(ids.newRecordId("idB"), liveTag);
        assertTrue(linksFromB.contains(new FieldedLink(createAbsoluteId(id3), field1)));
        assertTrue(linksFromB.contains(new FieldedLink(createAbsoluteId(id4), field1)));
        assertEquals(2, linksFromB.size());

        // Test backward link retrieval
        Set<RecordId> referrers1 = linkIndex.getReferrers(id1, liveTag);
        assertTrue(referrers1.contains(ids.newRecordId("idA")));
        assertEquals(1, referrers1.size());

        Set<RecordId> referrers2 = linkIndex.getReferrers(id2, liveTag);
        assertTrue(referrers2.contains(ids.newRecordId("idA")));
        assertEquals(1, referrers2.size());

        Set<RecordId> referrers3 = linkIndex.getReferrers(id3, liveTag);
        assertTrue(referrers3.contains(ids.newRecordId("idB")));
        assertEquals(1, referrers3.size());

        Set<RecordId> referrers4 = linkIndex.getReferrers(id4, liveTag);
        assertTrue(referrers4.contains(ids.newRecordId("idB")));
        assertEquals(1, referrers4.size());
    }

    @Test
    public void testLinkIndexUpdater() throws Exception {
        FieldType nonVersionedFt = typeManager.newFieldType(typeManager.getValueType("LINK"),
                new QName("ns", "link1"), Scope.NON_VERSIONED);
        nonVersionedFt = typeManager.createFieldType(nonVersionedFt);

        FieldType versionedFt = typeManager.newFieldType(typeManager.getValueType("LIST<LINK>"),
                new QName("ns", "link2"), Scope.VERSIONED);
        versionedFt = typeManager.createFieldType(versionedFt);

        FieldType versionedMutableFt = typeManager.newFieldType(typeManager.getValueType("LIST<LINK>"),
                new QName("ns", "link3"), Scope.VERSIONED_MUTABLE);
        versionedMutableFt = typeManager.createFieldType(versionedMutableFt);

        FieldType nestedFt = typeManager.newFieldType(typeManager.getValueType("LIST<LIST<PATH<LINK>>>"),
                new QName("ns", "nestedLinks"), Scope.NON_VERSIONED);
        nestedFt = typeManager.createFieldType(nestedFt);

        FieldType complexFt = typeManager.newFieldType(typeManager.getValueType("LIST<RECORD>"),
                new QName("ns", "complexLinks"), Scope.NON_VERSIONED);
        complexFt = typeManager.createFieldType(complexFt);

        RecordType recordType = typeManager.newRecordType(new QName("ns", "MyRecordType"));
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(nonVersionedFt.getId(), false));
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(versionedFt.getId(), false));
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(versionedMutableFt.getId(), false));
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(nestedFt.getId(), false));
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(complexFt.getId(), false));
        recordType = typeManager.createRecordType(recordType);

        SchemaId lastVTag = typeManager.getFieldTypeByName(VersionTag.LAST).getId();

        //
        // Link extraction from a record without versions
        //
        {
            Record record = repository.newRecord();
            record.setRecordType(recordType.getName());
            record.setField(nonVersionedFt.getName(), new Link(ids.newRecordId("foo1")));
            record = repository.create(record);
            repoSetup.waitForSepProcessing();

            Set<RecordId> referrers = linkIndex.getReferrers(ids.newRecordId("foo1"), lastVTag);
            assertEquals(1, referrers.size());
            assertTrue(referrers.contains(record.getId()));

            referrers = linkIndex.getReferrers(ids.newRecordId("bar1"), lastVTag);
            assertEquals(0, referrers.size());

            // Now perform an update so that there is a version
            record.setField(versionedFt.getName(), Arrays.asList(new Link(ids.newRecordId("foo2")),
                    new Link(ids.newRecordId("foo3"))));
            record = repository.update(record);
            repoSetup.waitForSepProcessing();

            referrers = linkIndex.getReferrers(ids.newRecordId("foo1"), lastVTag);
            assertEquals(1, referrers.size());
            assertTrue(referrers.contains(record.getId()));

            referrers = linkIndex.getReferrers(ids.newRecordId("foo2"), lastVTag);
            assertEquals(1, referrers.size());
            assertTrue(referrers.contains(record.getId()));
        }

        //
        // Link extraction from nested types
        //
        {
            Record record = repository
                    .recordBuilder()
                    .defaultNamespace("ns")
                    .recordType("MyRecordType")
                    .field("nestedLinks",
                            Arrays.asList(
                                    Arrays.asList(
                                            new HierarchyPath(new Link(ids.newRecordId("nl1"))),
                                            new HierarchyPath(new Link(ids.newRecordId("nl2")))
                                    ),
                                    Arrays.asList(
                                            new HierarchyPath(new Link(ids.newRecordId("nl3"))),
                                            new HierarchyPath(new Link(ids.newRecordId("nl4")))
                                    )
                            ))
                    .create();
            repoSetup.waitForSepProcessing();

            Set<RecordId> referrers = linkIndex.getReferrers(ids.newRecordId("nl1"), lastVTag);
            assertEquals(1, referrers.size());
            assertTrue(referrers.contains(record.getId()));

            Set<RecordId> forwardLinks = linkIndex.getForwardLinks(record.getId(), lastVTag, nestedFt.getId());
            assertEquals(4, forwardLinks.size());
            assertTrue(forwardLinks.contains(ids.newRecordId("nl1")));
            assertTrue(forwardLinks.contains(ids.newRecordId("nl2")));
            assertTrue(forwardLinks.contains(ids.newRecordId("nl3")));
            assertTrue(forwardLinks.contains(ids.newRecordId("nl4")));
        }

        //
        // Link extraction from complex types
        //
        {
            Record record = repository
                    .recordBuilder()
                    .defaultNamespace("ns")
                    .recordType("MyRecordType")
                    .field("complexLinks",
                            Arrays.asList(
                                    repository
                                            .recordBuilder()
                                            .defaultNamespace("ns")
                                            .recordType("MyRecordType")
                                            .field("link1", new Link(ids.newRecordId("cl1")))
                                            .build(),
                                    repository
                                            .recordBuilder()
                                            .defaultNamespace("ns")
                                            .recordType("MyRecordType")
                                            .field("link1", new Link(ids.newRecordId("cl2")))
                                            .build()
                            ))
                    .create();
            repoSetup.waitForSepProcessing();

            Set<RecordId> referrers = linkIndex.getReferrers(ids.newRecordId("cl1"), lastVTag);
            assertEquals(1, referrers.size());
            assertTrue(referrers.contains(record.getId()));

            Set<RecordId> forwardLinks = linkIndex.getForwardLinks(record.getId(), lastVTag, complexFt.getId());
            assertEquals(2, forwardLinks.size());
            assertTrue(forwardLinks.contains(ids.newRecordId("cl1")));
            assertTrue(forwardLinks.contains(ids.newRecordId("cl2")));
        }
    }

    private AbsoluteRecordId createAbsoluteId(String recordIdString) {
        return createAbsoluteId(ids.newRecordId(recordIdString));
    }

    private AbsoluteRecordId createAbsoluteId(RecordId recordId) {
        return ids.newAbsoluteRecordId(Table.RECORD.name, recordId);
    }
}
