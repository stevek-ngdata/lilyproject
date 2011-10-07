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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilyproject.hbaseindex.IndexManager;
import org.lilyproject.linkindex.FieldedLink;
import org.lilyproject.linkindex.LinkIndex;
import org.lilyproject.linkindex.LinkIndexUpdater;
import org.lilyproject.repository.api.*;
import org.lilyproject.repository.impl.*;
import org.lilyproject.repotestfw.RepositorySetup;
import org.lilyproject.rowlog.api.RowLogMessageListenerMapping;
import org.lilyproject.rowlog.api.RowLogSubscription;
import org.lilyproject.hadooptestfw.TestHelper;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.repo.VersionTag;

public class LinkIndexTest {
    private final static RepositorySetup repoSetup = new RepositorySetup();

    private static TypeManager typeManager;
    private static Repository repository;
    private static IdGenerator ids;
    private static LinkIndex linkIndex;

    private SchemaId field1 = new SchemaIdImpl(UUID.randomUUID());

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging("org.lilyproject.linkindex", "org.lilyproject.rowlog.impl.RowLogImpl");

        repoSetup.setupCore();
        repoSetup.setupRepository(true);

        typeManager = repoSetup.getTypeManager();
        repository = repoSetup.getRepository();
        ids = repository.getIdGenerator();

        IndexManager indexManager = new IndexManager(repoSetup.getHadoopConf());

        linkIndex = new LinkIndex(indexManager, repository);

        repoSetup.getRowLogConfManager().addSubscription("WAL", "LinkIndexUpdater", RowLogSubscription.Type.VM, 1);
        RowLogMessageListenerMapping.INSTANCE.put("LinkIndexUpdater", new LinkIndexUpdater(repository, linkIndex));

        repoSetup.waitForSubscription(repoSetup.getWal(), "LinkIndexUpdater");
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        Closer.close(repoSetup);
    }

    @Test
    public void testLinkIndex() throws Exception {
        SchemaId liveTag = repository.getIdGenerator().getSchemaId(UUID.randomUUID());
        
        Set<FieldedLink> links1 = new HashSet<FieldedLink>();
        links1.add(new FieldedLink(ids.newRecordId("id1"), field1));
        links1.add(new FieldedLink(ids.newRecordId("id2"), field1));

        Set<FieldedLink> links2 = new HashSet<FieldedLink>();
        links2.add(new FieldedLink(ids.newRecordId("id3"), field1));
        links2.add(new FieldedLink(ids.newRecordId("id4"), field1));

        linkIndex.updateLinks(ids.newRecordId("idA"), liveTag, links1);
        linkIndex.updateLinks(ids.newRecordId("idB"), liveTag, links1);
        linkIndex.updateLinks(ids.newRecordId("idC"), liveTag, links2);

        // Test forward link retrieval
        Set<FieldedLink> links = linkIndex.getFieldedForwardLinks(ids.newRecordId("idA"), liveTag);
        assertTrue(links.contains(new FieldedLink(ids.newRecordId("id1"), field1)));
        assertTrue(links.contains(new FieldedLink(ids.newRecordId("id2"), field1)));
        assertEquals(2, links.size());

        // Test backward link retrieval
        Set<RecordId> referrers = linkIndex.getReferrers(ids.newRecordId("id1"), liveTag);
        assertTrue(referrers.contains(ids.newRecordId("idA")));
        assertTrue(referrers.contains(ids.newRecordId("idB")));
        assertEquals(2, referrers.size());

        // Update the links for record idA and re-check
        links1.add(new FieldedLink(ids.newRecordId("id2a"), field1));
        linkIndex.updateLinks(ids.newRecordId("idA"), liveTag, links1);

        links = linkIndex.getFieldedForwardLinks(ids.newRecordId("idA"), liveTag);
        assertTrue(links.contains(new FieldedLink(ids.newRecordId("id1"), field1)));
        assertTrue(links.contains(new FieldedLink(ids.newRecordId("id2"), field1)));
        assertTrue(links.contains(new FieldedLink(ids.newRecordId("id2a"), field1)));
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

            Set<RecordId> referrers = linkIndex.getReferrers(ids.newRecordId("foo1"), lastVTag);
            assertEquals(1, referrers.size());
            assertTrue(referrers.contains(record.getId()));

            referrers = linkIndex.getReferrers(ids.newRecordId("bar1"), lastVTag);
            assertEquals(0, referrers.size());

            // Now perform an update so that there is a version
            record.setField(versionedFt.getName(), Arrays.asList(new Link(ids.newRecordId("foo2")),
                    new Link(ids.newRecordId("foo3"))));
            record = repository.update(record);

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
                    .defaultNameSpace("ns")
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
                    .defaultNameSpace("ns")
                    .recordType("MyRecordType")
                    .field("complexLinks",
                            Arrays.asList(
                                    repository
                                            .recordBuilder()
                                            .defaultNameSpace("ns")
                                            .recordType("MyRecordType")
                                            .field("link1", new Link(ids.newRecordId("cl1")))
                                            .newRecord(),
                                    repository
                                            .recordBuilder()
                                            .defaultNameSpace("ns")
                                            .recordType("MyRecordType")
                                            .field("link1", new Link(ids.newRecordId("cl2")))
                                            .newRecord()
                            ))
                    .create();

            Set<RecordId> referrers = linkIndex.getReferrers(ids.newRecordId("cl1"), lastVTag);
            assertEquals(1, referrers.size());
            assertTrue(referrers.contains(record.getId()));

            Set<RecordId> forwardLinks = linkIndex.getForwardLinks(record.getId(), lastVTag, complexFt.getId());
            assertEquals(2, forwardLinks.size());
            assertTrue(forwardLinks.contains(ids.newRecordId("cl1")));
            assertTrue(forwardLinks.contains(ids.newRecordId("cl2")));
        }
    }
}
