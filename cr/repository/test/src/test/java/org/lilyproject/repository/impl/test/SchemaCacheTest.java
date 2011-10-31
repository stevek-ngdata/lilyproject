/*
 * Copyright 2011 Outerthought bvba
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
package org.lilyproject.repository.impl.test;

import java.util.ArrayList;
import java.util.List;

import org.junit.*;
import org.lilyproject.hadooptestfw.TestHelper;
import org.lilyproject.repository.api.*;
import org.lilyproject.repotestfw.RepositorySetup;

/**
 *
 */
public class SchemaCacheTest {
    private static final RepositorySetup repoSetup = new RepositorySetup();

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging();
        repoSetup.setupCore();
        repoSetup.setupTypeManager();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        repoSetup.stop();
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }



    @Test
    public void testRefresh() throws Exception {
        String namespace = "testRefresh";
        TypeManager typeManager = repoSetup.getTypeManager();
        TypeManager typeManager2 = repoSetup.getNewTypeManager();

        RecordType recordType1 = typeManager.recordTypeBuilder().defaultNamespace(namespace).name("recordType1")
                .fieldEntry().defineField().name("type1").create().add().create();

        Assert.assertEquals(recordType1, typeManager2.getRecordTypeByName(new QName(namespace, "recordType1"), null));
    }

    @Test
    public void testDisableRefresh() throws Exception {
        String namespace = "testDisableRefresh";
        TypeManager typeManager = repoSetup.getTypeManager();
        TypeManager typeManager2 = repoSetup.getNewTypeManager();

        // Disable the cache refreshing
        typeManager.disableSchemaCacheRefresh();

        // Give all type managers time to notice the disabling
        Thread.sleep(1000);

        // Check all type managers have the refreshing disabled
        Assert.assertFalse(typeManager2.isSchemaCacheRefreshEnabled());
        Assert.assertFalse(typeManager.isSchemaCacheRefreshEnabled());

        RecordType recordType1 = typeManager.recordTypeBuilder().defaultNamespace(namespace).name("recordType1")
                .fieldEntry().defineField().name("type1").create().add().create();

        try {
            typeManager2.getRecordTypeByName(new QName(namespace, "recordType1"), null);
            Assert
                    .fail("Did not expect typeManager2 to contain the record type since the cache refreshing is disabled");
        } catch (RecordTypeNotFoundException expected) {
        }

        // Force a cache refresh
        typeManager.triggerSchemaCacheRefresh();

        // Give all caches time to refresh
        Thread.sleep(1000);

        Assert.assertEquals(recordType1, typeManager2.getRecordTypeByName(new QName(namespace, "recordType1"), null));

        RecordType recordType2 = typeManager.recordTypeBuilder().defaultNamespace(namespace).name("recordType2")
                .fieldEntry().defineField().name("type2").create().add().create();

        typeManager.enableSchemaCacheRefresh();

        // Give all type managers time to notice the enabling
        Thread.sleep(1000);

        // Check all type managers have the refreshing enabled
        Assert.assertTrue(typeManager2.isSchemaCacheRefreshEnabled());
        Assert.assertTrue(typeManager.isSchemaCacheRefreshEnabled());

        // Assert that the record type created before enabling the cache
        // refreshing
        // is now seen. i.e. the cache of typeManager2 is refreshed
        Assert.assertEquals(recordType2, typeManager2.getRecordTypeByName(new QName(namespace, "recordType2"), null));

        RecordType recordType3 = typeManager.recordTypeBuilder().defaultNamespace(namespace).name("recordType3")
                .fieldEntry().defineField().name("type3").create().add().create();
        Assert.assertEquals(recordType3, typeManager2.getRecordTypeByName(new QName(namespace, "recordType3"), null));
    }

    // This test is mainly introduced to do some JProfiling
    // @Test
    public void testManyTypeManagers() throws Exception {
        String namespace = "testManyTypeManagers";
        final List<TypeManager> typeManagers = new ArrayList<TypeManager>();
        List<Thread> typeManagerThreads = new ArrayList<Thread>();
        for (int i = 0; i < 10; i++) {
            typeManagerThreads.add(new Thread() {
                public void run() {
                    try {
                        typeManagers.add(repoSetup.getNewTypeManager());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }
        for (Thread thread : typeManagerThreads) {
            thread.start();
        }
        for (Thread thread : typeManagerThreads) {
            thread.join();
        }

        Thread.sleep(1000);

        typeManagers.get(0).disableSchemaCacheRefresh();
        Thread.sleep(1000);

        RecordType recordType = null;
        for (int i = 0; i < 100; i++) {
            recordType = typeManagers.get(0).recordTypeBuilder().defaultNamespace(namespace).name("recordType" + i)
                    .fieldEntry().defineField().name("type" + i).create().add().create();
        }
        // Give all caches time to refresh
        typeManagers.get(0).enableSchemaCacheRefresh();
        Thread.sleep(1000);

        for (TypeManager typeManager : typeManagers) {
            Assert
                    .assertEquals(recordType, typeManager.getRecordTypeByName(new QName(namespace, "recordType99"),
                            null));
        }

    }

    // This test is introduced to do some profiling
    // @Test
    public void testManyTypesSameCache() throws Exception {
        String namespace = "testManyTypesSameCache";
        TypeManager typeManager = repoSetup.getTypeManager();

        long before = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            typeManager.recordTypeBuilder().defaultNamespace(namespace).name("recordType" + i).fieldEntry()
                    .defineField().name("fieldType" + i).create().add().create();
        }
        System.out.println("Creating 10000 record types and 10000 field types took: "
                + (System.currentTimeMillis() - before));
        for (int j = 0; j < 10000; j++) {
            typeManager.getFieldTypeByName(new QName(namespace, "fieldType" + j));
            typeManager.getRecordTypeByName(new QName(namespace, "recordType" + j), null);
        }
    }
}
