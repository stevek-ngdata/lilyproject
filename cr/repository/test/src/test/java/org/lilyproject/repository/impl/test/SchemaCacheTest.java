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

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;
import org.lilyproject.hadooptestfw.TestHelper;
import org.lilyproject.repository.api.*;
import org.lilyproject.repository.impl.AbstractSchemaCache;
import org.lilyproject.repository.impl.SchemaIdImpl;
import org.lilyproject.repotestfw.RepositorySetup;

public class SchemaCacheTest {

    private static final RepositorySetup repoSetup = new RepositorySetup();

    private List<TypeManager> typeManagersToClose = new ArrayList<TypeManager>();
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
        for (TypeManager typeManager : typeManagersToClose) {
            typeManager.close();
        }
        typeManagersToClose.clear();
    }

    @Test
    public void testRefresh() throws Exception {
        String namespace = "testRefresh";
        TypeManager typeManager = repoSetup.getTypeManager();
        TypeManager typeManager2 = repoSetup.getNewTypeManager();
        typeManagersToClose.add(typeManager2);

        RecordType recordType1 = typeManager.recordTypeBuilder().defaultNamespace(namespace).name("recordType1")
                .fieldEntry().defineField().name("type1").create().add().create();

        RecordType rt = waitForRecordType(5000, new QName(namespace, "recordType1"), typeManager2);
        Assert.assertEquals(recordType1, rt);
    }


    @Test
    public void testDisableRefresh() throws Exception {
        String namespace = "testDisableRefresh";
        TypeManager typeManager = repoSetup.getTypeManager();
        TypeManager typeManager2 = repoSetup.getNewTypeManager();
        typeManagersToClose.add(typeManager2);

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

        Assert.assertEquals(recordType1, waitForRecordType(5000, new QName(namespace, "recordType1"), typeManager2));

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
        Assert.assertEquals(recordType2, waitForRecordType(5000, new QName(namespace, "recordType2"), typeManager2));

        RecordType recordType3 = typeManager.recordTypeBuilder().defaultNamespace(namespace).name("recordType3")
                .fieldEntry().defineField().name("type3").create().add().create();
        Assert.assertEquals(recordType3, waitForRecordType(5000, new QName(namespace, "recordType3"), typeManager2));
    }

    // This test is mainly introduced to do some JProfiling
    @Test
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
        typeManagersToClose.addAll(typeManagers);

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

        for (TypeManager typeManager : typeManagers) {
            Assert.assertEquals(recordType, waitForRecordType(5000, new QName(namespace, "recordType99"), typeManager));
        }
    }

    // This test is introduced to do some profiling
    @Test
    public void testManyTypes() throws Exception {
        String namespace = "testManyTypesSameCache";
        TypeManager typeManager = repoSetup.getTypeManager();

        // Add some extra type managers to simulate multiple caches that need to
        // be refreshed
        for (int i = 0; i < 10; i++) {
            typeManagersToClose.add(repoSetup.getNewTypeManager());
        }

        long total = 0;
        int iterations = 10;
        int nrOfTypes = 1000; // Set to a low number to reduce automated test
                             // time
        for (int i = 0; i < iterations; i++) {
            long before = System.currentTimeMillis();
            for (int j = 0; j < nrOfTypes; j++) {
                typeManager.recordTypeBuilder().defaultNamespace(namespace).name("recordType" + (i * nrOfTypes + j))
                        .fieldEntry().defineField().name("fieldType" + (i * nrOfTypes + j)).create().add().create();
            }
            long duration = (System.currentTimeMillis() - before);
            total += duration;
            System.out.println(i + " :Creating " + nrOfTypes + " record types and " + nrOfTypes + " field types took: "
                    + duration);
            // if (i == 5)
            // newTypeManager.close();
        }
        System.out.println("Creating " + (iterations * nrOfTypes) + " record types and " + (iterations * nrOfTypes)
                + " field types took: " + total);

        // Make sure all types are known by the typeManager
        for (int i = 0; i < iterations * nrOfTypes; i++) {
            typeManager.getFieldTypeByName(new QName(namespace, "fieldType" + i));
            typeManager.getRecordTypeByName(new QName(namespace, "recordType" + i), null);
        }

        long before = System.currentTimeMillis();
        for (int i = 0; i < 5; i++) {
            typeManager.recordTypeBuilder().defaultNamespace(namespace).name("extraRecordType" + i).fieldEntry()
                    .defineField().name("extraFieldType" + i).create().add().create();
        }
        System.out.println("Creating 5 extra record types and 5 extra field types took: "
                + (System.currentTimeMillis() - before));
        
        for (TypeManager tm : typeManagersToClose) {
            waitForRecordType(10000, new QName(namespace, "recordType" + ((iterations * nrOfTypes) - 1)), tm);
        }
    }

    private RecordType waitForRecordType(long timeout, QName name, TypeManager typeManager2)
            throws RepositoryException, InterruptedException {
        long before = System.currentTimeMillis();
        while (System.currentTimeMillis() < before + timeout) {
            try {
                return typeManager2.getRecordTypeByName(name, null);
            } catch (RecordTypeNotFoundException e) {
                // continue
            }
        }
        throw new RecordTypeNotFoundException(name, null);
    }

    private FieldType waitForFieldType(long timeout, QName name, TypeManager typeManager2) throws RepositoryException,
            InterruptedException {
        long before = System.currentTimeMillis();
        while (System.currentTimeMillis() < before + timeout) {
            try {
                return typeManager2.getFieldTypeByName(name);
            } catch (FieldTypeNotFoundException e) {
                // continue
            }
        }
        throw new FieldTypeNotFoundException(name);
    }

    private static Random random = new Random();
    private static final byte[] CF = Bytes.toBytes("cf");
    private static final byte[] C1 = Bytes.toBytes("c1");
    private static final byte[] C2 = Bytes.toBytes("c2");
    private static final byte[] C3 = Bytes.toBytes("c3");

    private byte[] putRandomRecord(HTableInterface table) throws IOException {
        SchemaId id = new SchemaIdImpl(UUID.randomUUID());
        byte[] rowId = id.getBytes();
        Put put = new Put(rowId);
        put.add(CF, C1, Bytes.toBytes(random.nextInt()));
        put.add(CF, C2, Bytes.toBytes(random.nextInt()));
        put.add(CF, C3, Bytes.toBytes(random.nextInt()));
        table.put(put);
        return rowId;
    }

    private void scanBucket(HTableInterface table) throws IOException {
        byte[] rowPrefix = new byte[1];
        byte[] decodeHexAndNextHex = AbstractSchemaCache.decodeHexAndNextHex(AbstractSchemaCache.encodeHex(rowPrefix));
        random.nextBytes(rowPrefix);
        Scan scan = new Scan(rowPrefix);
        scan.setStopRow(new byte[] { decodeHexAndNextHex[1] });
        scan.addColumn(CF, C1);
        scan.addColumn(CF, C2);
        scan.addColumn(CF, C3);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            result.getRow();
        }
    }

    private class ScanThread extends Thread {
        private final int count;
        private final HTableInterface table;
        private final String name;

        public ScanThread(String name, int count, HTableInterface table) {
            this.name = name;
            this.count = count;
            this.table = table;
        }

        @Override
        public void run() {
            long before = System.currentTimeMillis();
            for (int i = 0; i < count; i++) {
                try {
                    scanBucket(table);
                } catch (IOException e) {
                    throw new RuntimeException();
                }
            }
            System.out.println("Scanner " + name + ", count="+count+": " + (System.currentTimeMillis() - before));
        }
    }
}
