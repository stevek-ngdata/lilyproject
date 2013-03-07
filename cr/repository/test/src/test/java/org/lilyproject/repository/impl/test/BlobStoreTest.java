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
package org.lilyproject.repository.impl.test;


import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.lilyproject.hadooptestfw.TestHelper;
import org.lilyproject.repository.impl.BlobStoreAccessRegistry;
import org.lilyproject.util.hbase.LilyHBaseSchema.Table;

public class BlobStoreTest extends AbstractBlobStoreTest {

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging();
        repoSetup.setupCore();
        repoSetup.setupRepository();

        repository = repoSetup.getRepositoryManager().getRepository(Table.RECORD.name);
        typeManager = repoSetup.getTypeManager();
        blobManager = repoSetup.getBlobManager();

        // Create a blobStoreAccessRegistry for testing purposes
        testBlobStoreAccessRegistry = new BlobStoreAccessRegistry(blobManager);
        testBlobStoreAccessRegistry.setBlobStoreAccessFactory(repoSetup.getBlobStoreAccessFactory());
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


}
