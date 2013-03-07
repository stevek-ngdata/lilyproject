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
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repotestfw.RepositorySetup;
import org.lilyproject.hadooptestfw.TestHelper;

/**
 *
 */
public class AvroTypeManagerRecordTypeTest extends AbstractTypeManagerRecordTypeTest {

    private static final RepositorySetup repoSetup = new RepositorySetup();

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging();
        repoSetup.setupCore();
        repoSetup.setupRepository();
        repoSetup.setupRemoteAccess();
        typeManager = repoSetup.getRemoteTypeManager();
        setupFieldTypes();
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

    protected void waitOnRecordTypeVersion(long version, SchemaId recordTypeId)
            throws InterruptedException, RepositoryException {
        long tryUntil = System.currentTimeMillis() + 20000;
        long currentVersion;
        while ((currentVersion = typeManager.getRecordTypeById(recordTypeId, null).getVersion()) != version) {
            if (System.currentTimeMillis() > tryUntil)
                throw new RuntimeException("RecordType was not updated to expected version within time, " +
                        "expected version: " + version + ", current version = " + currentVersion);
            Thread.sleep(20);
        }
    }
}
