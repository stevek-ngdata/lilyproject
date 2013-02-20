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
import org.junit.Test;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repotestfw.RepositorySetup;
import org.lilyproject.hadooptestfw.TestHelper;

import static org.junit.Assert.assertEquals;

public class TypeManagerRecordTypeTest extends AbstractTypeManagerRecordTypeTest {

    private static final RepositorySetup repoSetup = new RepositorySetup();

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging();
        repoSetup.setupCore();
        repoSetup.setupTypeManager();
        typeManager = repoSetup.getTypeManager();
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


    // The refreshSubtypes tests (except for one) are only in the 'local' test variant, because otherwise it would
    // require lots of waiting on the remote schema cache to be up to date and I didn't want to pollute the code
    // with that. If one day we have an easier solution to this, we can move these test up to
    // AbstractTypeManagerRecordTypeTest again
    @Test
    public void testRefreshSubtypesLoops() throws Exception {
        // The following code creates this type hierarchy:
        //
        //     rtA---
        //      |   |
        //     rtB  |
        //      |   |
        //     rtC<-|
        //

        RecordType rtA = typeManager.recordTypeBuilder()
                .name("RefreshSubtypesLoops", "rtA")
                .fieldEntry().use(fieldType1).add()
                .create();

        RecordType rtB = typeManager.recordTypeBuilder()
                .name("RefreshSubtypesLoops", "rtB")
                .fieldEntry().use(fieldType1).add()
                .supertype().use(rtA).add()
                .create();

        RecordType rtC = typeManager.recordTypeBuilder()
                .name("RefreshSubtypesLoops", "rtC")
                .fieldEntry().use(fieldType1).add()
                .supertype().use(rtB).add()
                .create();

        rtA = typeManager.recordTypeBuilder()
                .name("RefreshSubtypesLoops", "rtA")
                .fieldEntry().use(fieldType1).add()
                .supertype().use(rtC).add()
                .update();

        // Update record type A, this should update B & C but don't go in an endless loop
        rtA.addFieldTypeEntry(fieldType2.getId(), false);
        rtA = typeManager.updateRecordType(rtA, true);

        // Check the subtypes were updated
        rtB = typeManager.getRecordTypeById(rtB.getId(), null);
        rtC = typeManager.getRecordTypeById(rtC.getId(), null);

        assertEquals(Long.valueOf(3L), rtB.getSupertypes().get(rtA.getId()));
        assertEquals(Long.valueOf(2L), rtC.getSupertypes().get(rtB.getId()));
    }

    @Test
    public void testRefreshSubtypesCreateOrUpdate() throws Exception {
        // This test verifies that the refreshSubtypes flag also works for createOrUpdateRecordType
        //
        // The following code creates this type hierarchy:
        //
        //     rtA
        //      |
        //     rtB

        RecordType rtA = typeManager.recordTypeBuilder()
                .name("RefreshSubtypesCreateOrUpdate", "rtA")
                .fieldEntry().use(fieldType1).add()
                .create();

        RecordType rtB = typeManager.recordTypeBuilder()
                .name("RefreshSubtypesCreateOrUpdate", "rtB")
                .fieldEntry().use(fieldType1).add()
                .supertype().use(rtA).add()
                .create();

        assertEquals(Long.valueOf(1L), rtB.getSupertypes().get(rtA.getId()));

        // Update record type A, pointer in record type B should be updated
        rtA.addFieldTypeEntry(fieldType2.getId(), false);
        rtA = typeManager.updateRecordType(rtA, true);

        // Now B should point to new version of A
        rtB = typeManager.getRecordTypeById(rtB.getId(), null);
        assertEquals(Long.valueOf(2L), rtB.getSupertypes().get(rtA.getId()));
    }

    @Test
    public void testRefreshNoSubtypes() throws Exception {
        // This test verifies that the refreshSubtypes flag also works for createOrUpdateRecordType
        //
        // The following code creates this type hierarchy:
        //
        //     rtA
        //      |
        //     rtB

        RecordType rtA = typeManager.recordTypeBuilder()
                .name("RefreshNoSubtypes", "rtA")
                .fieldEntry().use(fieldType1).add()
                .create();

        RecordType rtB = typeManager.recordTypeBuilder()
                .name("RefreshNoSubtypes", "rtB")
                .fieldEntry().use(fieldType1).add()
                .supertype().use(rtA).add()
                .create();

        assertEquals(Long.valueOf(1L), rtB.getSupertypes().get(rtA.getId()));

        // Update record type A, pointer in record type B should NOT be updated
        rtA.addFieldTypeEntry(fieldType2.getId(), false);
        rtA = typeManager.updateRecordType(rtA, false);

        // Verify B still points to old version of A
        rtB = typeManager.getRecordTypeById(rtB.getId(), null);
        assertEquals(Long.valueOf(1L), rtB.getSupertypes().get(rtA.getId()));

        // Do the same without explicit refreshSubtypes argument
        rtA.addFieldTypeEntry(fieldType3.getId(), false);
        rtA = typeManager.updateRecordType(rtA);

        // Verify B still points to old version of A
        rtB = typeManager.getRecordTypeById(rtB.getId(), null);
        assertEquals(Long.valueOf(1L), rtB.getSupertypes().get(rtA.getId()));
    }

    @Test
    public void testRefreshSubtypesBuilder() throws Exception {
        // This test verifies that the refreshSubtypes flag also works for when using the builder
        //
        // The following code creates this type hierarchy:
        //
        //     rtA
        //      |
        //     rtB

        RecordType rtA = typeManager.recordTypeBuilder()
                .name("RefreshSubtypesBuilder", "rtA")
                .fieldEntry().use(fieldType1).add()
                .create();

        RecordType rtB = typeManager.recordTypeBuilder()
                .name("RefreshSubtypesBuilder", "rtB")
                .fieldEntry().use(fieldType1).add()
                .supertype().use(rtA).add()
                .create();

        assertEquals(Long.valueOf(1L), rtB.getSupertypes().get(rtA.getId()));

        // Update record type A, pointer in record type B should be updated
        rtA = typeManager.recordTypeBuilder()
                .name("RefreshSubtypesBuilder", "rtA")
                .fieldEntry().use(fieldType1).add()
                .fieldEntry().use(fieldType2).add()
                .update(true);

        // Now B should point to new version of A
        rtB = typeManager.getRecordTypeById(rtB.getId(), null);
        assertEquals(Long.valueOf(2L), rtB.getSupertypes().get(rtA.getId()));
    }
}
