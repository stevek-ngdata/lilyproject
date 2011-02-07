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


import static org.junit.Assert.assertEquals;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.impl.HBaseTypeManager;
import org.lilyproject.rowlog.api.RowLogMessageListenerMapping;
import org.lilyproject.rowlog.api.RowLogSubscription.Type;
import org.lilyproject.testfw.TestHelper;
import org.lilyproject.util.io.Closer;

public class HBaseRepositoryTest extends AbstractRepositoryTest {

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging();
        repoSetup.setupCore();
        repoSetup.setupRepository(true);

        repoSetup.setupMessageQueue(true);

        idGenerator = repoSetup.getIdGenerator();
        repository = repoSetup.getRepository();
        typeManager = repoSetup.getTypeManager();

        setupTypes();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        repoSetup.stop();
    }
    
    @Test
    public void testFieldTypeCacheInitialization() throws Exception {
        TypeManager newTypeManager = new HBaseTypeManager(repoSetup.getIdGenerator(), repoSetup.getHadoopConf(),
                repoSetup.getZk(), repoSetup.getHbaseTableFactory());
        assertEquals(fieldType1, newTypeManager.getFieldTypeByName(fieldType1.getName()));
        Closer.close(newTypeManager);
    }
    
    @Test
    public void testUpdateProcessesRemainingMessages() throws Exception {
        HBaseRepositoryTestConsumer.reset();
        RowLogMessageListenerMapping.INSTANCE.put("TestSubscription", new HBaseRepositoryTestConsumer());
        repoSetup.getRowLogConfManager().addSubscription("WAL", "TestSubscription", Type.VM, 3, 2);
        repoSetup.waitForSubscription(repoSetup.getWal(), "TestSubscription");
        
        Record record = repository.newRecord();
        record.setRecordType(recordType1.getName(), recordType1.getVersion());
        record.setField(fieldType1.getName(), "value1");
        record = repository.create(record);
        record.setField(fieldType1.getName(), "value2");
        record = repository.update(record);

        assertEquals("value2", record.getField(fieldType1.getName()));

        assertEquals(record, repository.read(record.getId()));
        repoSetup.getRowLogConfManager().removeSubscription("WAL", "TestSubscription");
        RowLogMessageListenerMapping.INSTANCE.remove("TestSubscription");
    }
    
    
}
