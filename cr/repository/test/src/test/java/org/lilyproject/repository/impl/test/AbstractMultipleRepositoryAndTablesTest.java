/*
 * Copyright 2013 NGDATA nv
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

import com.google.common.collect.Lists;
import org.junit.Test;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.LRepository;
import org.lilyproject.repository.api.LTable;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.RepositoryManager;
import org.lilyproject.repository.api.RepositoryTable;
import org.lilyproject.repository.api.TableManager;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.TableNotFoundException;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repotestfw.RepositorySetup;
import org.lilyproject.repository.model.api.RepositoryDefinition;
import org.lilyproject.repository.model.api.RepositoryExistsException;
import org.lilyproject.repository.model.api.RepositoryModel;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests related to multiple repositories and tables.
 */
public abstract class AbstractMultipleRepositoryAndTablesTest {
    protected static final RepositorySetup repoSetup = new RepositorySetup();
    protected static RepositoryManager repositoryManager;

    /**
     * Trying to get a non-existing repository should fail.
     */
    @Test(expected = RepositoryException.class)
    public void testRepositoryRegistrationIsRequired() throws Exception {
        repositoryManager.getRepository("funkyRepo");
    }

    /**
     * The default repository should exist without need to create it.
     */
    @Test
    public void testDefaultRepositoryExists() throws Exception {
        repositoryManager.getDefaultRepository();
    }

    /**
     * Trying to get a non-existing table should fail.
     */
    @Test(expected = TableNotFoundException.class)
    public void testTableCreationIsRequired() throws Exception {
        LRepository repository = repositoryManager.getDefaultRepository();
        repository.getTable("aNonExistingTable");
    }

    /**
     * Trying to get a non-existing table should fail.
     */
    @Test(expected = RepositoryExistsException.class)
    public void testCreateRepositoryTwice() throws Exception {
        RepositoryModel repositoryModel = repoSetup.getRepositoryModel();
        repositoryModel.create("somerepo");
        repositoryModel.create("somerepo");
    }

    /**
     * Tests that two repositories can have a table with the same name containing records with the
     * same id's, without conflict.
     */
    @Test
    public void testTwoRepositoriesSameTableSameId() throws Exception {
        RepositoryModel repositoryModel = repoSetup.getRepositoryModel();
        repositoryModel.create("company1");
        repositoryModel.create("company2");
        assertTrue(repositoryModel.waitUntilRepositoryInState("company1", RepositoryDefinition.RepositoryLifecycleState.ACTIVE, 60000L));
        assertTrue(repositoryModel.waitUntilRepositoryInState("company2", RepositoryDefinition.RepositoryLifecycleState.ACTIVE, 60000L));

        TypeManager typeMgr = repositoryManager.getDefaultRepository().getTypeManager();
        FieldType fieldType1 = typeMgr.createFieldType("STRING", new QName("test", "field1"), Scope.NON_VERSIONED);
        RecordType recordType1 = typeMgr.recordTypeBuilder()
                .name(new QName("test", "rt1"))
                .field(fieldType1.getId(), false)
                .create();

        List<String> repositories = Lists.newArrayList("company1", "company2", "default");

        for (String repoName : repositories) {
            LRepository repo = repositoryManager.getRepository(repoName);
            repo.getTableManager().createTable("mytable");
            LTable table = repo.getTable("mytable");
            table.recordBuilder()
                    .id("id1")
                    .recordType(recordType1.getName())
                    .field(fieldType1.getName(), repoName + "-value1")
                    .create();
        }

        for (String repoName : repositories) {
            LRepository repo = repositoryManager.getRepository(repoName);
            IdGenerator idGenerator = repo.getIdGenerator();
            LTable table = repo.getTable("mytable");
            assertEquals(repoName + "-value1", table.read(idGenerator.newRecordId("id1")).getField(fieldType1.getName()));
        }
    }

    @Test(expected = Exception.class)
    public void testRecordTableCannotBeDeleted() throws Exception {
        String repositoryName = "testRecordTableCannotBeDeleted";
        TableManager tableManager = repositoryManager.getRepository(repositoryName).getTableManager();
        tableManager.dropTable("record");
    }

    @Test
    public void testBasicTableManagement() throws Exception {
        String repositoryName = "tablemgmttest";
        RepositoryModel repositoryModel = repoSetup.getRepositoryModel();
        repositoryModel.create(repositoryName);
        assertTrue(repositoryModel.waitUntilRepositoryInState(repositoryName, RepositoryDefinition.RepositoryLifecycleState.ACTIVE, 60000L));

        LRepository repository = repositoryManager.getRepository(repositoryName);
        TableManager tableManager = repository.getTableManager();

        List<RepositoryTable> tables = tableManager.getTables();
        assertEquals(1, tables.size());
        assertEquals("record", tables.get(0).getName());

        tableManager.createTable("foo");

        assertEquals(2, tableManager.getTables().size());

        assertTrue(tableManager.tableExists("foo"));

        repository.getTable("foo");

        tableManager.dropTable("foo");

        assertEquals(1, tableManager.getTables().size());
    }
}
