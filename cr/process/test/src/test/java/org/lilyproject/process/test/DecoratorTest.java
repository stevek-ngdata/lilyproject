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
package org.lilyproject.process.test;

import java.io.File;
import java.util.IdentityHashMap;

import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.lilyproject.client.LilyClient;
import org.lilyproject.lilyservertestfw.LilyProxy;
import org.lilyproject.process.decorator.TestRepositoryDecoratorFactory;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.LRepository;
import org.lilyproject.repository.api.LTable;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.model.impl.RepositoryModelImpl;
import org.lilyproject.server.modules.repository.DecoratingRepositoryManager;
import org.lilyproject.server.modules.repository.RepositoryDecoratorChain;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.lilyproject.repository.model.api.RepositoryDefinition.RepositoryLifecycleState;

/**
 * Tests the repository decorator feature.
 *
 * <p>This test relies on test-decorator being installed in the local maven repository, hence
 * requires that the build is done with "mvn install".</p>
 */
public class DecoratorTest {

    @Test
    public void test() throws Exception {
        LilyProxy lilyProxy = createLilyProxy();

        LilyClient client = lilyProxy.getLilyServerProxy().getClient();

        RepositoryModelImpl repositoryModel = new RepositoryModelImpl(lilyProxy.getLilyServerProxy().getZooKeeper());
        repositoryModel.create("repo1");
        assertTrue(repositoryModel.waitUntilRepositoryInState("repo1", RepositoryLifecycleState.ACTIVE, 60000L));
        repositoryModel.create("repo2");
        assertTrue(repositoryModel.waitUntilRepositoryInState("repo2", RepositoryLifecycleState.ACTIVE, 60000L));
        repositoryModel.close();

        // Make a schema
        TypeManager typeMgr = client.getDefaultRepository().getTypeManager();

        QName field1 = new QName("ns", "f1");
        FieldType fieldType1 = typeMgr.newFieldType(typeMgr.getValueType("STRING"), field1, Scope.NON_VERSIONED);
        fieldType1 = typeMgr.createFieldType(fieldType1);

        QName field2 = new QName("ns", "f2");
        FieldType fieldType2 = typeMgr.newFieldType(typeMgr.getValueType("STRING"), field2, Scope.NON_VERSIONED);
        fieldType2 = typeMgr.createFieldType(fieldType2);

        QName typeName = new QName("ns", "rt1");
        RecordType recordType = typeMgr.newRecordType(typeName);
        recordType.addFieldTypeEntry(fieldType1.getId(), false);
        recordType.addFieldTypeEntry(fieldType2.getId(), false);
        recordType = typeMgr.createRecordType(recordType);

        DecoratingRepositoryManager repositoryMgr = (DecoratingRepositoryManager)lilyProxy.getLilyServerProxy()
                .getLilyServerTestingUtility().getRuntime().getModuleById("repository")
                .getApplicationContext().getBean("repositoryManager");
        IdentityHashMap<Object, Object> chains = new IdentityHashMap<Object, Object>();

        // Test the decorator is applied for each repository and each table
        for (String repositoryName : new String[] {"default", "repo1", "repo2"}) {
            LRepository repository = client.getRepository(repositoryName);

            repository.getTableManager().createTable("table1");
            repository.getTableManager().createTable("table2");

            for (String tableName : new String[] {"record", "table1", "table2"}) {
                LTable table = repository.getTable(tableName);
                Record record = table.newRecord();
                record.setRecordType(typeName);
                record.setField(field1, "foobar");
                record = table.create(record);
                assertEquals("foo", record.getField(field2));
                assertEquals("foo", table.read(record.getId()).getField(field2));

                // Test we can get access to our test decorator: this is something that is occasionally useful
                // in test cases to check certain conditions, e.g. if the decorator would have async side effects.
                RepositoryDecoratorChain chain = repositoryMgr.getRepositoryDecoratorChain(repositoryName, tableName);
                assertNotNull(chain);
                assertNotNull(chain.getDecorator(TestRepositoryDecoratorFactory.NAME));
                chains.put(chain.getDecorator(TestRepositoryDecoratorFactory.NAME), null);
                assertEquals(2, chain.getEntries().size());
            }
        }

        // There should be one instance of the decorator created for each repository-table combination (that
        // was accessed)
        assertEquals(3 * 3, chains.size());

        // Check that if we ask the same table twice, we get the same instance (verifies cache works)
        assertTrue(client.getRepository("repo1").getTable("table1") == client.getRepository("repo1").getTable("table1"));

        lilyProxy.stop();

        // Check each of the decorators was properly closed
        assertEquals(9, TestRepositoryDecoratorFactory.CLOSE_COUNT.get());
    }

    private static LilyProxy createLilyProxy() throws Exception {
        String basedir = System.getProperty("basedir");
        if (basedir == null) {
            basedir = System.getProperty("java.io.tmpdir");
        }

        // Temp dir where we will create plugin & conf stuff
        File tmpDir = new File(basedir + "/target/lily-test");

        String pluginDir = setupPluginsDirectory(tmpDir);
        System.setProperty("lily.plugin.dir", pluginDir);


        File customConfDir = setupConfDirectory(tmpDir);
        System.setProperty("lily.conf.dir", basedir + "/../server/conf");
        System.setProperty("lily.conf.customdir", customConfDir.getAbsolutePath());

        try {
            LilyProxy lilyProxy = new LilyProxy();
            lilyProxy.start();
            return lilyProxy;
        } finally {
            // Make sure it's properties won't be used by later-running tests
            System.getProperties().remove("lily.plugin.dir");
            System.getProperties().remove("lily.conf.dir");
            System.getProperties().remove("lily.conf.customdir");
        }
    }

    private static String setupPluginsDirectory(File tmpDir) throws Exception {
        File pluginDir = new File(tmpDir + "/plugins");

        // Delete it if it would already exist
        FileUtils.deleteDirectory(pluginDir);

        FileUtils.forceMkdir(pluginDir);

        File beforeRepoDir = new File(pluginDir, "load-before-repository");
        FileUtils.forceMkdir(beforeRepoDir);

        String projectVersion = System.getProperty("project.version");
        if (projectVersion == null) {
            throw new Exception("This test relies on a system property project.version being set. Probably you " +
                    "are running this test outside of Maven? Try adding -Dproject.version=...");
        }

        String wiringXml = "<wiring>\n" +
                "  <modules>\n" +
                "    <artifact id=\"test-decorator\" groupId=\"org.lilyproject\" artifactId=\"lily-test-decorator\" version=\"" + projectVersion + "\">\n" +
                "    </artifact>\n" +
                "  </modules>\n" +
                "</wiring>";

        FileUtils.writeStringToFile(new File(beforeRepoDir, "testdecorator.xml"), wiringXml, "UTF-8");

        return pluginDir.getAbsolutePath();
    }

    private static File setupConfDirectory(File tmpDir) throws Exception {
        File confDir = new File(tmpDir + "/conf");

        File repoConfDir = new File(confDir, "repository");
        FileUtils.forceMkdir(repoConfDir);

        // Write configuration to activate the decorator
        String repositoryXml = "<repository xmlns:conf=\"http://lilyproject.org/configuration\" conf:inherit=\"shallow\">" +
                "<decorators><decorator>test-decorator</decorator></decorators>" +
                "</repository>";

        FileUtils.writeStringToFile(new File(repoConfDir, "repository.xml"), repositoryXml, "UTF-8");

        return confDir;
    }
}
