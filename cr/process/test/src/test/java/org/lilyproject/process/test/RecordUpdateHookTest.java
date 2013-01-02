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

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilyproject.client.LilyClient;
import org.lilyproject.lilyservertestfw.LilyProxy;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.TypeManager;

import static org.junit.Assert.assertEquals;

/**
 * Tests the record update hook feature.
 * <p/>
 * <p>This test relies on test-updatehook being installed in the local maven repository, hence
 * requires that the build is done with "mvn install".</p>
 */
public class RecordUpdateHookTest {
    private static LilyProxy lilyProxy;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
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
            lilyProxy = new LilyProxy();
            lilyProxy.start();
        } finally {
            // Make sure it's properties won't be used by later-running tests
            System.getProperties().remove("lily.plugin.dir");
            System.getProperties().remove("lily.conf.dir");
            System.getProperties().remove("lily.conf.customdir");
        }
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        try {
            if (lilyProxy != null) {
                lilyProxy.stop();
            }
        } catch (Throwable t) {
            t.printStackTrace();
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
                "    <artifact id=\"test-updatehook\" groupId=\"org.lilyproject\" artifactId=\"lily-test-updatehook\" version=\"" + projectVersion + "\">\n" +
                "    </artifact>\n" +
                "  </modules>\n" +
                "</wiring>";

        FileUtils.writeStringToFile(new File(beforeRepoDir, "testupdatehook.xml"), wiringXml, "UTF-8");

        return pluginDir.getAbsolutePath();
    }

    private static File setupConfDirectory(File tmpDir) throws Exception {
        File confDir = new File(tmpDir + "/conf");

        File repoConfDir = new File(confDir, "repository");
        FileUtils.forceMkdir(repoConfDir);

        // Write configuration to activate the decorator
        String repositoryXml = "<repository xmlns:conf=\"http://kauriproject.org/configuration\" conf:inherit=\"shallow\">" +
                "<updateHooks><updateHook>test-updatehook</updateHook></updateHooks>" +
                "</repository>";

        FileUtils.writeStringToFile(new File(repoConfDir, "repository.xml"), repositoryXml, "UTF-8");

        return confDir;
    }

    @Test
    public void test() throws Exception {
        LilyClient client = lilyProxy.getLilyServerProxy().getClient();

        // Obtain a repository
        Repository repository = client.getRepository();

        TypeManager typeMgr = repository.getTypeManager();

        QName fieldName = new QName("ns", "f1");
        FieldType fieldType = typeMgr.newFieldType(typeMgr.getValueType("STRING"), fieldName, Scope.NON_VERSIONED);
        fieldType = typeMgr.createFieldType(fieldType);

        QName typeName = new QName("ns", "rt1");
        RecordType recordType = typeMgr.newRecordType(typeName);
        recordType.addFieldTypeEntry(fieldType.getId(), false);
        recordType = typeMgr.createRecordType(recordType);

        Record record = repository.newRecord();
        record.setRecordType(typeName);
        record.setField(fieldName, "foo");
        record = repository.create(record);

        assertEquals("foo-create-hook", record.getField(fieldName));

        record = repository.read(record.getId());
        assertEquals("foo-create-hook", record.getField(fieldName));

        record.setField(fieldName, "bar");
        record = repository.update(record);

        assertEquals("bar-update-hook", record.getField(fieldName));

        record = repository.read(record.getId());
        assertEquals("bar-update-hook", record.getField(fieldName));
    }
}
