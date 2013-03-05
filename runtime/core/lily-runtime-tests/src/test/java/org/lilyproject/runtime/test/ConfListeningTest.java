/*
 * Copyright 2013 NGDATA nv
 * Copyright 2008 Outerthought bvba and Schaubroeck nv
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
package org.lilyproject.runtime.test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.lilyproject.runtime.configuration.ConfManager;
import org.lilyproject.runtime.configuration.ConfManagerImpl;
import org.lilyproject.runtime.model.LilyRuntimeModel;
import org.lilyproject.runtime.model.ModuleDefinition;
import org.lilyproject.runtime.model.ModuleSourceType;
import org.lilyproject.runtime.rapi.ConfListener;
import org.lilyproject.runtime.rapi.ConfRegistry;
import org.lilyproject.runtime.testfw.AbstractRuntimeTest;

public class ConfListeningTest extends AbstractRuntimeTest {
    private File conflistenmodConfDir;

    @Override
    protected LilyRuntimeModel getRuntimeModel() throws Exception {
        File moduleDir = createModule("org.lilyproject.runtime.test.testmodules.conflistenmod");

        LilyRuntimeModel model = new LilyRuntimeModel();

        ModuleDefinition module = new ModuleDefinition("conflistenmod", moduleDir, ModuleSourceType.EXPANDED_JAR);
        model.addModule(module);

        return model;
    }

    @Override
    protected ConfManager getConfManager() throws Exception {
        List<File> confDirs = new ArrayList<File>();

        // Base dir below which the conf will be defined
        File confDir = createTempDir();
        File test1ConfDir = new File(confDir, "conftest1");
        test1ConfDir.mkdirs();

        // Create a config for the configuration manager
        File lilyRuntimeConfDir = new File(test1ConfDir, "runtime");
        writeConf(lilyRuntimeConfDir, "configuration.xml", "<conf><reloading enabled='true' delay='1000'/></conf>");

        // Create a conf for our test module
        conflistenmodConfDir = new File(test1ConfDir, "conflistenmod");
        writeConf(conflistenmodConfDir, "foobar.xml", "<conf/>");

        // Create the ConfManager
        confDirs.add(test1ConfDir);
        return new ConfManagerImpl(confDirs);
    }

    public void testConf() throws Exception {
        Map beans = runtime.getModuleById("conflistenmod").getApplicationContext().getBeansOfType(ConfRegistry.class);
        ConfRegistry confRegistry = (ConfRegistry)beans.get("conf");

        final Set<String> changedPaths = new HashSet<String>();
        final Set<String> changedConfs = new HashSet<String>();

        confRegistry.addListener(new ConfListener() {
            public void confAltered(String path, ChangeType changeType) {
                switch (changeType) {
                    case CONF_CHANGE:
                        changedConfs.add(path);
                        break;
                    case PATH_CHANGE:
                        changedPaths.add(path);
                        break;
                }
            }
        }, null, ConfListener.ChangeType.CONF_CHANGE, ConfListener.ChangeType.PATH_CHANGE);

        // Sleep a second, as it seems the resolution for (my) file system changes is 1 second
        Thread.sleep(1500);
        writeConf(conflistenmodConfDir, "foobar.xml", "<conf x='y'/>");
        writeConf(conflistenmodConfDir, "x/y/foobar.xml", "<conf x='y'/>");

        // Sleep so that we can receive notifications (see configured delay)
        Thread.sleep(1000);

        Assert.assertEquals(2, changedConfs.size());
        Assert.assertEquals(1, changedPaths.size());

        Assert.assertTrue(changedConfs.contains("foobar"));
        Assert.assertTrue(changedConfs.contains("x/y/foobar"));
        Assert.assertTrue(changedPaths.contains("x/y"));

        Collection<String> childPaths = confRegistry.getConfigurations("x/y");
        Assert.assertEquals(1, childPaths.size());
        Assert.assertEquals("foobar", childPaths.iterator().next());

        //
        // Test that removal of a conf causes both a change event for the conf and its containing directory
        //
        changedPaths.clear();
        changedConfs.clear();

        boolean deleteSuccess = new File(conflistenmodConfDir, "x/y/foobar.xml").delete();
        Assert.assertTrue(deleteSuccess);

        // Sleep so that we can receive notifications (see configured delay)
        Thread.sleep(1000);

        for (String p : changedPaths) {
            System.out.println("changed path: " + p);
        }
        Assert.assertEquals(1, changedConfs.size());
        Assert.assertEquals(1, changedPaths.size());

        Assert.assertTrue(changedConfs.contains("x/y/foobar"));
        Assert.assertTrue(changedPaths.contains("x/y"));
    }

    private void writeConf(File dir, String path, String content) throws FileNotFoundException {
        File foobar = new File(dir, path);
        foobar.getParentFile().mkdirs();

        PrintWriter writer = new PrintWriter(new FileOutputStream(foobar));
        writer.print(content);
        writer.close();
    }

}
