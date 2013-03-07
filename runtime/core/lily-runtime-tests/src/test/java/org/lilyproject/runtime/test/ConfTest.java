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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.lilyproject.runtime.conf.Conf;
import org.lilyproject.runtime.configuration.ConfManager;
import org.lilyproject.runtime.configuration.ConfManagerImpl;
import org.lilyproject.runtime.model.LilyRuntimeModel;
import org.lilyproject.runtime.model.ModuleDefinition;
import org.lilyproject.runtime.model.ModuleSourceType;
import org.lilyproject.runtime.rapi.ConfRegistry;
import org.lilyproject.runtime.test.testmodules.confmod.ConfDependentBean;
import org.lilyproject.runtime.testfw.AbstractRuntimeTest;
import org.springframework.context.ApplicationContext;

public class ConfTest extends AbstractRuntimeTest {
    @Override
    protected LilyRuntimeModel getRuntimeModel() throws Exception {
        File moduleDir = createModule("org.lilyproject.runtime.test.testmodules.confmod");

        LilyRuntimeModel model = new LilyRuntimeModel();

        ModuleDefinition module = new ModuleDefinition("confmod", moduleDir, ModuleSourceType.EXPANDED_JAR);
        model.addModule(module);

        return model;
    }

    @Override
    protected ConfManager getConfManager() throws Exception {
        List<File> confDirs = new ArrayList<File>();

        confDirs.add(createConfDir("conftest1"));
        confDirs.add(createConfDir("conftest2"));

        return new ConfManagerImpl(confDirs);
    }

    public void testConf() {
        ApplicationContext appContext = runtime.getModuleById("confmod").getApplicationContext();

        Map beans = appContext.getBeansOfType(ConfRegistry.class);
        ConfRegistry confRegistry = (ConfRegistry)beans.get("conf");

        Conf conf = confRegistry.getConfiguration("test1");
        Assert.assertEquals("Jef", conf.getChild("name").getValue());

        conf = confRegistry.getConfiguration("test2");
        Assert.assertEquals("foobar@hotmail.com", conf.getChild("email").getValue());
        Assert.assertEquals("smtp.google.com", conf.getChild("smtp").getValue());

        conf = confRegistry.getConfiguration("test3");
        Assert.assertEquals(599, conf.getChild("delay").getValueAsInteger());

        String confTestBean1 = (String)appContext.getBean("confTestBean1");
        Assert.assertEquals("foobar@hotmail.com", confTestBean1);

        ConfDependentBean confTestBean2 = (ConfDependentBean)appContext.getBean("confTestBean2");
        Assert.assertNotNull(confTestBean2.getConf());
        assertEquals("foobar@hotmail.com", confTestBean2.getConf().getChild("email").getValue());

        String confTestBean3 = (String)appContext.getBean("confTestBean3");
        Assert.assertEquals("foobar@hotmail.com", confTestBean3);
    }

}
