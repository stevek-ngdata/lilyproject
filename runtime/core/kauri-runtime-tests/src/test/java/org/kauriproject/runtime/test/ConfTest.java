package org.kauriproject.runtime.test;

import org.kauriproject.runtime.testfw.AbstractRuntimeTest;
import org.kauriproject.runtime.model.*;
import org.kauriproject.runtime.rapi.ConfRegistry;
import org.kauriproject.runtime.configuration.ConfManager;
import org.kauriproject.runtime.configuration.ConfManagerImpl;
import org.kauriproject.runtime.test.testmodules.confmod.ConfDependentBean;
import org.kauriproject.conf.Conf;
import org.springframework.context.ApplicationContext;

import java.io.File;
import java.util.Collections;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;

public class ConfTest extends AbstractRuntimeTest {
    @Override
    protected KauriRuntimeModel getRuntimeModel() throws Exception {
        File moduleDir = createModule("org.kauriproject.runtime.test.testmodules.confmod");

        KauriRuntimeModel model = new KauriRuntimeModel();

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
        assertEquals("Jef", conf.getChild("name").getValue());

        conf = confRegistry.getConfiguration("test2");
        assertEquals("foobar@hotmail.com", conf.getChild("email").getValue());
        assertEquals("smtp.google.com", conf.getChild("smtp").getValue());

        conf = confRegistry.getConfiguration("test3");
        assertEquals(599, conf.getChild("delay").getValueAsInteger());

        String confTestBean1 = (String)appContext.getBean("confTestBean1");
        assertEquals("foobar@hotmail.com", confTestBean1);

        ConfDependentBean confTestBean2 = (ConfDependentBean)appContext.getBean("confTestBean2");
        assertNotNull(confTestBean2.getConf());
        assertEquals("foobar@hotmail.com", confTestBean2.getConf().getChild("email").getValue());        

        String confTestBean3 = (String)appContext.getBean("confTestBean3");
        assertEquals("foobar@hotmail.com", confTestBean3);
    }

}
