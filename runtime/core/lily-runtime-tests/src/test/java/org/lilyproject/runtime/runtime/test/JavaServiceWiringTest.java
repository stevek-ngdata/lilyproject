/*
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
package org.lilyproject.runtime.runtime.test;

import org.lilyproject.runtime.runtime.testfw.AbstractRuntimeTest;
import org.lilyproject.runtime.runtime.model.*;

import java.io.File;

/**
 * Tests explicit wiring of Java services.
 */
public class JavaServiceWiringTest extends AbstractRuntimeTest {
    protected KauriRuntimeModel getRuntimeModel() throws Exception {
        KauriRuntimeModel model = new KauriRuntimeModel();

        {
            File module1Dir = createModule("org.lilyproject.runtime.test.testmodules.jwiringmod1");
            ModuleDefinition module1 = new ModuleDefinition("jwiringmod1", module1Dir, ModuleSourceType.EXPANDED_JAR);
            model.addModule(module1);
        }

        {
            File module2Dir = createModule("org.lilyproject.runtime.test.testmodules.jwiringmod2");
            ModuleDefinition module2 = new ModuleDefinition("jwiringmod2", module2Dir, ModuleSourceType.EXPANDED_JAR);
            model.addModule(module2);
        }

        {
            File module3Dir = createModule("org.lilyproject.runtime.test.testmodules.jwiringmod3");
            ModuleDefinition module3 = new ModuleDefinition("jwiringmod3", module3Dir, ModuleSourceType.EXPANDED_JAR);
            module3.addInject(new JavaServiceInjectByNameDefinition("bean1", "jwiringmod1", "foo1Bean"));
            module3.addInject(new JavaServiceInjectByNameDefinition("someDependency", "jwiringmod1", "someName"));
            module3.addInject(new JavaServiceInjectByNameDefinition("bean3", "jwiringmod2"));
            model.addModule(module3);
        }

        {
            File module4Dir = createModule("org.lilyproject.runtime.test.testmodules.jwiringmod4");
            ModuleDefinition module4 = new ModuleDefinition("jwiringmod4", module4Dir, ModuleSourceType.EXPANDED_JAR);
            module4.addInject(new JavaServiceInjectByServiceDefinition("java.lang.CharSequence", "jwiringmod2"));
            model.addModule(module4);
        }

        return model;
    }

    public void testIt() {
        // The main test is that the Kauri Runtime is able to boot up. The below is just some additional verification.
        assertEquals("foo1", runtime.getModuleById("jwiringmod3").getApplicationContext().getBean("bean1").toString());
        assertEquals("bar", runtime.getModuleById("jwiringmod4").getApplicationContext().getBean("bean1").toString());
    }
}
