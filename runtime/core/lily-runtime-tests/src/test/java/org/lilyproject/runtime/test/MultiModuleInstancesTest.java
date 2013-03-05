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

import org.lilyproject.runtime.model.*;
import org.lilyproject.runtime.testfw.AbstractRuntimeTest;

import java.io.File;

/**
 * Tests having multiple instances of the same module.
 */
public class MultiModuleInstancesTest extends AbstractRuntimeTest {
    protected LilyRuntimeModel getRuntimeModel() throws Exception {
        LilyRuntimeModel model = new LilyRuntimeModel();

        File module1Dir = createModule("org.lilyproject.runtime.test.testmodules.jwiringmod1");

        {
            ModuleDefinition module = new ModuleDefinition("jwiringmod1a", module1Dir, ModuleSourceType.EXPANDED_JAR);
            model.addModule(module);
        }

        {
            ModuleDefinition module = new ModuleDefinition("jwiringmod1b", module1Dir, ModuleSourceType.EXPANDED_JAR);
            model.addModule(module);
        }

        File module4Dir = createModule("org.lilyproject.runtime.test.testmodules.jwiringmod4");
        {
            ModuleDefinition module = new ModuleDefinition("jwiringmod4a", module4Dir, ModuleSourceType.EXPANDED_JAR);
            module.addInject(new JavaServiceInjectByServiceDefinition("java.lang.CharSequence", "jwiringmod1a", "foo1Bean"));
            model.addModule(module);
        }

        {
            ModuleDefinition module = new ModuleDefinition("jwiringmod4b", module4Dir, ModuleSourceType.EXPANDED_JAR);
            module.addInject(new JavaServiceInjectByServiceDefinition("java.lang.CharSequence", "jwiringmod1b", "foo1Bean"));
            model.addModule(module);
        }

        return model;
    }

    public void testIt() {
    }
}
