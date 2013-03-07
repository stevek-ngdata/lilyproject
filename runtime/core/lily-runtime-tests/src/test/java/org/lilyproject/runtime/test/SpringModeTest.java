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

import org.junit.Assert;
import org.lilyproject.runtime.model.LilyRuntimeModel;
import org.lilyproject.runtime.model.ModuleDefinition;
import org.lilyproject.runtime.model.ModuleSourceType;
import org.lilyproject.runtime.rapi.Mode;
import org.lilyproject.runtime.testfw.AbstractRuntimeTest;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;

/**
 * Test case to check that different spring configurations can be used
 * depending on the runtime mode.
 */
public class SpringModeTest extends AbstractRuntimeTest {

    @Override
    protected LilyRuntimeModel getRuntimeModel() throws Exception {
        File moduleDir = createModule("org.lilyproject.runtime.test.testmodules.springmode");

        LilyRuntimeModel model = new LilyRuntimeModel();

        ModuleDefinition module = new ModuleDefinition("mod", moduleDir, ModuleSourceType.EXPANDED_JAR);
        model.addModule(module);

        return model;
    }

    @Override
    protected boolean startRuntime() {
        return false;
    }

    public void testProductionMode() throws Exception {
        runtime.setMode(Mode.PRODUCTION);
        runtime.start();

        try {
            runtime.getModuleById("mod").getApplicationContext().getBean("commonbean");
            runtime.getModuleById("mod").getApplicationContext().getBean("productionbean");

            try {
                runtime.getModuleById("mod").getApplicationContext().getBean("prototypebean");
                Assert.fail("expected exception");
            } catch (NoSuchBeanDefinitionException e) {
            }
        } finally {
            runtime.stop();
        }
    }

    public void testPrototypeMode() throws Exception {
        runtime.setMode(Mode.PROTOTYPE);
        runtime.start();

        try {
            runtime.getModuleById("mod").getApplicationContext().getBean("commonbean");
            runtime.getModuleById("mod").getApplicationContext().getBean("prototypebean");

            try {
                runtime.getModuleById("mod").getApplicationContext().getBean("productionbean");
                Assert.fail("expected exception");
            } catch (NoSuchBeanDefinitionException e) {
            }
        } finally {
            runtime.stop();
        }
    }
}
