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
package org.lilyproject.runtime.test;

import org.junit.Assert;
import org.lilyproject.runtime.LilyRuntime;
import org.lilyproject.runtime.LilyRuntimeSettings;
import org.lilyproject.runtime.configuration.ConfManagerImpl;
import org.lilyproject.runtime.model.LilyRuntimeModel;
import org.lilyproject.runtime.testfw.AbstractRuntimeTest;

/**
 * Test start/stop of runtime without any modules.
 */
public class NoModuleRuntime extends AbstractRuntimeTest {

    public void testRuntimeWithoutModules() throws Exception {
        LilyRuntimeModel model = new LilyRuntimeModel();
        LilyRuntimeSettings settings = new LilyRuntimeSettings();
        settings.setModel(model);
        settings.setRepository(dummyRepository);
        settings.setConfManager(new ConfManagerImpl());

        LilyRuntime runtime = new LilyRuntime(settings);
        runtime.start();
        runtime.stop();

        // Test we can start the runtime a second time
        runtime = new LilyRuntime(settings);
        runtime.start();
        runtime.stop();

        // Same with connectors configured
        runtime = new LilyRuntime(settings);
        runtime.start();
        runtime.stop();

        // Test that we can't start the same runtime twice
        try {
            runtime.start();
            Assert.fail("Starting the same runtime instance twice should fail.");
        } catch (Exception e) {}

        // Test that we can't start if we don't set a repository in the config
        settings.setRepository(null);
        runtime = new LilyRuntime(settings);
        try {
            runtime.start();
            Assert.fail("Starting runtime should fail if there is no artifact repository configured.");
        } catch (Exception e) {}
    }
}
