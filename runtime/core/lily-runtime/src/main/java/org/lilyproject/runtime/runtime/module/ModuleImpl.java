/*
 * Copyright 2007 Outerthought bvba and Schaubroeck nv
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
package org.lilyproject.runtime.runtime.module;

import org.lilyproject.runtime.runtime.model.ModuleDefinition;
import org.lilyproject.runtime.runtime.rapi.ModuleSource;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.GenericApplicationContext;

public class ModuleImpl implements Module {
    private GenericApplicationContext applicationContext;
    private final ClassLoader classLoader;
    private final ModuleDefinition definition;
    private final ModuleSource source;

    public ModuleImpl(ClassLoader classLoader, GenericApplicationContext applicationContext,
                      ModuleDefinition moduleDefinition, ModuleSource source) {
        this.classLoader = classLoader;
        this.applicationContext = applicationContext;
        this.definition = moduleDefinition;
        this.source = source;
    }

    public void start() {
        // Nothing to do
    }

    public void shutdown() throws Exception {
        applicationContext.close();
        source.dispose();
    }

    public boolean isAlive() {
        return applicationContext.isActive();
    }

    public ModuleDefinition getDefinition() {
        return definition;
    }

    public ModuleSource getSource() {
        return source;
    }

    public ClassLoader getClassLoader() {
        return classLoader;
    }

    public AbstractApplicationContext getApplicationContext() {
        return applicationContext;
    }
}
