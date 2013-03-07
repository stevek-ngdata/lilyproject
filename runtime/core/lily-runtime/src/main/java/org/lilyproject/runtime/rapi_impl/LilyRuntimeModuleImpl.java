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
package org.lilyproject.runtime.rapi_impl;

import java.util.ArrayList;
import java.util.List;

import org.lilyproject.runtime.LilyRuntime;
import org.lilyproject.runtime.module.Module;
import org.lilyproject.runtime.module.ModuleConfig;
import org.lilyproject.runtime.rapi.ConfRegistry;
import org.lilyproject.runtime.rapi.LilyRuntimeModule;
import org.lilyproject.runtime.rapi.Mode;
import org.lilyproject.runtime.rapi.ModuleSource;
import org.springframework.context.ApplicationContext;

public class LilyRuntimeModuleImpl implements LilyRuntimeModule {
    private Module module;
    private LilyRuntime runtime;

    public LilyRuntimeModuleImpl(Module module, LilyRuntime runtime) {
        this.module = module;
        this.runtime = runtime;
    }

    public String getId() {
        return module.getDefinition().getId();
    }

    public ModuleSource getSource() {
        return module.getSource();
    }

    public List<ModuleSource> getModuleSources() {
        List<ModuleSource> moduleSources = new ArrayList<ModuleSource>();

        for (ModuleConfig config : runtime.getModuleConfigs()) {
            if (!moduleSources.contains(config.getModuleSource())) {
                moduleSources.add(config.getModuleSource());
            }
        }

        return moduleSources;
    }

    public List<LilyRuntimeModule> getModules() {
        List<LilyRuntimeModule> modules = new ArrayList<LilyRuntimeModule>();

        for (Module module : runtime.getModules()) {
            modules.add(new LilyRuntimeModuleImpl(module, runtime));
        }

        return modules;
    }

    public LilyRuntimeModule getModuleById(String id) {
        Module module = runtime.getModuleById(id);
        if (module == null) {
            return null;
        }

        return new LilyRuntimeModuleImpl(module, runtime);
    }

    public ClassLoader getClassLoader() {
        return module.getClassLoader();
    }

    public Object getImplementation() {
        return module;
    }

    public Object getRuntime() {
        return runtime;
    }

    public ApplicationContext getSpringContext() {
        return module.getApplicationContext();
    }

    public Mode getRuntimeMode() {
        return runtime.getMode();
    }

    public ConfRegistry getConfRegistry() {
        return runtime.getConfManager().getConfRegistry(module.getDefinition().getId());
    }

    public String moduleInfo() {
        return module.getDefinition().moduleInfo();
    }
}
