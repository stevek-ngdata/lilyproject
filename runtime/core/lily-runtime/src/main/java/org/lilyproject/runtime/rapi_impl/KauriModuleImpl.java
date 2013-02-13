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
package org.lilyproject.runtime.rapi_impl;

import java.util.ArrayList;
import java.util.List;

import org.lilyproject.runtime.KauriRuntime;
import org.lilyproject.runtime.module.Module;
import org.lilyproject.runtime.module.ModuleConfig;
import org.lilyproject.runtime.rapi.ConfRegistry;
import org.lilyproject.runtime.rapi.KauriModule;
import org.lilyproject.runtime.rapi.Mode;
import org.lilyproject.runtime.rapi.ModuleSource;
import org.springframework.context.ApplicationContext;

public class KauriModuleImpl implements KauriModule {
    private Module module;
    private KauriRuntime runtime;

    public KauriModuleImpl(Module module, KauriRuntime runtime) {
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

    public List<KauriModule> getModules() {
        List<KauriModule> modules = new ArrayList<KauriModule>();

        for (Module module : runtime.getModules()) {
            modules.add(new KauriModuleImpl(module, runtime));
        }

        return modules;
    }

    public KauriModule getModuleById(String id) {
        Module module = runtime.getModuleById(id);
        if (module == null)
            return null;

        return new KauriModuleImpl(module, runtime);
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
