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
package org.kauriproject.runtime.module;

import org.kauriproject.runtime.classloading.ClassLoadingConfig;
import org.kauriproject.runtime.model.ModuleDefinition;
import org.kauriproject.runtime.rapi.ModuleSource;

public class ModuleConfigImpl implements ModuleConfig {
    private ClassLoadingConfig classLoadingConfig;
    private ModuleDefinition moduleDefinition;
    private ModuleSource moduleSource;

    public ModuleConfigImpl(ModuleDefinition moduleDefinition, ClassLoadingConfig classLoadingConfig, ModuleSource moduleSource) {
        this.moduleDefinition = moduleDefinition;
        this.classLoadingConfig = classLoadingConfig;
        this.moduleSource = moduleSource;
  }

    public ClassLoadingConfig getClassLoadingConfig() {
        return classLoadingConfig;
    }

    public String getLocation() {
        return moduleDefinition.getFile().getAbsolutePath();
    }

    public String getId() {
        return moduleDefinition.getId();
    }

    public ModuleDefinition getDefinition() {
        return moduleDefinition;
    }

    public ModuleSource getModuleSource() {
        return moduleSource;
    }
}
