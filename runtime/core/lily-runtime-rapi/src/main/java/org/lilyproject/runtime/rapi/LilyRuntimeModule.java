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
package org.lilyproject.runtime.rapi;

import java.util.List;

import org.springframework.context.ApplicationContext;

public interface LilyRuntimeModule {
    String getId();

    ModuleSource getSource();

    /**
     * Returns the list of module sources. They are returned the order in which modules
     * are started. If there are duplicates (a module can be instantiated multiple times),
     * only the first occurence is included.
     */
    List<ModuleSource> getModuleSources();

    /**
     * Returns list of all modules.
     *
     * <p>During startup, this list will only contain the modules already started!
     *
     * <p>Note that in general, modules should not access other modules. This method
     * is intended for exceptional use, and might in the future be limited to modules
     * having special system privileges. Thus, don't use this, unless you know what
     * you are doing.
     */
    List<LilyRuntimeModule> getModules();

    /**
     * Returns a module by ID.
     *
     * <p>Returns null if the module does not exist or is not yet started.
     *
     * <p>Note that in general, modules should not access other modules. This method
     * is intended for exceptional use, and might in the future be limited to modules
     * having special system privileges. Thus, don't use this, unless you know what
     * you are doing.
     */
    LilyRuntimeModule getModuleById(String id);

    ClassLoader getClassLoader();

    Object getImplementation();

    Object getRuntime();

    ApplicationContext getSpringContext();

    Mode getRuntimeMode();

    ConfRegistry getConfRegistry();
    
    String moduleInfo();
}
