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

import org.kauriproject.runtime.model.ModuleDefinition;
import org.kauriproject.runtime.rapi.ModuleSource;
import org.springframework.context.support.AbstractApplicationContext;

/**
 * A module.
 */
public interface Module {
    /**
     * Shuts down this module.
     */
    void shutdown() throws Exception;

    /**
     * Returns true if the module is alive, thus when it is not shut down.
     */
    boolean isAlive();

    ModuleDefinition getDefinition();

    ModuleSource getSource();

    ClassLoader getClassLoader();

    // TODO maybe this should move below a JavaServiceFacet?
    AbstractApplicationContext getApplicationContext();
}
