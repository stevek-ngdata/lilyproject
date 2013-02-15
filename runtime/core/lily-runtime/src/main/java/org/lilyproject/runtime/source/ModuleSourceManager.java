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
package org.lilyproject.runtime.source;

import org.lilyproject.runtime.LilyRTException;
import org.lilyproject.runtime.rapi.ModuleSource;
import org.lilyproject.runtime.model.ModuleSourceType;
import org.apache.commons.jci.monitor.FilesystemAlterationMonitor;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;

/**
 * Manages ModuleSources. One of the main purposes is so that when multiple module instances
 * are created from the same module, only one module source is created for them.
 */
public class ModuleSourceManager {
    private Map<String, SharedModuleSource> moduleSources = new HashMap<String, SharedModuleSource>();
    private FilesystemAlterationMonitor fam;

    public ModuleSourceManager(FilesystemAlterationMonitor fam) {
        this.fam = fam;
    }

    public synchronized ModuleSource getModuleSource(File location, ModuleSourceType moduleSourceType)
            throws ModuleSourceCreationException {
        String key = getKey(location);
        SharedModuleSource moduleSource = moduleSources.get(key);
        if (moduleSource == null) {
            ModuleSource newModuleSource = createModuleSource(location, moduleSourceType);
            moduleSource = new SharedModuleSource(newModuleSource, this, key);
            moduleSources.put(key, moduleSource);
        } else {
            if (getType(moduleSource.getDelegate()) != moduleSourceType) {
                throw new LilyRTException("The same module location was requested earlier but with a different type. Type 1: " + getType(moduleSource) + ", type 2: " + moduleSourceType);
            }
        }
        moduleSource.increaseRefCount();
        return moduleSource;
    }

    private String getKey(File location) {
        try {
            return location.getCanonicalFile().getAbsolutePath();
        } catch (IOException e) {
            throw new LilyRTException("Error making key for module source location " + location, e);
        }
    }

    private ModuleSource createModuleSource(File location, ModuleSourceType moduleSourceType)
            throws ModuleSourceCreationException {
        try {
            ModuleSource moduleSource;
            switch (moduleSourceType) {
                case JAR:
                    moduleSource = new JarModuleSource(location);
                    break;
                case EXPANDED_JAR:
                    moduleSource = new ExpandedJarModuleSource(location, fam);
                    break;
                case SOURCE_DIRECTORY:
                    moduleSource = new MavenSourceDirectoryModuleSource(location, fam);
                    break;
                default:
                    throw new LilyRTException("Unexpected module definition type: " + location);
            }
            return moduleSource;
        } catch (Throwable t) {
            throw new ModuleSourceCreationException("Creating a module source from " + location + " of type " + moduleSourceType + " failed.", t);
        }
    }

    private ModuleSourceType getType(ModuleSource  moduleSource) {
        if (moduleSource instanceof JarModuleSource)
            return ModuleSourceType.JAR;
        else if (moduleSource instanceof ExpandedJarModuleSource)
            return ModuleSourceType.EXPANDED_JAR;
        else if (moduleSource instanceof MavenSourceDirectoryModuleSource)
            return ModuleSourceType.SOURCE_DIRECTORY;
        else
            throw new LilyRTException("Unrecognized module source implementation: " + moduleSource.getClass().getName());
    }

    protected synchronized void dispose(SharedModuleSource moduleSource) throws Exception {
        // Remove from list of managed module sources
        if (moduleSources.remove(moduleSource.getKey()) == null)
            throw new LilyRTException("Unexpected situation: disposed module source not found.");

        // perform disposal of actual module source
        moduleSource.getDelegate().dispose();
    }
}
