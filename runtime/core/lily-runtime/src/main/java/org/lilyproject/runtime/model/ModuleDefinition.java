/*
 * Copyright 2013 NGDATA nv
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
package org.lilyproject.runtime.model;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.lilyproject.runtime.LilyRTException;
import org.lilyproject.util.ArgumentValidator;
import org.lilyproject.util.location.Location;

/**
 * Pointer to a module file (to be part of the runtime).
 */
public class ModuleDefinition {
    private final String id;
    private final File file;
    private ModuleSourceType sourceType;
    private Map<String, JavaServiceInjectByNameDefinition> javaServiceInjects = new HashMap<String, JavaServiceInjectByNameDefinition>();
    private Map<String, JavaServiceInjectByServiceDefinition> javaServiceInjectsByService = new HashMap<String, JavaServiceInjectByServiceDefinition>();
    private Location location;
    private String version;

    public ModuleDefinition(String id, File file, ModuleSourceType sourceType) {
        ArgumentValidator.notNull(id, "id");
        ArgumentValidator.notNull(file, "file");
        ArgumentValidator.notNull(sourceType, "sourceType");

        this.id = id;
        this.file = file;
        this.sourceType = sourceType;
    }

    public String getId() {
        return id;
    }

    public Location getLocation() {
        return location;
    }

    public void setLocation(Location location) {
        this.location = location;
    }
    
    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }
    
    public String moduleInfo() {
        if (Boolean.getBoolean("lilyruntime.info.verbose"))
            return String.format("%s (version: %s) - running from [%s] (in mode: %s)", id, version, file.getAbsolutePath(), sourceType.name());
        //else
        return String.format("%s (version: %s)", id, version);
    }
    
    /**
     * Use {@link #getSourceType()} to find out which kind of module source type this
     * file points to.
     */
    public File getFile() {
        return file;
    }

    public ModuleSourceType getSourceType() {
        return sourceType;
    }

    public void addInject(JavaServiceInjectByNameDefinition injectDefinition) {
        if (javaServiceInjects.containsKey(injectDefinition.getDependencyName()))
            throw new LilyRTException("Duplicate inject for Java service " + injectDefinition.getDependencyName() + " on module " + id);
        javaServiceInjects.put(injectDefinition.getDependencyName(), injectDefinition);
    }

    public JavaServiceInjectByNameDefinition getJavaServiceInject(String dependencyName) {
        return javaServiceInjects.get(dependencyName);
    }

    public void addInject(JavaServiceInjectByServiceDefinition injectDefinition) {
        if (javaServiceInjectsByService.containsKey(injectDefinition.getServiceType()))
            throw new LilyRTException("Duplicate inject for Java service " + injectDefinition.getServiceType() + " on module " + id);
        javaServiceInjectsByService.put(injectDefinition.getServiceType(), injectDefinition);
    }

    public JavaServiceInjectByServiceDefinition getJavaServiceInjectByService(String serviceType) {
        return javaServiceInjectsByService.get(serviceType);
    }

    public void validate(List<ConfigError> configErrors, LilyRuntimeModel model) {
    }

}
