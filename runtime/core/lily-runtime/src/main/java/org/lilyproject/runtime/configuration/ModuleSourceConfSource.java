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
package org.lilyproject.runtime.configuration;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.lilyproject.runtime.module.ModuleConfig;
import org.lilyproject.runtime.rapi.ModuleSource;
import org.lilyproject.runtime.rapi.ModuleSource.Resource;

public class ModuleSourceConfSource extends ConfSource {
    private ModuleConfig moduleConfig;
    private ModuleSource moduleSource;

    public ModuleSourceConfSource(ModuleConfig moduleConfig) {
        super("[built-in config of module " + moduleConfig.getId() + " loaded from " + moduleConfig.getLocation() + "]");
        this.moduleConfig = moduleConfig;
        this.moduleSource = moduleConfig.getModuleSource();
    }

    protected List<ConfigPath> getConfigFiles() {
        List<ConfigPath> configPaths = new ArrayList<ConfigPath>();

        Resource resource = moduleSource.getResource("conf");
        if (resource == null) {
            return configPaths;
        }

        collectConfigFiles(configPaths, resource, "");

        return configPaths;
    }

    private void collectConfigFiles(List<ConfigPath> configPaths, Resource resource, String path) {
        for (String child : resource.getChildren()) {
            String childPath = path.length() > 0 ? path + "/" + child : child;
            String resourcePath = "conf/" + childPath;
            Resource childResource = moduleSource.getResource(resourcePath);
            if (acceptFileName(childResource.isDirectory(), child)) {
                if (childResource.isDirectory()) {
                    collectConfigFiles(configPaths, childResource, childPath);
                } else {
                    if (childPath.endsWith(CONFIG_FILE_EXT)) { // should be the case
                        childPath = childPath.substring(0, childPath.length() - CONFIG_FILE_EXT.length());
                    }
                    configPaths.add(new ConfigPath(childPath, new MSConfigFile(childResource,
                            "[resource " + resourcePath + " in module " + moduleConfig.getId())));
                }
            }
        }
    }

    private static class MSConfigFile implements ConfigFile {
        private Resource resource;
        private String path;

        public MSConfigFile(Resource resource, String path) {
            this.resource = resource;
            this.path = path;
        }

        public InputStream getInputStream() throws IOException {
            return resource.getInputStream();
        }

        public String getPath() {
            return path;
        }

        public long lastModified() {
            return resource.lastModified();
        }
    }
}
