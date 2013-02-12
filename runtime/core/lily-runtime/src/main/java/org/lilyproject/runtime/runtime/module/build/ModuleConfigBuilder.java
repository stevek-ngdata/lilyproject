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
package org.lilyproject.runtime.runtime.module.build;

import java.io.InputStream;
import java.util.Collections;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.runtime.runtime.KauriRTException;
import org.lilyproject.runtime.runtime.KauriRuntime;
import org.lilyproject.runtime.runtime.classloading.ArtifactSharingMode;
import org.lilyproject.runtime.runtime.classloading.ClassLoadingConfig;
import org.lilyproject.runtime.runtime.classloading.ClassLoadingConfigImpl;
import org.lilyproject.runtime.runtime.classloading.ClasspathEntry;
import org.lilyproject.runtime.runtime.classloading.XmlClassLoaderBuilder;
import org.lilyproject.runtime.runtime.model.ModuleDefinition;
import org.lilyproject.runtime.runtime.module.ModuleConfig;
import org.lilyproject.runtime.runtime.module.ModuleConfigImpl;
import org.lilyproject.runtime.runtime.rapi.ModuleSource;
import org.lilyproject.runtime.runtime.repository.FileArtifactRef;
import org.lilyproject.runtime.util.io.IOUtils;
import org.lilyproject.runtime.util.xml.DocumentHelper;
import org.w3c.dom.Document;

public class ModuleConfigBuilder {
    private ModuleDefinition moduleDefinition;
    private KauriRuntime runtime;

    private final Log log = LogFactory.getLog(getClass());

    public static ModuleConfig build(ModuleDefinition moduleDefinition, KauriRuntime runtime) throws KauriRTException {
        return new ModuleConfigBuilder(moduleDefinition, runtime).build();
    }

    private ModuleConfigBuilder(ModuleDefinition moduleDefinition, KauriRuntime runtime) {
        this.moduleDefinition = moduleDefinition;
        this.runtime = runtime;
    }

    private ModuleConfig build() {
        ModuleSource moduleSource = null;
        VersionManager versionManager = new VersionManager(runtime);
        try {
            moduleSource = runtime.getModuleSourceManager().getModuleSource(moduleDefinition.getFile(), moduleDefinition.getSourceType());

            // build classpath
            ClassLoadingConfig classLoadingConfig;
            InputStream is = moduleSource.getClassLoaderConfig();
            if (is != null) {
                try {
                    Document document = DocumentHelper.parse(is);
                    classLoadingConfig = XmlClassLoaderBuilder.build(document.getDocumentElement(), moduleSource, runtime.getArtifactRepository(), versionManager);
                } finally {
                    IOUtils.closeQuietly(is);
                }
            } else {
                ClasspathEntry selfEntry = new ClasspathEntry(new FileArtifactRef(moduleSource.getClassPathEntry()), ArtifactSharingMode.PROHIBITED, moduleSource);
                classLoadingConfig = new ClassLoadingConfigImpl(Collections.singletonList(selfEntry), runtime.getArtifactRepository());
            }

            ModuleConfigImpl moduleConfig = new ModuleConfigImpl(moduleDefinition, classLoadingConfig, moduleSource);

            return moduleConfig;

        } catch (Throwable e) {
            if (moduleSource != null) {
                try {
                    moduleSource.dispose();
                } catch (Throwable e2) {
                    log.error("Error disposing module source for file " + moduleDefinition.getFile().getAbsolutePath(), e2);
                }
            }

            throw new KauriRTException("Error reading module config from " + moduleDefinition.getFile().getAbsolutePath(), e);
        }
    }



}
