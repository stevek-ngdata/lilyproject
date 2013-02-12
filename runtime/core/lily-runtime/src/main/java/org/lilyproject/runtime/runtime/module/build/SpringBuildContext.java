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
package org.lilyproject.runtime.runtime.module.build;

import java.util.ArrayList;
import java.util.List;

import org.lilyproject.runtime.runtime.KauriRuntime;
import org.lilyproject.runtime.runtime.module.Module;

/**
 * Context information that should be available while building the spring
 * bean container (= BeanFactory) for handling Kauri's namespace, see also
 * {@link KauriRuntimeNamespaceHandler}.
 */
public class SpringBuildContext {
    private List<JavaServiceExport> exportedJavaServices = new ArrayList<JavaServiceExport>();
    private KauriRuntime runtime;
    private ClassLoader moduleClassLoader;
    private Module module;

    public SpringBuildContext(KauriRuntime runtime, Module module, ClassLoader moduleClassLoader) {
        this.runtime = runtime;
        this.module = module;
        this.moduleClassLoader = moduleClassLoader;
    }

    public KauriRuntime getRuntime() {
        return runtime;
    }

    public void exportJavaService(String name, Class serviceType, String beanName) {
        exportedJavaServices.add(new JavaServiceExport(name, serviceType, beanName));
    }

    public Module getModule() {
        return module;
    }

    public List<JavaServiceExport> getExportedJavaServices() {
        return exportedJavaServices;
    }

    public static class JavaServiceExport {
        public String name;
        public Class serviceType;
        public String beanName;

        public JavaServiceExport(String name, Class serviceType, String beanName) {
            this.name = name;
            this.serviceType = serviceType;
            this.beanName = beanName;
        }
    }

    public ClassLoader getModuleClassLoader() {
        return moduleClassLoader;
    }
}
