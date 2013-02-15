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
package org.lilyproject.runtime.launcher;

import java.io.File;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;
import java.util.Set;

public class RuntimeLauncher {
    private String repositoryLocation;
    private String runtimeConfigLocation;
    private Set<String> disabledModuleIds;

    public static RuntimeHandle launch(String runtimeConfigLocation, String repositoryLocation, Set<String> disabledModuleIds) {
        return new RuntimeLauncher(runtimeConfigLocation, repositoryLocation, disabledModuleIds).run();
    }

    private RuntimeLauncher(String runtimeConfigLocation, String repositoryLocation, Set<String> disabledModuleIds) {
        this.runtimeConfigLocation = runtimeConfigLocation;
        this.repositoryLocation = repositoryLocation;
        this.disabledModuleIds = disabledModuleIds;
    }

    public RuntimeHandle run() {
        ClassLoader classLoader = LauncherClasspathHelper.getClassLoader("org/lilyproject/launcher/classloader.xml", new File(repositoryLocation));

        ClassLoader oldContextClassLoader = Thread.currentThread().getContextClassLoader();
        Object runtime;
        try {
            Thread.currentThread().setContextClassLoader(classLoader);
            Class runtimeHelperClass = classLoader.loadClass("org.lilyproject.runtime.LilyRuntimeHelper");
            Method createMethod = runtimeHelperClass.getMethod("createRuntime", String.class, String.class, Set.class);
            runtime = createMethod.invoke(null, runtimeConfigLocation, repositoryLocation, disabledModuleIds);
            return new RuntimeHandle(runtime);
        } catch (InvocationTargetException e) {
            throw new RuntimeException("Error loading Lily runtime", e.getTargetException());
        } catch (Exception e) {
            throw new RuntimeException("Error loading Lily runtime", e);
        } finally {
            Thread.currentThread().setContextClassLoader(oldContextClassLoader);
        }
    }
}
