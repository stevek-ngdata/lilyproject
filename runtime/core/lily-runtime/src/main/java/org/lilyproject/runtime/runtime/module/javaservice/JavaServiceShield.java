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
package org.lilyproject.runtime.runtime.module.javaservice;

import org.lilyproject.runtime.runtime.KauriRTException;
import org.lilyproject.runtime.runtime.module.Module;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;

/**
 * Dynamic proxy around services exported by modules (i.e. a wrapper around
 * spring beans). This has multiple purpose: provide shielding so that only service interface
 * methods can be called, check that the target module is still alive (e.g. to check
 * the shutdown scenario is ok), and to set the context classloader to the classloader
 * of the target module.
 */
public class JavaServiceShield implements InvocationHandler {
    private final Object component;
    private final Module owner;
    private final Class serviceType;
    private final ClassLoader classLoader;

    public JavaServiceShield(Object component, Module owner, Class serviceType, ClassLoader classLoader) {
        this.component = component;
        this.owner = owner;
        this.serviceType = serviceType;
        this.classLoader = classLoader;
    }

    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (!owner.isAlive()) {
            throw new KauriRTException("Component belongs to module which is no longer alive, for service " + serviceType.getName());
        }

        Thread currentThread = Thread.currentThread();
        ClassLoader previousContextClassLoader = currentThread.getContextClassLoader();
        try {
            currentThread.setContextClassLoader(classLoader);
            return method.invoke(component, args);
        } catch (InvocationTargetException e) {
            throw e.getTargetException();
        } finally {
            currentThread.setContextClassLoader(previousContextClassLoader);
        }
    }
}
