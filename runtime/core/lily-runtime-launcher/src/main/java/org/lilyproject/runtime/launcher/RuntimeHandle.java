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

import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;

public class RuntimeHandle {
    private Object runtime;
    private Method getServiceMethod;
    private Method shutdownMethod;

    public RuntimeHandle(Object runtime) throws NoSuchMethodException {
        this.runtime = runtime;
        this.getServiceMethod = runtime.getClass().getMethod("getService", Class.class);
        this.shutdownMethod = runtime.getClass().getMethod("shutdown");
    }

    public Object getService(Class type) {
        try {
            return getServiceMethod.invoke(runtime, type);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Error getting service from the Kauri Runtime.", e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException("Error getting service from the Kauri Runtime.", e.getTargetException());
        }
    }

    public void shutdown() {
        try {
            shutdownMethod.invoke(runtime);
        } catch (InvocationTargetException e) {
            throw new RuntimeException("Error shutting down the Kauri Runtime.", e.getTargetException());
        } catch (Exception e) {
            throw new RuntimeException("Error shutting down the Kauri Runtime.", e);
        }
    }
}
