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
package org.kauriproject.template.el;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * Functions have to be static. If you can't rewrite you function to be static, you should: <li>use the
 * method invocation mechanism e.g. x.myFunction()</li> <li>or add a new expressionparser for your function
 * resolving e.g. $f{myFunction()}</li> <li>or use factory.createMethodExpression</li>
 * 
 */
public class FunctionRegistry {

    private Map<String, Method> functions;
    private String prefix;

    public FunctionRegistry(String prefix) {
        this.functions = new HashMap<String, Method>();
        this.prefix = prefix;
    }

    public boolean registerFunction(String name, Method method) {
        boolean added = false;
        if (method != null && !functions.containsKey(name)) {
            functions.put(name, method);
            added = true;
        }
        return added;
    }

    public void registerFunctionsFromClass(Class<?> functionClass) {
        Method[] methods = functionClass.getDeclaredMethods();
        for (Method method : methods) {
            registerFunction(method.getName(), method);
        }
    }

    public String getPrefix() {
        return prefix;
    }

    public Map<String, Method> getFunctions() {
        return functions;
    }

    public Method getFunction(String name) {
        return functions.get(name);
    }
}
