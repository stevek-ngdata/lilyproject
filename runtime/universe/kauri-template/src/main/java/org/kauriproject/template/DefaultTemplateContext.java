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
package org.kauriproject.template;

import java.util.*;

/**
 * Default implementation.
 * 
 * TODO: this probably needs an extreme makeover
 */
public class DefaultTemplateContext implements TemplateContext {

    private Stack<ContextMap> contexts;

    private ContextMap current;

    public DefaultTemplateContext() {
        contexts = new Stack<ContextMap>();
        current = new ContextMap();
    }

    public Object get(String key) {
        return current.get(key);
    }

    public Set<String> getAll() {
        return current.keySet();
    }

    public Object put(String key, Object value) {
        return current.put(key, value);
    }

    public void putAll(Map<String, Object> map) {
        current.putAll(map);
    }

    public boolean containsKey(String key) {
        return current.containsKey(key);
    }

    public void saveContext() {
        contexts.push(current);
        current = new ContextMap(current);
    }

    public void restoreContext() {
        current = contexts.pop();
    }

    // simple extension of hashmap
    class ContextMap extends HashMap<String, Object> {

        public ContextMap() {
            super();
        }

        public ContextMap(ContextMap contextMap) {
            super(contextMap);
        }
    }

}
