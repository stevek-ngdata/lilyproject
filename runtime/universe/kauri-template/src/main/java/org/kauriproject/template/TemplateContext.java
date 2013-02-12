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

import java.util.Map;
import java.util.Set;

/**
 * Interface for the context used in Kauri Templating.
 */
public interface TemplateContext {

    /**
     * Add a new (key,value) pair to the context, or if an entry with that key exists, overwrite the existing
     * value.
     */
    public Object put(String key, Object value);

    /**
     * Add all entries from a given map to the context. If a key already exists, the old value will be
     * overwritten.
     */
    public void putAll(Map<String, Object> map);

    /**
     * Retrieve the value matching with the specified key from the context, or retrieve null if it doesn't
     * exist.
     */
    public Object get(String key);

    /**
     * Retrieve all the keys that live in the context.
     */
    public Set<String> getAll();

    /**
     * Check if the context contains an entry with a certain key.
     */
    public boolean containsKey(String key);

    /**
     * Save the current context, so it can be restored to this point.
     */
    public void saveContext();

    /**
     * Restore the context to the situation of the last 'saveContext' call.
     */
    public void restoreContext();
}
