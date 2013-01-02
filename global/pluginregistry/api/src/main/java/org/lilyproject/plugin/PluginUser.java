/*
 * Copyright 2012 NGDATA nv
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
package org.lilyproject.plugin;

/**
 * Interface to be implemented by the user of a particular
 * type of plugin.
 * <p/>
 * <p>The {@link #pluginAdded} and {@link #pluginRemoved} methods will
 * never be called concurrently by multiple threads.
 */
public interface PluginUser<T> {
    /**
     * Notification of an available plugin.
     * <p/>
     * <p>Plugin users can be guaranteed that there won't be two
     * plugins registred with the same name (unless previously removed via
     * {@link #pluginRemoved}. The name also won't be null, an empty string,
     * or a whitespace string.
     */
    void pluginAdded(PluginHandle<T> pluginHandle);

    void pluginRemoved(PluginHandle<T> pluginHandle);
}
