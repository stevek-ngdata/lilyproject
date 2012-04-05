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
 * The place where plugins are registered.
 */
public interface PluginRegistry {
    /**
     * Adds (registers) a plugin.
     *
     * @param pluginType type of the plugin (usually an interface)
     * @param name a unique name (within a certain plugin type) for this plugin
     * @param plugin the actual plugin implementation
     */
    <T> void addPlugin(Class<T> pluginType, String name, T plugin);

    /**
     * Removes (unregisters) a plugin.
     *
     * <p>Plugins should only be removed by the components which register them.
     */
    <T> void removePlugin(Class<T> pluginType, String name, T plugin);

    /**
     * Sets the user for a specific type of plugins.
     *
     * <p>There can be at most one user for each type of plugin.
     *
     * <p>Upon setting the plugin user, any plugins already available
     * of that type are passed to the plugin user using {@link PluginUser#pluginAdded}.
     *
     * @param pluginType
     * @param pluginUser
     */
    <T> void setPluginUser(Class<T> pluginType, PluginUser<T> pluginUser);

    <T> void unsetPluginUser(Class<T> pluginType, PluginUser<T> pluginUser);
}
