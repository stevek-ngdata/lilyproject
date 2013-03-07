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
package org.lilyproject.plugin.impl;

import org.lilyproject.plugin.PluginRegistry;
import org.lilyproject.plugin.PluginUser;
import org.lilyproject.plugin.PluginException;
import org.lilyproject.plugin.PluginHandle;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.annotation.PreDestroy;
import java.util.*;

public class PluginRegistryImpl implements PluginRegistry {
    private Map<Class, PluginManager> pluginsByType = new HashMap<Class, PluginManager>();
    private final Log log = LogFactory.getLog(getClass());
    private MBeanServer mbeanServer;

    public PluginRegistryImpl(MBeanServer mbeanServer) {
        this.mbeanServer = mbeanServer;
    }

    @Override
    public synchronized <T> void addPlugin(Class<T> pluginType, String name, T plugin) {
        getManager(pluginType).addPlugin(name, plugin);
    }

    @Override
    public synchronized <T> void removePlugin(Class<T> pluginType, String name, T plugin) {
        PluginManager<T> manager = getManager(pluginType);
        manager.removePlugin(name, plugin);
        if (manager.getPluginCount() == 0 && !manager.isUserSet()) {
            manager.destroy();
            pluginsByType.remove(pluginType);
        }
    }

    @Override
    public synchronized <T> void setPluginUser(Class<T> pluginType, PluginUser<T> pluginUser) {
        getManager(pluginType).setPluginUser(pluginUser);
    }

    @Override
    public synchronized <T> void unsetPluginUser(Class<T> pluginType, PluginUser<T> pluginUser) {
        getManager(pluginType).unsetPluginUser(pluginUser);
    }

    @PreDestroy
    public synchronized void destroy() {
        for (PluginManager manager : pluginsByType.values()) {
            if (manager.isUserSet()) {
                log.error("Plugin type " + manager.getType().getName() + ": plugin user has not been unset.");
            }
            if (manager.getPluginCount() > 0) {
                StringBuilder pluginNames = new StringBuilder();
                for (PluginEntry entry : (List<PluginEntry>)manager.getPlugins()) {
                    if (pluginNames.length() > 0)
                        pluginNames.append(", ");
                    pluginNames.append(entry.getName());
                }
                log.error("Plugin type " + manager.getType().getName() + ": still " + manager.getPluginCount() + " plugin(s) registered: " + pluginNames);
            }
            manager.destroy();
        }
    }

    @SuppressWarnings("unchecked")
    private <T> PluginManager<T> getManager(Class<T> pluginType) {
        PluginManager<T> manager = pluginsByType.get(pluginType);
        if (manager == null) {
            manager = new PluginManager<T>(pluginType);
            pluginsByType.put(pluginType, manager);
        }
        return manager;
    }

    public static interface PluginManagerMBean {
        String[] getRegisteredNames();

        boolean isUserSet();
    }

    private class PluginManager<T> implements PluginManagerMBean {
        private List<PluginEntry<T>> plugins = new ArrayList<PluginEntry<T>>();
        private PluginUser<T> user;
        private Class<T> type;
        private final Log log = LogFactory.getLog(getClass());
        private final ObjectName mbeanName;

        public PluginManager(Class<T> pluginType) {
            this.type = pluginType;

            String pluginTypeName = pluginType.getName();
            int dotPos = pluginTypeName.lastIndexOf('.');
            if (dotPos != -1) {
                pluginTypeName = pluginTypeName.substring(dotPos + 1);
            }

            try {
                mbeanName = new ObjectName("Lily:name=Plugins,type=" + pluginTypeName);
                mbeanServer.registerMBean(this, mbeanName);
            } catch (Exception e) {
                throw new RuntimeException("Unexpected error registering plugin type as mbean.", e);
            }
        }

        public void addPlugin(String name, T plugin) {
            if (name == null || name.trim().length() == 0) {
                throw new PluginException("Null, empty or whitespace argument: name");
            }

            if (plugin == null) {
                throw new IllegalArgumentException("Null argument: plugin");
            }

            if (!type.isAssignableFrom(plugin.getClass())) {
                throw new PluginException("Plugin does not implement its plugin type. Plugin \"" + name + "\" of type " + type.getName());
            }

            PluginEntry<T> newEntry = new PluginEntry<T>(name, plugin);

            for (PluginEntry entry : plugins) {
                if (entry.equals(newEntry))
                    throw new PluginException("This plugin instance is already registered. Plugin \"" + name + "\" of type " + type.getName());
                if (entry.name.equals(newEntry.name))
                    throw new PluginException("There is already another plugin registered with this name: \"" + name + "\".");
            }

            plugins.add(newEntry);

            notifyPluginAdded(newEntry);
        }

        public void removePlugin(String name, T plugin) {
            PluginEntry<T> removedEntry = new PluginEntry<T>(name, plugin);

            boolean found = false;
            Iterator<PluginEntry<T>> it = plugins.iterator();
            while (it.hasNext()) {
                PluginEntry entry = it.next();
                if (entry.equals(removedEntry)) {
                    it.remove();
                    found = true;
                    break;
                }
            }

            if (!found) {
                throw new PluginException("It is not possible to remove an plugin which is not registered. Plugin \"" + name + "\".");
            }

            notifyPluginRemoved(removedEntry);
        }

        public void setPluginUser(PluginUser<T> pluginUser) {
            if (this.user != null)
                throw new PluginException("Error setting plugin user: there can be only one PluginUser per plugin type. Type = " + type.getName());

            this.user = pluginUser;

            for (PluginEntry<T> entry : plugins) {
                notifyPluginAdded(entry);
            }
        }

        public void unsetPluginUser(PluginUser<T> pluginUser) {
            if (this.user != pluginUser)
                throw new PluginException("Error removing plugin user: the current plugin user does not correspond to the specified plugin user.");

            this.user = null;
        }

        private void notifyPluginAdded(PluginHandle<T> plugin) {
            if (user != null) {
                try {
                    user.pluginAdded(plugin);
                } catch (Throwable e) {
                    log.error("Error notifying plugin user of an added plugin. Plugin \"" + plugin.getName() + "\" of type " + type.getName());
                }
            }
        }

        private void notifyPluginRemoved(PluginHandle<T> plugin) {
            if (user != null) {
                try {
                    user.pluginRemoved(plugin);
                } catch (Throwable e) {
                    log.error("Error notifying plugin user of a removed plugin. Plugin \"" + plugin.getName() + "\" of type " + type.getName());
                }
            }
        }

        @Override
        public boolean isUserSet() {
            return user != null;
        }

        public int getPluginCount() {
            return plugins.size();
        }

        public Class getType() {
            return type;
        }

        public List<PluginEntry<T>> getPlugins() {
            return plugins;
        }

        @Override
        public String[] getRegisteredNames() {
            synchronized (PluginRegistryImpl.this) {
                String[] names = new String[plugins.size()];
                for (int i = 0; i < plugins.size(); i++) {
                    names[i] = plugins.get(i).getName();
                }
                return names;
            }
        }

        public void destroy() {
            try {
                mbeanServer.unregisterMBean(mbeanName);
            } catch (Exception e) {
                log.error("Unexpected error unregistering plugin mbean.", e);
            }
        }
    }

    private static class PluginEntry<T> implements PluginHandle<T> {
        private final String name;
        private final T plugin;

        public PluginEntry(String name, T plugin) {
            this.name = name;
            this.plugin = plugin;
        }

        public boolean equals(Object obj) {
            if (!(obj instanceof PluginEntry))
                return false;

            PluginEntry other = (PluginEntry)obj;
            return other.plugin == plugin && other.name.equals(name);
        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + (plugin != null ? plugin.hashCode() : 0);
            return result;
        }

        @Override
        public T getPlugin() {
            return plugin;
        }

        @Override
        public String getName() {
            return name;
        }
    }

}
