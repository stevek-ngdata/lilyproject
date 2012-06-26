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
package org.lilyproject.server.modules.repository;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.kauriproject.conf.Conf;
import org.lilyproject.plugin.PluginHandle;
import org.lilyproject.plugin.PluginRegistry;
import org.lilyproject.plugin.PluginUser;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.impl.HBaseRepository;
import org.lilyproject.repository.spi.RecordUpdateHook;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RecordUpdateHookActivator implements PluginUser<RecordUpdateHook> {
    private Map<String, RecordUpdateHook> hooks = new HashMap<String, RecordUpdateHook>();
    private PluginRegistry pluginRegistry;
    private List<String> configuredHooks = new ArrayList<String>();
    private Log log = LogFactory.getLog(getClass());

    public RecordUpdateHookActivator(PluginRegistry pluginRegistry, Conf conf) {
        this.pluginRegistry = pluginRegistry;

        for (Conf hookConf : conf.getChild("updateHooks").getChildren("updateHook")) {
            configuredHooks.add(hookConf.getValue());
        }
    }

    @PostConstruct
    public void init() {
        pluginRegistry.setPluginUser(RecordUpdateHook.class, this);
    }

    @PreDestroy
    public void destroy() {
        pluginRegistry.unsetPluginUser(RecordUpdateHook.class, this);
    }

    @Override
    public void pluginAdded(PluginHandle<RecordUpdateHook> pluginHandle) {
        hooks.put(pluginHandle.getName(), pluginHandle.getPlugin());
    }

    @Override
    public void pluginRemoved(PluginHandle<RecordUpdateHook> pluginHandle) {
    }

    public Repository activateUpdateHooks(HBaseRepository repository) {
        // We don't use all the registered update-hook plugins, but only those the user
        // activated through the configuration, and in the order specified in the
        // configuration.
        //
        // An exception are Lily's core hooks, which we recognize by their name prefix,
        // which are always activated.

        List<RecordUpdateHook> updateHooks = new ArrayList<RecordUpdateHook>(configuredHooks.size());

        // Handle built-in hooks
        for (Map.Entry<String, RecordUpdateHook> entry : hooks.entrySet()) {
            if (entry.getKey().startsWith("org.lilyproject")) {
                updateHooks.add(entry.getValue());
            }
        }

        // Handle user-configured hooks
        for (String name : configuredHooks) {
            if (!name.startsWith("org.lilyproject")) {
                RecordUpdateHook updateHook = this.hooks.get(name);
                if (updateHook == null) {
                    throw new RuntimeException("No record update hook registered with the name '" + name + "'");
                }
                updateHooks.add(updateHook);
            } else {
                log.error("Custom update hooks shouldn't start their name with 'org.lilyproject'.");
            }
        }

        repository.setRecordUpdateHooks(updateHooks);

        log.info("The active record update hooks are: " + configuredHooks);

        return repository;
    }

}
