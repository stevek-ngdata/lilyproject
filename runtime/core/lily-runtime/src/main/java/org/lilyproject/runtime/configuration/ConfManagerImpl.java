/*
 * Copyright 2013 NGDATA nv
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
package org.lilyproject.runtime.configuration;

import org.lilyproject.runtime.rapi.ConfRegistry;
import org.lilyproject.runtime.module.ModuleConfig;
import org.lilyproject.runtime.conf.Conf;

import java.util.*;
import java.util.concurrent.*;
import java.io.File;

public class ConfManagerImpl implements ConfManager {
    private List<File> configLocations;
    private Map<String, ConfRegistryImpl> registries = new HashMap<String, ConfRegistryImpl>();
    private ScheduledExecutorService executor;
    private State state = State.STOPPED;

    private static final String RUNTIME_CONF_NAME = "runtime";

    private enum State { STOPPED, RUNTIME_CONF_AVAILABLE, MODULES_CONF_AVAILABLE }

    public ConfManagerImpl() {
        this(Collections.<File>emptyList());
    }

    public ConfManagerImpl(List<File> configLocations) {
        this.configLocations = configLocations;
    }

    public void shutdown() {
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }
        registries.clear();
        state = State.STOPPED;
    }

    public void initRuntimeConfig() {
        if (state.ordinal() >= State.RUNTIME_CONF_AVAILABLE.ordinal()) {
            throw new IllegalStateException("ConfManager: runtime config already initialized.");
        }

        List<ConfSource> sources = new ArrayList<ConfSource>();
        for (File location : configLocations) {
            FilesystemConfSource source = new FilesystemConfSource(new File(location, RUNTIME_CONF_NAME));
            sources.add(source);
        }
        this.registries.put(RUNTIME_CONF_NAME, new ConfRegistryImpl(RUNTIME_CONF_NAME, sources));

        refresh(false);

        state = State.RUNTIME_CONF_AVAILABLE;
    }

    public void initModulesConfig(List<ModuleConfig> moduleConfigs) {
        if (state.ordinal() >= State.MODULES_CONF_AVAILABLE.ordinal()) {
            throw new IllegalStateException("ConfManager: modules config already initialized.");
        }

        for (ModuleConfig moduleConfig : moduleConfigs) {
            List<ConfSource> sources = new ArrayList<ConfSource>();
            for (File location : configLocations) {
                FilesystemConfSource source = new FilesystemConfSource(new File(location, moduleConfig.getId()));
                sources.add(source);
            }
            sources.add(new ModuleSourceConfSource(moduleConfig));

            this.registries.put(moduleConfig.getId(), new ConfRegistryImpl(moduleConfig.getId(), sources));
        }

        refresh(false);

        state = State.MODULES_CONF_AVAILABLE;
    }

    public void startRefreshing() {
        if (executor != null) {
            throw new RuntimeException("Unexpected situation: executor is not null. Are you reusing the same "
                    + getClass().getName() + " instance concurrently or sequentially without proper shutdown?");
        }

        executor = Executors.newScheduledThreadPool(1);

        Conf conf = getConfRegistry(RUNTIME_CONF_NAME).getConfiguration("configuration", true);
        Conf reloadingConf = conf.getChild("reloading");
        boolean reloadingEnabled = reloadingConf.getAttributeAsBoolean("enabled", true);
        int delay = reloadingConf.getAttributeAsInteger("delay", 5000);

        if (reloadingEnabled) {
            executor.scheduleWithFixedDelay(new Runnable() {
                public void run() {
                    refresh(true);
                }
            }, delay, delay, TimeUnit.MILLISECONDS);
        }
    }

    private void refresh(boolean notifyListeners) {
        final List<Runnable> notifyTasks = new ArrayList<Runnable>();

        for (ConfRegistryImpl registry : registries.values()) {
            Runnable runnable = registry.refresh();
            if (runnable != null) {
                notifyTasks.add(runnable);
            }
        }

        if (notifyListeners) {
            for (Runnable runnable : notifyTasks) {
                runnable.run();
            }
        }
    }

    public ConfRegistry getConfRegistry(String moduleId) {
        if (state.ordinal() < State.MODULES_CONF_AVAILABLE.ordinal()) {
            throw new IllegalStateException("ConfManager: modules configuration not yet available.");
        }
        return registries.get(moduleId);
    }

    public ConfRegistry getRuntimeConfRegistry() {
        if (state.ordinal() < State.RUNTIME_CONF_AVAILABLE.ordinal()) {
            throw new IllegalStateException("ConfManager: runtime configuration not yet available.");
        }
        return registries.get(RUNTIME_CONF_NAME);
    }
}
