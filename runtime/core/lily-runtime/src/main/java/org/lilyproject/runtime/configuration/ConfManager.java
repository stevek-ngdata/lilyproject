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

import java.util.List;

/**
 * Provides configuration data, both for the core Lily Runtime as well
 * as for modules.
 *
 * <p>The Runtime will take the supplied ConfManager through the following
 * lifecycle:
 *
 * <ul>
 * <li>{@link #initRuntimeConfig()} is called. After this, the
 * ConfManager should be able to respond to requests for the runtime
 * configuration, see {@link #getRuntimeConfRegistry}.
 * <li>{@link #initModulesConfig(java.util.List)} is called. After this,
 * the configuration of all modules should be accessible. Note that
 * this initialization is split from the previous one since the list
 * of modules is only known after retrieving them from the runtime
 * configuration.
 * <li>{@link #startRefreshing} is called. This is called after all
 * modules are started, and allows the ConfManager to start notifying
 * {@link org.lilyproject.runtime.rapi.ConfListener}s.
 * <li>When the runtime is stopped, {@link #shutdown} is called. This
 * could e.g. be used to stop background threads.
 * </ul>
 */
public interface ConfManager {
    void initRuntimeConfig();

    void initModulesConfig(List<ModuleConfig> moduleConfigs);

    /**
     * This method is called to allow the ConfManager to start refreshing
     * configuration if it is changed, and notifying listeners of those
     * changes. This will be called after all modules have been started,
     * to avoid notifications to be broadcasted while the system is still
     * being started.
     *
     * <p>This method is guaranteed to be called after {@link #initModulesConfig}.
     */
    void startRefreshing();

    void shutdown();

    ConfRegistry getConfRegistry(String moduleId);

    ConfRegistry getRuntimeConfRegistry();
}
