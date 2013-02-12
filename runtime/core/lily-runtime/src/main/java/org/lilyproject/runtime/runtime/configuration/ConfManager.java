package org.lilyproject.runtime.runtime.configuration;

import org.lilyproject.runtime.runtime.rapi.ConfRegistry;
import org.lilyproject.runtime.runtime.module.ModuleConfig;

import java.util.List;

/**
 * Provides configuration data, both for the core Kauri Runtime as well
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
