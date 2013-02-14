/*
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
package org.lilyproject.runtime;

import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.jci.monitor.FilesystemAlterationMonitor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.runtime.conf.Conf;
import org.lilyproject.runtime.classloading.ClassLoaderBuilder;
import org.lilyproject.runtime.classloading.ClasspathEntry;
import org.lilyproject.runtime.configuration.ConfManager;
import org.lilyproject.runtime.model.ConfigError;
import org.lilyproject.runtime.model.KauriRuntimeModel;
import org.lilyproject.runtime.model.KauriRuntimeModelBuilder;
import org.lilyproject.runtime.model.ModuleDefinition;
import org.lilyproject.runtime.model.SourceLocations;
import org.lilyproject.runtime.module.Module;
import org.lilyproject.runtime.module.ModuleConfig;
import org.lilyproject.runtime.module.build.ModuleBuilder;
import org.lilyproject.runtime.module.build.ModuleConfigBuilder;
import org.lilyproject.runtime.module.javaservice.JavaServiceManager;
import org.lilyproject.runtime.rapi.ConfRegistry;
import org.lilyproject.runtime.rapi.Mode;
import org.lilyproject.runtime.repository.ArtifactNotFoundException;
import org.lilyproject.runtime.repository.ArtifactRepository;
import org.lilyproject.runtime.source.ModuleSourceManager;
import org.lilyproject.util.ArgumentValidator;
import org.lilyproject.util.Version;

/**
 * This is the main entry point of the KauriRuntime.
 *
 * <p>Basic usage:
 * <ul>
 *   <li>Create a {@link KauriRuntimeSettings}
 *   <li>Use {@link #KauriRuntime(KauriRuntimeSettings)}
 *   <li>Call {@link #start()}
 * </ul>
 */
public class KauriRuntime {
    private KauriRuntimeSettings settings;
    private KauriRuntimeModel model;
    private ClassLoader rootClassLoader;
    private List<Module> modules;
    private Map<String, Module> modulesById = new HashMap<String, Module>();
    private List<ModuleConfig> moduleConfigs;
    private JavaServiceManager javaServiceManager;
    private ModuleSourceManager moduleSourceManager;

    private enum LifeCycle { NOT_STARTED, STARTED, STOPPED }
    private LifeCycle state = LifeCycle.NOT_STARTED;
    private Mode mode = Mode.getDefault();
    private FilesystemAlterationMonitor fam = new FilesystemAlterationMonitor();

    protected final Log infolog = LogFactory.getLog(INFO_LOG_CATEGORY);

    public static final String INFO_LOG_CATEGORY = "org.lilyproject.runtime.info";
    public static final String CLASSLOADING_LOG_CATEGORY = "org.lilyproject.runtime.classloading-info";
    public static final String CLASSLOADING_REPORT_CATEGORY = "org.lilyproject.runtime.classloading-report";

    public KauriRuntime(KauriRuntimeSettings settings) {
        ArgumentValidator.notNull(settings, "settings");

        if (settings.getRepository() == null)
            throw new KauriRTException("KauriRuntimeSettings should contain an artifact repository.");

        if (settings.getConfManager() == null)
            throw new KauriRTException("KauriRuntimeSettings should contain a ConfManager.");

        this.settings = settings;

        this.javaServiceManager = new JavaServiceManager();
        this.moduleSourceManager = new ModuleSourceManager(fam);
    }

    public Mode getMode() {
        return mode;
    }

    public void setMode(Mode mode) {
        if (state.ordinal() > LifeCycle.NOT_STARTED.ordinal())
            throw new IllegalStateException("Runtime mode canot be changed once started.");

        this.mode = mode;
    }


    public KauriRuntimeModel buildModel() {
        // Init the configuration manager
        ConfManager confManager = settings.getConfManager();
        confManager.initRuntimeConfig();

        ConfRegistry confRegistry = confManager.getRuntimeConfRegistry();
        
        return buildModel(confRegistry);
    }


    private KauriRuntimeModel buildModel(ConfRegistry confRegistry) {
        KauriRuntimeModel newModel;
        if ((settings.getModel() != null)) {
            newModel = settings.getModel();
        } else {
            Conf modulesConf = confRegistry.getConfiguration("wiring", false, false);
            Set<String> disabledModuleIds = settings.getDisabledModuleIds() != null ?
                    settings.getDisabledModuleIds() : Collections.<String>emptySet();
            SourceLocations sourceLocations = settings.getSourceLocations() != null ?
                    settings.getSourceLocations() : new SourceLocations();
            try {
                newModel = KauriRuntimeModelBuilder.build(modulesConf, disabledModuleIds, settings.getRepository(), sourceLocations);
            } catch (Exception e) {
                throw new KauriRTException("Error building the Kauri model from configuration.", e);
            }
        }
        return newModel;
    }

    /**
     * Starts the Kauri Runtime. This will launch all modules (i.e. their Spring containers),
     * and set up the restservices.
     *
     * <p>A KauriRuntime instance can only be started once, even after it has been stopped.
     * Just create a new KauriRuntime instance with the same KauriRuntimeConfig if you want
     * to (re)start another instance.
     */
    public void start() throws KauriRTException, MalformedURLException, ArtifactNotFoundException {
        // a KauriRuntime object cannot be started twice, even if it has been stopped in between
        if (state.ordinal() > LifeCycle.NOT_STARTED.ordinal())
            throw new KauriRTException("This Kauri Runtime instance has already been started before.");
        state = LifeCycle.STARTED;

        // Init the configuration manager
        ConfManager confManager = settings.getConfManager();
        confManager.initRuntimeConfig();

        ConfRegistry confRegistry = confManager.getRuntimeConfRegistry();

        this.model = buildModel(confRegistry);
        
        // Validate the config
        List<ConfigError> configErrors = new ArrayList<ConfigError>();
        model.validate(configErrors);
        if (configErrors.size() > 0) {
            StringBuilder errorMsg = new StringBuilder();
            String subject = configErrors.size() == 1 ? "error" : "errors";
            errorMsg.append("Encountered the following ").append(subject).append(" in the runtime configuration: ");
            for (int i = 0; i < configErrors.size(); i++) {
                if (i > 0)
                    errorMsg.append(", ");
                errorMsg.append(configErrors.get(i).getMessage());
            }
            throw new KauriRTException(errorMsg.toString());
        }

        moduleConfigs = new ArrayList<ModuleConfig>();

        // First read the configuration of each module, and do some classpath checks
        if (infolog.isInfoEnabled())
            infolog.info("Reading module configurations of " + model.getModules().size() + " modules.");
        for (ModuleDefinition entry : model.getModules()) {
            if (infolog.isInfoEnabled())
                infolog.debug("Reading module config " + entry.getId() + " - " + entry.getFile().getAbsolutePath());
            ModuleConfig moduleConf = ModuleConfigBuilder.build(entry, this);
            moduleConfigs.add(moduleConf);
        }

        // Check / build class path configurations
        Conf classLoadingConf = confRegistry.getConfiguration("classloading");
        List<ClasspathEntry> sharedClasspath = ClassLoaderConfigurer.configureClassPaths(moduleConfigs,
                settings.getEnableArtifactSharing(), classLoadingConf);

        // Construct the shared classloader
        infolog.debug("Creating shared classloader");

        rootClassLoader = ClassLoaderBuilder.build(sharedClasspath, this.getClass().getClassLoader(), settings.getRepository());

        // Construct the classloaders of the various modules
        List<ClassLoader> moduleClassLoaders = new ArrayList<ClassLoader>();
        for (ModuleConfig cfg : moduleConfigs) {
            ClassLoader classLoader = cfg.getClassLoadingConfig().getClassLoader(getClassLoader());
            moduleClassLoaders.add(classLoader);
        }

        // Initialize the ConfManager for the modules configuration
        confManager.initModulesConfig(moduleConfigs);

        // Create the modules
        infolog.info("Starting the modules.");
        
        modules = new ArrayList<Module>(model.getModules().size());
        for (int i = 0; i < moduleConfigs.size(); i++) {
            ModuleConfig moduleConfig = moduleConfigs.get(i);
            Module module = ModuleBuilder.build(moduleConfig, moduleClassLoaders.get(i), this);
            modules.add(module);
            modulesById.put(module.getDefinition().getId(), module);
        }

        // Start the FAM, conf manager refreshing
        fam.start();
        confManager.startRefreshing();

        infolog.info("Runtime initialisation finished.");
    }

    public List<ModuleConfig> getModuleConfigs() {
        return moduleConfigs;
    }

    public ArtifactRepository getArtifactRepository() {
        return settings.getRepository();
    }

    public ClassLoader getClassLoader() {
        return rootClassLoader;
    }

    public JavaServiceManager getJavaServiceManager() {
        return javaServiceManager;
    }
    
    public ModuleSourceManager getModuleSourceManager() {
        return moduleSourceManager;
    }

    public List<Module> getModules() {
        return modules;
    }

    public Module getModuleById(String moduleId) {
        return modulesById.get(moduleId);
    }

    public KauriRuntimeSettings getSettings() {
        return settings;
    }

    public KauriRuntimeModel getModel() {
        return model;
    }

    public void stop() {
        if (state != LifeCycle.STARTED)
            throw new KauriRTException("Cannot stop the runtime, it is in state " + state + " instead of " + LifeCycle.STARTED);

        // TODO temporarily disabled because FAM.stop() is slow
        // See JCI jira patch: https://issues.apache.org/jira/browse/JCI-57 (the newer one in commons-io has the same problem)
        // fam.stop();
        // Added the following workaround:
        if (System.getSecurityManager() == null) {
            try {
                Field famRunningField = fam.getClass().getDeclaredField("running");
                famRunningField.setAccessible(true);
                Field threadField = fam.getClass().getDeclaredField("thread");
                threadField.setAccessible(true);
                Thread famThread = (Thread)threadField.get(fam);
                if (famThread != null) {
                    famRunningField.setBoolean(fam, false);
                    famThread.interrupt();
                    fam.stop();
                }
            } catch (Exception e) {
                infolog.error("Error stopping FilesystemAlterationMonitor", e);
            }
        } else {
            infolog.warn("Unable to stop the FilesystemAlterationMonitor using workaround since a security manager is installed.");
        }

        // In case starting the runtime failed, modules might be null
        if (modules != null) {
            infolog.info("Shutting down the modules.");
            List<Module> reversedModules = new ArrayList<Module>(this.modules);
            Collections.reverse(reversedModules);
            this.modules = null;
            this.javaServiceManager.stop();

            for (Module module : reversedModules) {
                try {
                    module.shutdown();
                } catch (Throwable t) {
                    infolog.error("Error shutting down module " + module.getDefinition().getId(), t);
                }
            }
        }

        settings.getConfManager().shutdown();

    }

    public static String getVersion() {
        return Version.readVersion("org.lilyproject", "lily-runtime");
    }

    public ConfManager getConfManager() {
        return settings.getConfManager();
    }

}
