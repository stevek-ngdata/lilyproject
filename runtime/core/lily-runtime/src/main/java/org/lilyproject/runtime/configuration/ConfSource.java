package org.lilyproject.runtime.configuration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.runtime.conf.XmlConfBuilder;
import org.lilyproject.runtime.conf.ConfImpl;
import org.lilyproject.util.io.IOUtils;

import java.io.InputStream;
import java.io.IOException;
import java.util.*;

public abstract class ConfSource {
    private final String location;
    private Map<String, CachedConfig> confs = new HashMap<String, CachedConfig>();
    protected static final String CONFIG_FILE_EXT = ".xml";
    private final Log log = LogFactory.getLog(getClass());

    public ConfSource(String location) {
        this.location = location;
    }

    /**
     * Returns all the available configuration paths.
     */
    public Collection<String> getPaths() {
        // Only return paths of valid configurations
        Set<String> paths = new HashSet<String>();
        for (Map.Entry<String, CachedConfig> entry : confs.entrySet()) {
            if (entry.getValue().state == ConfigState.OK)
                paths.add(entry.getKey());
        }
        return paths;
    }

    /**
     * Returns the configuration at a certain path.
     */
    public CachedConfig get(String path) {
        return confs.get(path);
    }

    public void refresh() {
        // Search all config files on disk
        List<ConfigPath> configPaths = getConfigFiles();

        // Delete configs from cache which don't exist on disk anymore
        Iterator<String> currentEntriesIt = confs.keySet().iterator();
        while (currentEntriesIt.hasNext()) {
            String path = currentEntriesIt.next();
            boolean found = false;
            for (ConfigPath configPath : configPaths) {
                if (configPath.path.equals(path)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                currentEntriesIt.remove();
                if (log.isDebugEnabled())
                    log.debug("Configuration: detected removed config " + path + " in " + location);
            }
        }

        // Add/update configs
        for (ConfigPath configPath : configPaths) {
            CachedConfig cachedConfig = confs.get(configPath.path);
            if (cachedConfig == null || cachedConfig.lastModified != configPath.file.lastModified()) {
                if (log.isDebugEnabled())
                    log.debug("Configuration: detected updated or added config " + configPath.path + " in " + location);
                long lastModified = configPath.file.lastModified();
                ConfImpl conf = parseConfiguration(configPath.file);
                cachedConfig = new CachedConfig();
                cachedConfig.lastModified = lastModified;
                cachedConfig.conf = conf;
                cachedConfig.state = conf == null ? ConfigState.ERROR : ConfigState.OK;
                confs.put(configPath.path, cachedConfig);
            }
        }
    }

    protected static class ConfigPath {
        String path;
        ConfigFile file;

        public ConfigPath(String path, ConfigFile file) {
            this.path = path;
            this.file = file;
        }
    }

    public static interface ConfigFile {
        InputStream getInputStream() throws IOException;

        String getPath();

        long lastModified();
    }

    public static class CachedConfig {
        long lastModified;
        ConfImpl conf;
        ConfigState state;

        public long getLastModified() {
            return lastModified;
        }

        public ConfImpl getConfiguration() {
            return conf;
        }
    }

    enum ConfigState { OK, ERROR }

    protected abstract List<ConfigPath> getConfigFiles();
        
    protected static boolean acceptFileName(boolean isDirectory, String name) {
        if (isDirectory) {
            return !name.startsWith(".") && !name.equals("CVS");
        } else {
            return !name.startsWith(".") && name.endsWith(CONFIG_FILE_EXT);
        }
    }

    private ConfImpl parseConfiguration(ConfigFile file) {
        ConfImpl config = null;
        InputStream is = null;
        try {
            is = file.getInputStream();
            config = XmlConfBuilder.build(is, file.getPath());
        } catch (Throwable e) {
            log.error("Error reading configuration file " + file.getPath(), e);
        } finally {
            IOUtils.closeQuietly(is);
        }
        return config;
    }
}
