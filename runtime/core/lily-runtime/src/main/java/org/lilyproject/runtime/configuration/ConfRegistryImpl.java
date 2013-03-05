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
import org.lilyproject.runtime.rapi.ConfListener;
import org.lilyproject.runtime.rapi.ConfListener.ChangeType;
import org.lilyproject.runtime.rapi.ConfNotFoundException;
import org.lilyproject.runtime.configuration.ConfSource.CachedConfig;
import org.lilyproject.runtime.conf.Conf;
import org.lilyproject.runtime.conf.ConfImpl;
import org.lilyproject.util.location.LocationImpl;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ConfRegistryImpl implements ConfRegistry {
    private String name;
    private List<ConfSource> sources;

    private ConfPath root = new ConfPath();
    private List<ListenerHandle> listeners = new ArrayList<ListenerHandle>();
    private Log log = LogFactory.getLog(getClass());

    private static int INITIAL_CACHE_SIZE = 16;
    private static float CACHE_LOAD_FACTOR = .75f;
    private static int CACHE_CONCURRENCY_LEVEL = 1;

    private static final Conf EMPTY_CONF = new ConfImpl("<empty>", new LocationImpl(null, "<generated>", -1, -1));

    public ConfRegistryImpl(String name, List<ConfSource> sources) {
        this.name = name;
        this.sources = sources;
    }

    /**
     * Checks for configuration changes on disk and reloads as necessary.
     *
     * <p>If there are listeners to be notified of changes, this will not happen immediately,
     * but a Runnable will be returned, executing this Runnable will notify the listeners.
     * The purpose of this is that the notification of listeners would not block the
     * refreshing performed by the ConfManager (ConfListener's should return quickly,
     * but you never know). The ConfManager might decide to either run the Runnable
     * after refreshing all ConfRegistry's, possibly executing them on a background thread.
     */
    public Runnable refresh() {
        // Refresh the lower-level conf registries
        for (ConfSource source : sources) {
            source.refresh();
        }

        // Determine list of all available configuration paths
        Set<String> paths = new HashSet<String>();
        for (ConfSource source : sources) {
            paths.addAll(source.getPaths());
        }

        Map<ChangeType, Set<String>> changesByType = new EnumMap<ChangeType, Set<String>>(ChangeType.class);
        for (ChangeType changeType : ChangeType.values()) {
            changesByType.put(changeType, new HashSet<String>());
        }

        // Delete confs which no longer exist
        root.removeUnexistingChildren(paths, "", changesByType);

        // Update/add the existing/new confs
        for (String path : paths) {
            String[] parsedPath = path.split("/"); // we assume our sources only deliver clean paths without empty path segments
            ConfPath confPath = root.getConfPath(parsedPath, 0);
            boolean changes;

            if (confPath != null && confPath.conf != null) {
                changes = confPath.conf.refresh();
            } else {
                MergedConfig config = new MergedConfig(path);
                config.refresh();
                root.addConf(parsedPath, 0, config, changesByType);
                changes = true; // a new config is always a change
            }

            if (changes) {
                changesByType.get(ChangeType.CONF_CHANGE).add(path);
            }
        }

        return getListenerNotificationRunnable(changesByType);
    }

    public Conf getConfiguration(String path) {
        return getConfiguration(path, true);
    }

    public Conf getConfiguration(String path, boolean create) {
        return getConfiguration(path, create, true);
    }

    public Conf getConfiguration(String path, boolean create, boolean silent) {
        ConfPath confPath = root.getConfPath(parsePath(path), 0);
        MergedConfig mergedConfig = confPath == null ? null : confPath.conf;

        if (mergedConfig == null || mergedConfig.conf == null /* will never be the case, but anyway */) {
            if (create) {
                return EMPTY_CONF;
            } else if (silent) {
                return null;
            } else {
                throw new ConfNotFoundException("Configuration \"" + path + "\" not found.");
            }
        }

        return mergedConfig.conf;
    }

    private String[] parsePath(String path) {
        String[] parts = path.split("/");
        List<String> result = new ArrayList<String>(parts.length);

        for (String part : parts) {
            part = part.trim();
            if (part.length() > 0)
                result.add(part);
        }

        return result.toArray(new String[result.size()]);
    }

    private String formatPath(String[] parts, int upTo) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i <= upTo; i++) {
            if (i > 0)
                builder.append("/");
            builder.append(parts[i]);
        }
        return builder.toString();
    }

    public Collection<String> getConfigurations(String path) {
        ConfPath confPath = root.getConfPath(parsePath(path), 0);
        if (confPath != null) {
            Collection<String> childConfNames = new ArrayList<String>(confPath.children.size());
            for (Map.Entry<String, ConfPath> child : confPath.children.entrySet()) {
                if (child.getValue().conf != null) {
                    childConfNames.add(child.getKey());
                }
            }
            return childConfNames;
        } else {
            return Collections.emptySet();
        }
    }

    public synchronized void addListener(ConfListener listener, String path, ConfListener.ChangeType... types) {
        EnumSet<ChangeType> typesSet = EnumSet.noneOf(ChangeType.class);
        typesSet.addAll(Arrays.asList(types));

        listeners.add(new ListenerHandle(listener, path, typesSet));
    }

    public void removeListener(ConfListener listener) {
        Iterator<ListenerHandle> listenersIt = listeners.iterator();
        while (listenersIt.hasNext()) {
            ListenerHandle handle = listenersIt.next();
            if (handle.listener == listener) {
                listenersIt.remove();
            }
        }
    }

    private class MergedConfig {
        private Long[] lastModifieds;
        private Conf conf;
        private String path;

        public MergedConfig(String path) {
            this.path = path;
            this.lastModifieds = new Long[sources.size()];
        }

        public boolean refresh() {
            boolean changes = false;
            for (int i = 0; i < sources.size(); i++) {
                CachedConfig cachedConfig = sources.get(i).get(path);
                if (cachedConfig == null) {
                    if (lastModifieds[i] != null) {
                        lastModifieds[i] = null;
                        changes = true;
                    }
                } else if (lastModifieds[i] == null || cachedConfig.lastModified != lastModifieds[i]) {
                    lastModifieds[i] = cachedConfig.lastModified;
                    changes = true;
                }
            }

            if (changes) {
                List<ConfImpl> confs = new ArrayList<ConfImpl>();
                for (ConfSource source : sources) {
                    CachedConfig cachedConfig = source.get(path);
                    if (cachedConfig != null) {
                        confs.add(cachedConfig.conf);
                    }
                }

                // Merge the confs
                while (confs.size() >= 2) {
                    // Replace the last 2 confs in the list by a merged conf.
                    ConfImpl parent = confs.remove(confs.size() - 1);
                    ConfImpl child = confs.remove(confs.size() - 1);
                    child.inherit(parent);
                    confs.add(child);
                }

                conf = confs.get(0);
            }

            return changes;
        }

        public Conf getConfiguration() {
            return conf;
        }
    }

    private class ConfPath {
        private Map<String, ConfPath> children = new ConcurrentHashMap<String, ConfPath>(
            INITIAL_CACHE_SIZE,CACHE_LOAD_FACTOR, CACHE_CONCURRENCY_LEVEL);
        private MergedConfig conf;

        public void removeUnexistingChildren(Set<String> availablePaths, String currentPath,
                Map<ChangeType, Set<String>> changes) {

            Iterator<Map.Entry<String, ConfPath>> childrenIt = children.entrySet().iterator();
            while (childrenIt.hasNext()) {
                Map.Entry<String, ConfPath> childEntry = childrenIt.next();
                ConfPath child = childEntry.getValue();

                int oldConfChildCount = child.getConfChildren().size();

                String childPath = currentPath + childEntry.getKey();

                child.removeUnexistingChildren(availablePaths, childPath + "/", changes);

                if (!availablePaths.contains(childPath) && child.conf != null) {
                    child.conf = null;
                    changes.get(ChangeType.CONF_CHANGE).add(childPath);
                }

                if (child.children.size() == 0 && child.conf == null) {
                    if (oldConfChildCount > 0)
                        changes.get(ChangeType.PATH_CHANGE).add(childPath);
                    childrenIt.remove();
                } else if (child.getConfChildren().size() != oldConfChildCount) {
                    changes.get(ChangeType.PATH_CHANGE).add(childPath);
                }
            }
        }

        public ConfPath getConfPath(String[] path, int pathPos) {
            ConfPath child = children.get(path[pathPos]);
            if (child == null) {
                return null;
            } else if (pathPos == path.length -1) {
                return child;
            } else {
                return child.getConfPath(path, pathPos + 1);
            }
        }

        public void addConf(String[] path, int pathPos, MergedConfig conf, Map<ChangeType, Set<String>> changes) {
            if (pathPos == path.length - 1) {
                ConfPath confPath = new ConfPath();
                confPath.conf = conf;
                children.put(path[pathPos], confPath);
            } else {
                ConfPath child = children.get(path[pathPos]);
                if (child == null) {
                    child = new ConfPath();
                    children.put(path[pathPos], child);

                    if (pathPos == path.length - 2) {
                        // We created a new path which will contain a conf, we need to notify of this
                        changes.get(ChangeType.PATH_CHANGE).add(formatPath(path, pathPos));
                    }
                }
                child.addConf(path, pathPos + 1, conf, changes);
            }
        }

        /**
         * Return the names of the children who have a conf, thus are not just a path.
         */
        public Collection<String> getConfChildren() {
            Collection<String> childConfNames = new ArrayList<String>(children.size());
            for (Map.Entry<String, ConfPath> child : children.entrySet()) {
                if (child.getValue().conf != null) {
                    childConfNames.add(child.getKey());
                }
            }
            return childConfNames;
        }
    }

    private static class ListenerHandle {
        private String path;
        private ConfListener listener;
        private Set<ChangeType> types;

        public ListenerHandle(ConfListener listener, String path, Set<ChangeType> types) {
            this.listener = listener;
            this.path = path;
            this.types = types;
        }
    }

    private Runnable getListenerNotificationRunnable(final Map<ChangeType, Set<String>> changes) {
        boolean anyChanges = false;
        for (ChangeType changeType : ChangeType.values()) {
            if (changes.get(changeType).size() > 0) {
                anyChanges = true;
                break;
            }
        }

        if (!anyChanges)
            return null;

        return new Runnable() {
            public void run() {
                notifyListeners(changes);
            }
        };
    }

    private void notifyListeners(Map<ChangeType, Set<String>> changes) {
        if (log.isDebugEnabled())
            log.debug("There are configuration changes in " + name + ", will notify " + listeners.size() + " listeners.");

        for (ListenerHandle listener : listeners) {
            if (listener.path == null) {
                // No path specified, listener wants to listen to changes for all paths
                for (ChangeType changeType : ChangeType.values()) {
                    if (listener.types.contains(changeType)) {
                        notifyChanges(changes.get(changeType), listener, changeType);
                    }
                }
            } else {
                for (ChangeType changeType : ChangeType.values()) {
                    if (listener.types.contains(changeType) && changes.get(changeType).contains(listener.path)) {
                        notifyChanges(Collections.singleton(listener.path), listener, changeType);
                    }
                }
            }
        }
    }

    private void notifyChanges(Set<String> paths, ListenerHandle listener, ChangeType changeType) {
        for (String path : paths) {
            try {
                if (log.isDebugEnabled()) {
                    log.debug("Notifying changes of type " + changeType + " for path " + path + " in " + name
                            + " to listener " + listener.listener);
                }
                listener.listener.confAltered(path, changeType);
            } catch (Throwable t) {
                log.error("Error while notifying configuration change of type " + changeType + " for path \""
                        + path + "\" in " + name, t);
            }
        }
    }

}
