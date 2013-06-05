/*
 * Copyright 2013 NGDATA nv
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
package org.lilyproject.repository.model.impl;

import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.lilyproject.repository.model.api.RepositoryDefinition;
import org.lilyproject.repository.model.api.RepositoryExistsException;
import org.lilyproject.repository.model.api.RepositoryModel;
import org.lilyproject.repository.model.api.RepositoryModelEvent;
import org.lilyproject.repository.model.api.RepositoryModelEventType;
import org.lilyproject.repository.model.api.RepositoryModelException;
import org.lilyproject.repository.model.api.RepositoryModelListener;
import org.lilyproject.repository.model.api.RepositoryNotFoundException;
import org.lilyproject.util.zookeeper.ZkUtil;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.zookeeper.Watcher.Event.EventType.NodeChildrenChanged;
import static org.apache.zookeeper.Watcher.Event.EventType.NodeDataChanged;
import static org.lilyproject.repository.model.api.RepositoryDefinition.RepositoryLifecycleState;

public class RepositoryModelImpl implements RepositoryModel {
    private Map<String, RepositoryDefinition> repos = new HashMap<String, RepositoryDefinition>();

    /**
     * Lock to be obtained when changing repos or when dispatching events. This assures the correct functioning
     * of methods like {@link #getRepositories(org.lilyproject.repository.model.api.RepositoryModelListener)}.
     */
    private final Object reposLock = new Object();

    private ZooKeeperItf zk;

    private final Set<RepositoryModelListener> listeners =
            Collections.newSetFromMap(new IdentityHashMap<RepositoryModelListener, Boolean>());

    private Watcher zkWatcher;

    private boolean closed = false;

    private static final String REPOSITORY_COLLECTION_PATH = "/lily/repositorymodel/repositories";

    private static final String REPOSITORY_COLLECTION_PATH_SLASH = REPOSITORY_COLLECTION_PATH + "/";

    private final Log log = LogFactory.getLog(getClass());

    /**
     * The default repository always exists and is always in state active.
     */
    private static final RepositoryDefinition DEFAULT_REPOSITORY = new RepositoryDefinition("default",
            RepositoryLifecycleState.ACTIVE);

    public RepositoryModelImpl(ZooKeeperItf zk) throws KeeperException, InterruptedException {
        this.zk = zk;
        init();
    }

    public void init() throws KeeperException, InterruptedException {
        ZkUtil.createPath(zk, REPOSITORY_COLLECTION_PATH);
        assureDefaultRepositoryExists();
        zkWatcher = new RepositoryZkWatcher();
        zk.addDefaultWatcher(zkWatcher);
        refresh();
    }

    private void assureDefaultRepositoryExists() throws KeeperException, InterruptedException {
        byte[] repoBytes = RepositoryDefinitionJsonSerDeser.INSTANCE.toJsonBytes(DEFAULT_REPOSITORY);
        try {
            zk.create(REPOSITORY_COLLECTION_PATH + "/" + DEFAULT_REPOSITORY.getName(), repoBytes, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException e) {
            // it already exists, fine
        }
    }

    public void close() {
        zk.removeDefaultWatcher(zkWatcher);
        closed = true;
    }

    @Override
    public void create(String repositoryName) throws RepositoryExistsException, InterruptedException, RepositoryModelException {
        if (!RepoDefUtil.isValidRepositoryName(repositoryName)) {
            throw new IllegalArgumentException(String.format("'%s' is not a valid repository name. "
                    + RepoDefUtil.VALID_NAME_EXPLANATION, repositoryName));
        }
        RepositoryDefinition repoDef = new RepositoryDefinition(repositoryName, RepositoryLifecycleState.CREATE_REQUESTED);
        byte[] repoBytes = RepositoryDefinitionJsonSerDeser.INSTANCE.toJsonBytes(repoDef);
        try {
            zk.create(REPOSITORY_COLLECTION_PATH + "/" + repositoryName, repoBytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException e) {
            throw new RepositoryExistsException("Can't create repository, a repository with this name already exists: "
                    + repositoryName);
        } catch (KeeperException e) {
            throw new RepositoryModelException(e);
        }
    }

    private void disallowDefaultRepository(String repositoryName) throws RepositoryModelException {
        if (DEFAULT_REPOSITORY.getName().equals(repositoryName)) {
            throw new RepositoryModelException("This operation is not allowed on the default repository.");
        }
    }

    @Override
    public void delete(String repositoryName)
            throws InterruptedException, RepositoryModelException, RepositoryNotFoundException {
        disallowDefaultRepository(repositoryName);
        RepositoryDefinition repoDef = new RepositoryDefinition(repositoryName, RepositoryLifecycleState.DELETE_REQUESTED);
        try {
            storeRepository(repoDef);
        } catch (KeeperException.NoNodeException e) {
            throw new RepositoryNotFoundException("Can't delete-request repository, a repository with this name doesn't exist: "
                    + repoDef.getName());
        } catch (KeeperException e) {
            throw new RepositoryModelException(e);
        }
    }

    @Override
    public void deleteDirect(String repositoryName)
            throws InterruptedException, RepositoryModelException, RepositoryNotFoundException {
        disallowDefaultRepository(repositoryName);
        try {
            zk.delete(REPOSITORY_COLLECTION_PATH + "/" + repositoryName, -1);
        } catch (KeeperException.NoNodeException e) {
            throw new RepositoryNotFoundException("Can't delete repository, a repository with this name doesn't exist: "
                    + repositoryName);
        } catch (KeeperException e) {
            throw new RepositoryModelException("Error deleting repository.", e);
        }
    }

    @Override
    public void updateRepository(RepositoryDefinition repoDef)
            throws InterruptedException, RepositoryModelException,RepositoryNotFoundException {
        disallowDefaultRepository(repoDef.getName());
        try {
            storeRepository(repoDef);
        } catch (KeeperException.NoNodeException e) {
            throw new RepositoryNotFoundException("Can't update repository, a repository with this name doesn't exist: "
                    + repoDef.getName());
        } catch (KeeperException e) {
            throw new RepositoryModelException(e);
        }
    }

    public void storeRepository(RepositoryDefinition repoDef) throws InterruptedException, KeeperException {
        byte[] repoBytes = RepositoryDefinitionJsonSerDeser.INSTANCE.toJsonBytes(repoDef);
        zk.setData(REPOSITORY_COLLECTION_PATH + "/" + repoDef.getName(), repoBytes, -1);
    }

    @Override
    public Set<RepositoryDefinition> getRepositories() throws RepositoryModelException, InterruptedException {
        try {
            return new HashSet<RepositoryDefinition>(loadRepositories(false).values());
        } catch (KeeperException e) {
            throw new RepositoryModelException("Error loading repositories.", e);
        }
    }

    @Override
    public RepositoryDefinition getRepository(String repositoryName)
            throws InterruptedException, RepositoryModelException, RepositoryNotFoundException {
        try {
            return loadRepository(repositoryName, false);
        } catch (KeeperException.NoNodeException e) {
            throw new RepositoryNotFoundException("No repository named " + repositoryName, e);
        } catch (KeeperException e) {
            throw new RepositoryModelException("Error loading repository " + repositoryName, e);
        }
    }

    @Override
    public boolean repositoryExistsAndActive(String repositoryName) {
        RepositoryDefinition repoDef = repos.get(repositoryName);
        return repoDef != null && repoDef.getLifecycleState() == RepositoryLifecycleState.ACTIVE;
    }

    @Override
    public boolean repositoryActive(String repositoryName) throws RepositoryNotFoundException {
        RepositoryDefinition repoDef = repos.get(repositoryName);
        if (repoDef == null) {
            throw new RepositoryNotFoundException("No repository named " + repositoryName);
        }
        return repoDef.getLifecycleState() == RepositoryLifecycleState.ACTIVE;
    }

    @Override
    public boolean waitUntilRepositoryInState(String repositoryName, RepositoryLifecycleState state, long timeout)
            throws InterruptedException {
        long waitUntil = System.currentTimeMillis() + timeout;
        while (!repositoryExistsAndActive(repositoryName) && System.currentTimeMillis() < waitUntil) {
            Thread.sleep(50);
        }
        return repositoryExistsAndActive(repositoryName);
    }

    private class RepositoryZkWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            boolean needsRefresh = false;
            if (event.getType() == Event.EventType.None && event.getState() == Event.KeeperState.SyncConnected) {
                needsRefresh = true;
            } else if (NodeChildrenChanged.equals(event.getType()) && event.getPath().equals(REPOSITORY_COLLECTION_PATH)) {
                needsRefresh = true;
            } else if (NodeDataChanged.equals(event.getType()) && event.getPath().startsWith(REPOSITORY_COLLECTION_PATH_SLASH)) {
                needsRefresh = true;
            }

            if (needsRefresh) {
                try {
                    refresh();
                } catch (Throwable t) {
                    log.error("Repository Model: error handling event from ZooKeeper. Event: " + event, t);
                }
            }
        }
    }

    private void refresh() throws KeeperException, InterruptedException {
        List<RepositoryModelEvent> events = new ArrayList<RepositoryModelEvent>();

        synchronized (reposLock) {
            Map<String, RepositoryDefinition> newRepos = loadRepositories(true);

            // Find out changes in repositories
            Set<String> removedRepos = Sets.difference(repos.keySet(), newRepos.keySet());
            for (String id : removedRepos) {
                events.add(new RepositoryModelEvent(RepositoryModelEventType.REPOSITORY_REMOVED, id));
            }

            Set<String> addedRepos = Sets.difference(newRepos.keySet(), repos.keySet());
            for (String id : addedRepos) {
                events.add(new RepositoryModelEvent(RepositoryModelEventType.REPOSITORY_ADDED, id));
            }

            for (RepositoryDefinition repoDef : newRepos.values()) {
                if (repos.containsKey(repoDef.getName()) && !repos.get(repoDef.getName()).equals(repoDef)) {
                    events.add(new RepositoryModelEvent(RepositoryModelEventType.REPOSITORY_UPDATED, repoDef.getName()));
                }
            }

            repos = newRepos;
        }

        notifyListeners(events);
    }

    private Map<String, RepositoryDefinition> loadRepositories(boolean watch) throws KeeperException, InterruptedException {
        Map<String, RepositoryDefinition> repositories = new HashMap<String, RepositoryDefinition>();
        List<String> children = zk.getChildren(REPOSITORY_COLLECTION_PATH, watch ? zkWatcher : null);
        for (String child : children) {
            repositories.put(child, loadRepository(child, watch));
        }
        return repositories;
    }

    private RepositoryDefinition loadRepository(String name, boolean watch) throws KeeperException, InterruptedException {
        byte[] repoJson = zk.getData(REPOSITORY_COLLECTION_PATH + "/" + name, watch ? zkWatcher : null, new Stat());
        return RepositoryDefinitionJsonSerDeser.INSTANCE.fromJsonBytes(name, repoJson);
    }

    private void notifyListeners(List<RepositoryModelEvent> events) {
        if (closed) {
            // Stop dispatching events once closed
            return;
        }

        for (RepositoryModelEvent event : events) {
            for (RepositoryModelListener listener : listeners) {
                listener.process(event);
            }
        }
    }

    @Override
    public Set<RepositoryDefinition> getRepositories(RepositoryModelListener listener) {
        synchronized (reposLock) {
            registerListener(listener);
            return new HashSet<RepositoryDefinition>(repos.values());
        }
    }

    @Override
    public void registerListener(RepositoryModelListener listener) {
        synchronized (reposLock) {
            listeners.add(listener);
        }
    }

    @Override
    public void unregisterListener(RepositoryModelListener listener) {
        listeners.remove(listener);
    }
}
