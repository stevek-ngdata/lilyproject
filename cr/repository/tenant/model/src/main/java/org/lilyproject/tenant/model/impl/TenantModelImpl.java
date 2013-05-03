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
package org.lilyproject.tenant.model.impl;

import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.lilyproject.tenant.model.api.Tenant;
import org.lilyproject.tenant.model.api.TenantExistsException;
import org.lilyproject.tenant.model.api.TenantModel;
import org.lilyproject.tenant.model.api.TenantModelEvent;
import org.lilyproject.tenant.model.api.TenantModelEventType;
import org.lilyproject.tenant.model.api.TenantModelException;
import org.lilyproject.tenant.model.api.TenantModelListener;
import org.lilyproject.tenant.model.api.TenantNotFoundException;
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
import java.util.regex.Pattern;

import static org.apache.zookeeper.Watcher.Event.EventType.NodeChildrenChanged;
import static org.apache.zookeeper.Watcher.Event.EventType.NodeDataChanged;
import static org.lilyproject.tenant.model.api.Tenant.TenantLifecycleState;

public class TenantModelImpl implements TenantModel {
    private Map<String, Tenant> tenants = new HashMap<String, Tenant>();

    /**
     * Lock to be obtained when changing tenants or when dispatching events. This assures the correct functioning
     * of methods like {@link #getTenants(org.lilyproject.tenant.model.api.TenantModelListener)}.
     */
    private final Object tenantsLock = new Object();

    private ZooKeeperItf zk;

    private final Set<TenantModelListener> listeners = Collections.newSetFromMap(new IdentityHashMap<TenantModelListener, Boolean>());

    private Watcher zkWatcher;

    private boolean closed = false;

    private static final String TENANT_COLLECTION_PATH = "/lily/tenant/tenants";

    private static final String TENANT_COLLECTION_PATH_SLASH = TENANT_COLLECTION_PATH + "/";

    private final Log log = LogFactory.getLog(getClass());

    /**
     * The public tenant always exists and is always in state active.
     */
    private static final Tenant PUBLIC_TENANT = new Tenant("public", TenantLifecycleState.ACTIVE);

    public TenantModelImpl(ZooKeeperItf zk) throws KeeperException, InterruptedException {
        this.zk = zk;
        init();
    }

    public void init() throws KeeperException, InterruptedException {
        ZkUtil.createPath(zk, TENANT_COLLECTION_PATH);
        assurePublicTenantExists();
        zkWatcher = new TenantZkWatcher();
        zk.addDefaultWatcher(zkWatcher);
        refresh();
    }

    private void assurePublicTenantExists() throws KeeperException, InterruptedException {
        byte[] tenantBytes = TenantJsonSerDeser.INSTANCE.toJsonBytes(PUBLIC_TENANT);
        try {
            zk.create(TENANT_COLLECTION_PATH + "/" + PUBLIC_TENANT.getName(), tenantBytes, ZooDefs.Ids.OPEN_ACL_UNSAFE,
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
    public void create(String tenantName) throws TenantExistsException, InterruptedException, TenantModelException {
        if (!TenantUtil.isValidTenantName(tenantName)) {
            throw new IllegalArgumentException(String.format("'%s' is not a valid tenant name. "
                    + TenantUtil.VALID_NAME_EXPLANATION, tenantName));
        }
        Tenant tenant = new Tenant(tenantName, TenantLifecycleState.CREATE_REQUESTED);
        byte[] tenantBytes = TenantJsonSerDeser.INSTANCE.toJsonBytes(tenant);
        try {
            zk.create(TENANT_COLLECTION_PATH + "/" + tenantName, tenantBytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException e) {
            throw new TenantExistsException("Can't create tenant, a tenant with this name already exists: " + tenantName);
        } catch (KeeperException e) {
            throw new TenantModelException(e);
        }
    }

    private void disallowPublicTenant(String tenantName) throws TenantModelException {
        if (PUBLIC_TENANT.getName().equals(tenantName)) {
            throw new TenantModelException("This operation is not allowed on the public tenant.");
        }
    }

    @Override
    public void delete(String tenantName) throws InterruptedException, TenantModelException {
        disallowPublicTenant(tenantName);
        Tenant tenant = new Tenant(tenantName, TenantLifecycleState.DELETE_REQUESTED);
        try {
            storeTenant(tenant);
        } catch (KeeperException.NoNodeException e) {
            throw new TenantModelException("Can't delete-request tenant, a tenant with this name doesn't exist: "
                    + tenant.getName());
        } catch (KeeperException e) {
            throw new TenantModelException(e);
        }
    }

    @Override
    public void deleteDirect(String tenantName) throws InterruptedException, TenantModelException, TenantNotFoundException {
        disallowPublicTenant(tenantName);
        try {
            zk.delete(TENANT_COLLECTION_PATH + "/" + tenantName, -1);
        } catch (KeeperException.NoNodeException e) {
            throw new TenantNotFoundException("Can't delete tenant, a tenant with this name doesn't exist: "
                    + tenantName);
        } catch (KeeperException e) {
            throw new TenantModelException("Error deleting tenant.", e);
        }
    }

    @Override
    public void updateTenant(Tenant tenant) throws InterruptedException, TenantModelException, TenantNotFoundException {
        disallowPublicTenant(tenant.getName());
        try {
            storeTenant(tenant);
        } catch (KeeperException.NoNodeException e) {
            throw new TenantNotFoundException("Can't update tenant, a tenant with this name doesn't exist: "
                    + tenant.getName());
        } catch (KeeperException e) {
            throw new TenantModelException(e);
        }
    }

    public void storeTenant(Tenant tenant) throws InterruptedException, KeeperException {
        byte[] tenantBytes = TenantJsonSerDeser.INSTANCE.toJsonBytes(tenant);
        zk.setData(TENANT_COLLECTION_PATH + "/" + tenant.getName(), tenantBytes, -1);
    }

    @Override
    public Set<Tenant> getTenants() throws TenantModelException, InterruptedException {
        try {
            return new HashSet<Tenant>(loadTenants(false).values());
        } catch (KeeperException e) {
            throw new TenantModelException("Error loading tenants.", e);
        }
    }

    @Override
    public Tenant getTenant(String tenantName) throws InterruptedException, TenantModelException, TenantNotFoundException {
        try {
            return loadTenant(tenantName, false);
        } catch (KeeperException.NoNodeException e) {
            throw new TenantNotFoundException("No tenant named " + tenantName, e);
        } catch (KeeperException e) {
            throw new TenantModelException("Error loading tenant " + tenantName, e);
        }
    }

    @Override
    public boolean tenantExistsAndActive(String tenantName) {
        Tenant tenant = tenants.get(tenantName);
        return tenant != null && tenant.getLifecycleState() == TenantLifecycleState.ACTIVE;
    }

    @Override
    public boolean tenantActive(String tenantName) throws TenantNotFoundException {
        Tenant tenant = tenants.get(tenantName);
        if (tenant == null) {
            throw new TenantNotFoundException("No tenant named " + tenantName);
        }
        return tenant.getLifecycleState() == TenantLifecycleState.ACTIVE;
    }

    @Override
    public boolean waitUntilTenantInState(String tenantName, TenantLifecycleState state, long timeout)
            throws InterruptedException {
        long waitUntil = System.currentTimeMillis() + timeout;
        while (!tenantExistsAndActive(tenantName) && System.currentTimeMillis() < waitUntil) {
            Thread.sleep(50);
        }
        return tenantExistsAndActive(tenantName);
    }

    private class TenantZkWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            boolean needsRefresh = false;
            if (event.getType() == Event.EventType.None && event.getState() == Event.KeeperState.SyncConnected) {
                needsRefresh = true;
            } else if (NodeChildrenChanged.equals(event.getType()) && event.getPath().equals(TENANT_COLLECTION_PATH)) {
                needsRefresh = true;
            } else if (NodeDataChanged.equals(event.getType()) && event.getPath().startsWith(TENANT_COLLECTION_PATH_SLASH)) {
                needsRefresh = true;
            }

            if (needsRefresh) {
                try {
                    refresh();
                } catch (Throwable t) {
                    log.error("Tenant Model: error handling event from ZooKeeper. Event: " + event, t);
                }
            }
        }
    }

    private void refresh() throws KeeperException, InterruptedException {
        List<TenantModelEvent> events = new ArrayList<TenantModelEvent>();

        synchronized (tenantsLock) {
            Map<String, Tenant> newTenants = loadTenants(true);

            // Find out changes in tenants
            Set<String> removedTenants = Sets.difference(tenants.keySet(), newTenants.keySet());
            for (String id : removedTenants) {
                events.add(new TenantModelEvent(TenantModelEventType.TENANT_REMOVED, id));
            }

            Set<String> addedTenants = Sets.difference(newTenants.keySet(), tenants.keySet());
            for (String id : addedTenants) {
                events.add(new TenantModelEvent(TenantModelEventType.TENANT_ADDED, id));
            }

            for (Tenant tenant : newTenants.values()) {
                if (tenants.containsKey(tenant.getName()) && !tenants.get(tenant.getName()).equals(tenant)) {
                    events.add(new TenantModelEvent(TenantModelEventType.TENANT_UPDATED, tenant.getName()));
                }
            }

            tenants = newTenants;
        }

        notifyListeners(events);
    }

    private Map<String, Tenant> loadTenants(boolean watch) throws KeeperException, InterruptedException {
        Map<String, Tenant> tenants = new HashMap<String, Tenant>();
        List<String> children = zk.getChildren(TENANT_COLLECTION_PATH, watch ? zkWatcher : null);
        for (String child : children) {
            tenants.put(child, loadTenant(child, watch));
        }
        return tenants;
    }

    private Tenant loadTenant(String name, boolean watch) throws KeeperException, InterruptedException {
        byte[] tenantJson = zk.getData(TENANT_COLLECTION_PATH + "/" + name, watch ? zkWatcher : null, new Stat());
        return TenantJsonSerDeser.INSTANCE.fromJsonBytes(name, tenantJson);
    }

    private void notifyListeners(List<TenantModelEvent> events) {
        if (closed) {
            // Stop dispatching events once closed
            return;
        }

        for (TenantModelEvent event : events) {
            for (TenantModelListener listener : listeners) {
                listener.process(event);
            }
        }
    }

    @Override
    public Set<Tenant> getTenants(TenantModelListener listener) {
        synchronized (tenantsLock) {
            registerListener(listener);
            return new HashSet<Tenant>(tenants.values());
        }
    }

    @Override
    public void registerListener(TenantModelListener listener) {
        synchronized (tenantsLock) {
            listeners.add(listener);
        }
    }

    @Override
    public void unregisterListener(TenantModelListener listener) {
        listeners.remove(listener);
    }
}
