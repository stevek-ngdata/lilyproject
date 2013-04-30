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
package org.lilyproject.tenant.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.lilyproject.plugin.PluginHandle;
import org.lilyproject.plugin.PluginRegistry;
import org.lilyproject.plugin.PluginUser;
import org.lilyproject.tenant.model.api.Tenant;
import org.lilyproject.tenant.model.api.TenantModel;
import org.lilyproject.tenant.model.api.TenantModelEvent;
import org.lilyproject.tenant.model.api.TenantModelEventType;
import org.lilyproject.tenant.model.api.TenantModelListener;
import org.lilyproject.tenant.model.api.TenantNotFoundException;
import org.lilyproject.util.LilyInfo;
import org.lilyproject.util.Logs;
import org.lilyproject.util.zookeeper.LeaderElection;
import org.lilyproject.util.zookeeper.LeaderElectionCallback;
import org.lilyproject.util.zookeeper.LeaderElectionSetupException;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.lilyproject.tenant.model.api.Tenant.TenantLifecycleState;

/**
 * TenantMaster is a component which is active in only one lily-server and performs side effects when
 * a tenant is being created or deleted.
 */
public class TenantMaster implements PluginUser<TenantMasterHook> {
    private final ZooKeeperItf zk;

    private final TenantModel tenantModel;

    private TenantModelListener listener = new MyListener();

    private LeaderElection leaderElection;

    private List<TenantMasterHook> hooks;

    private EventWorker eventWorker = new EventWorker();

    private LilyInfo lilyInfo;

    private PluginRegistry pluginRegistry;

    private Log log = LogFactory.getLog(getClass());

    public TenantMaster(ZooKeeperItf zk, TenantModel tenantModel, LilyInfo lilyInfo, List<TenantMasterHook> hooks) {
        this.zk = zk;
        this.tenantModel = tenantModel;
        this.lilyInfo = lilyInfo;
        this.hooks = hooks;
    }

    public TenantMaster(ZooKeeperItf zk, TenantModel tenantModel, LilyInfo lilyInfo, PluginRegistry pluginRegistry) {
        this.zk = zk;
        this.tenantModel = tenantModel;
        this.lilyInfo = lilyInfo;
        this.hooks = new ArrayList<TenantMasterHook>();
        this.pluginRegistry = pluginRegistry;
    }

    @PostConstruct
    public void start() throws LeaderElectionSetupException, IOException, InterruptedException, KeeperException {
        leaderElection = new LeaderElection(zk, "Tenant Master", "/lily/tenant/masters",
                new MyLeaderElectionCallback());

        if (pluginRegistry != null) {
            pluginRegistry.setPluginUser(TenantMasterHook.class, this);
        }
    }

    @PreDestroy
    public void stop() {
        if (pluginRegistry != null) {
            pluginRegistry.unsetPluginUser(TenantMasterHook.class, this);
        }

        try {
            if (leaderElection != null) {
                leaderElection.stop();
            }
        } catch (InterruptedException e) {
            log.info("Interrupted while shutting down leader election.");
        }
    }

    @Override
    public void pluginAdded(PluginHandle<TenantMasterHook> pluginHandle) {
        hooks.add(pluginHandle.getPlugin());
    }

    @Override
    public void pluginRemoved(PluginHandle<TenantMasterHook> pluginHandle) {
        // we don't need to be this dynamic for now
    }

    private class MyLeaderElectionCallback implements LeaderElectionCallback {
        @Override
        public void activateAsLeader() throws Exception {
            log.info("Starting up as tenant master.");

            // Start these processes, but it is not until we have registered our model listener
            // that these will receive work.
            eventWorker.start();

            Set<Tenant> tenants = tenantModel.getTenants(listener);

            // Perform an initial run over the tenants by generating fake events
            for (Tenant tenant : tenants) {
                eventWorker.putEvent(new TenantModelEvent(TenantModelEventType.TENANT_UPDATED, tenant.getName()));
            }

            log.info("Startup as tenant master successful.");
            lilyInfo.setTenantMaster(true);
        }

        @Override
        public void deactivateAsLeader() throws Exception {
            log.info("Shutting down as tenant master.");

            tenantModel.unregisterListener(listener);

            // Argument false for shutdown: we do not interrupt the event worker thread: if there
            // was something running there that is blocked until the ZK connection comes back up
            // we want it to finish
            eventWorker.shutdown(false);

            log.info("Shutdown as tenant master successful.");
            lilyInfo.setTenantMaster(false);
        }
    }

    private class MyListener implements TenantModelListener {
        @Override
        public void process(TenantModelEvent event) {
            try {
                // Let another thread process the events, so that we don't block the ZK watcher thread
                eventWorker.putEvent(event);
            } catch (InterruptedException e) {
                log.info("TenantMaster.TenantModelListener interrupted.");
            }
        }
    }

    private class EventWorker implements Runnable {

        private BlockingQueue<TenantModelEvent> eventQueue = new LinkedBlockingQueue<TenantModelEvent>();

        private boolean stop;

        private Thread thread;

        public synchronized void shutdown(boolean interrupt) throws InterruptedException {
            stop = true;
            eventQueue.clear();

            if (!thread.isAlive()) {
                return;
            }

            if (interrupt) {
                thread.interrupt();
            }
            Logs.logThreadJoin(thread);
            thread.join();
            thread = null;
        }

        public synchronized void start() throws InterruptedException {
            if (thread != null) {
                log.warn("EventWorker start was requested, but old thread was still there. Stopping it now.");
                thread.interrupt();
                Logs.logThreadJoin(thread);
                thread.join();
            }
            eventQueue.clear();
            stop = false;
            thread = new Thread(this, "TenantMasterEventWorker");
            thread.start();
        }

        public void putEvent(TenantModelEvent event) throws InterruptedException {
            if (stop) {
                throw new RuntimeException("This EventWorker is stopped, no events should be added.");
            }
            eventQueue.put(event);
        }

        @Override
        public void run() {
            long startedAt = System.currentTimeMillis();

            while (!stop && !Thread.interrupted()) {
                TenantModelEvent event = null;
                try {
                    while (!stop && event == null) {
                        event = eventQueue.poll(1000, TimeUnit.MILLISECONDS);
                    }

                    if (stop || event == null || Thread.interrupted()) {
                        return;
                    }

                    // Warn if the queue is getting large, but do not do this just after we started, because
                    // on initial startup a fake update event is added for every defined index, which would lead
                    // to this message always being printed on startup when more than 10 indexes are defined.
                    int queueSize = eventQueue.size();
                    if (queueSize >= 10 && (System.currentTimeMillis() - startedAt > 5000)) {
                        log.warn("EventWorker queue getting large, size = " + queueSize);
                    }

                    try {
                        Tenant tenant = tenantModel.getTenant(event.getTenantName());
                        if (tenant.getLifecycleState() == TenantLifecycleState.CREATE_REQUESTED) {
                            for (TenantMasterHook hook : hooks) {
                                try {
                                    hook.postCreate(tenant.getName());
                                } catch (InterruptedException e) {
                                    return;
                                } catch (Throwable t) {
                                    log.error("Failure executing a tenant post-create hook.", t);
                                }
                            }
                            Tenant updatedTenant = new Tenant(tenant.getName(), TenantLifecycleState.ACTIVE);
                            tenantModel.updateTenant(updatedTenant);
                        } else if (tenant.getLifecycleState() == TenantLifecycleState.DELETE_REQUESTED) {
                            for (TenantMasterHook hook : hooks) {
                                try {
                                    hook.preDelete(tenant.getName());
                                } catch (InterruptedException e) {
                                    return;
                                } catch (Throwable t) {
                                    log.error("Failure executing a tenant pre-delete hook.", t);
                                }
                            }
                            tenantModel.deleteDirect(tenant.getName());
                        }
                    } catch (TenantNotFoundException e) {
                        // no problem
                    }

                } catch (InterruptedException e) {
                    return;
                } catch (Throwable t) {
                    log.error("Error processing tenant model event in TenantMaster. Event: " + event, t);
                }
            }
        }
    }
}
