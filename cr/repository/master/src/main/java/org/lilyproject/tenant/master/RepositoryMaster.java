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
import org.lilyproject.tenant.model.api.RepositoryDefinition;
import org.lilyproject.tenant.model.api.RepositoryModel;
import org.lilyproject.tenant.model.api.RepositoryModelEvent;
import org.lilyproject.tenant.model.api.RepositoryModelEventType;
import org.lilyproject.tenant.model.api.RepositoryModelListener;
import org.lilyproject.tenant.model.api.RepositoryNotFoundException;
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

import static org.lilyproject.tenant.model.api.RepositoryDefinition.RepositoryLifecycleState;

/**
 * RepositoryMaster is a component which is active in only one lily-server and performs side effects when
 * a repository is being created or deleted.
 */
public class RepositoryMaster implements PluginUser<RepositoryMasterHook> {
    private final ZooKeeperItf zk;

    private final RepositoryModel repositoryModel;

    private RepositoryModelListener listener = new MyListener();

    private LeaderElection leaderElection;

    private List<RepositoryMasterHook> hooks;

    private EventWorker eventWorker = new EventWorker();

    private LilyInfo lilyInfo;

    private PluginRegistry pluginRegistry;

    private Log log = LogFactory.getLog(getClass());

    public RepositoryMaster(ZooKeeperItf zk, RepositoryModel repositoryModel, LilyInfo lilyInfo, List<RepositoryMasterHook> hooks) {
        this.zk = zk;
        this.repositoryModel = repositoryModel;
        this.lilyInfo = lilyInfo;
        this.hooks = hooks;
    }

    public RepositoryMaster(ZooKeeperItf zk, RepositoryModel repositoryModel, LilyInfo lilyInfo, PluginRegistry pluginRegistry) {
        this.zk = zk;
        this.repositoryModel = repositoryModel;
        this.lilyInfo = lilyInfo;
        this.hooks = new ArrayList<RepositoryMasterHook>();
        this.pluginRegistry = pluginRegistry;
    }

    @PostConstruct
    public void start() throws LeaderElectionSetupException, IOException, InterruptedException, KeeperException {
        leaderElection = new LeaderElection(zk, "Repository Master", "/lily/repositorymodel/masters",
                new MyLeaderElectionCallback());

        if (pluginRegistry != null) {
            pluginRegistry.setPluginUser(RepositoryMasterHook.class, this);
        }
    }

    @PreDestroy
    public void stop() {
        if (pluginRegistry != null) {
            pluginRegistry.unsetPluginUser(RepositoryMasterHook.class, this);
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
    public void pluginAdded(PluginHandle<RepositoryMasterHook> pluginHandle) {
        hooks.add(pluginHandle.getPlugin());
    }

    @Override
    public void pluginRemoved(PluginHandle<RepositoryMasterHook> pluginHandle) {
        // we don't need to be this dynamic for now
    }

    private class MyLeaderElectionCallback implements LeaderElectionCallback {
        @Override
        public void activateAsLeader() throws Exception {
            log.info("Starting up as repository master.");

            // Start these processes, but it is not until we have registered our model listener
            // that these will receive work.
            eventWorker.start();

            Set<RepositoryDefinition> repoDefs = repositoryModel.getRepositories(listener);

            // Perform an initial run over the repository definitions by generating fake events
            for (RepositoryDefinition repoDef : repoDefs) {
                eventWorker.putEvent(new RepositoryModelEvent(RepositoryModelEventType.REPOSITORY_UPDATED, repoDef.getName()));
            }

            log.info("Startup as repository master successful.");
            lilyInfo.setRepositoryMaster(true);
        }

        @Override
        public void deactivateAsLeader() throws Exception {
            log.info("Shutting down as repository master.");

            repositoryModel.unregisterListener(listener);

            // Argument false for shutdown: we do not interrupt the event worker thread: if there
            // was something running there that is blocked until the ZK connection comes back up
            // we want it to finish
            eventWorker.shutdown(false);

            log.info("Shutdown as repository master successful.");
            lilyInfo.setRepositoryMaster(false);
        }
    }

    private class MyListener implements RepositoryModelListener {
        @Override
        public void process(RepositoryModelEvent event) {
            try {
                // Let another thread process the events, so that we don't block the ZK watcher thread
                eventWorker.putEvent(event);
            } catch (InterruptedException e) {
                log.info("RepositoryMaster.RepositoryModelListener interrupted.");
            }
        }
    }

    private class EventWorker implements Runnable {

        private BlockingQueue<RepositoryModelEvent> eventQueue = new LinkedBlockingQueue<RepositoryModelEvent>();

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
            thread = new Thread(this, "RepositoryMasterEventWorker");
            thread.start();
        }

        public void putEvent(RepositoryModelEvent event) throws InterruptedException {
            if (stop) {
                throw new RuntimeException("This EventWorker is stopped, no events should be added.");
            }
            eventQueue.put(event);
        }

        @Override
        public void run() {
            long startedAt = System.currentTimeMillis();

            while (!stop && !Thread.interrupted()) {
                RepositoryModelEvent event = null;
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
                        RepositoryDefinition repoDef = repositoryModel.getRepository(event.getRepositoryName());
                        if (repoDef.getLifecycleState() == RepositoryLifecycleState.CREATE_REQUESTED) {
                            for (RepositoryMasterHook hook : hooks) {
                                try {
                                    hook.postCreate(repoDef.getName());
                                } catch (InterruptedException e) {
                                    return;
                                } catch (Throwable t) {
                                    log.error("Failure executing a repository post-create hook for "
                                            + event.getRepositoryName(), t);
                                }
                            }
                            RepositoryDefinition updatedRepoDef = new RepositoryDefinition(repoDef.getName(),
                                    RepositoryLifecycleState.ACTIVE);
                            repositoryModel.updateRepository(updatedRepoDef);
                        } else if (repoDef.getLifecycleState() == RepositoryLifecycleState.DELETE_REQUESTED) {
                            for (RepositoryMasterHook hook : hooks) {
                                try {
                                    hook.preDelete(repoDef.getName());
                                } catch (InterruptedException e) {
                                    return;
                                } catch (Throwable t) {
                                    log.error("Failure executing a repository pre-delete hook for "
                                            + event.getRepositoryName(), t);
                                }
                            }
                            repositoryModel.deleteDirect(repoDef.getName());
                        }
                    } catch (RepositoryNotFoundException e) {
                        // no problem
                    }

                } catch (InterruptedException e) {
                    return;
                } catch (Throwable t) {
                    log.error("Error processing repository model event in RepositoryMaster. Event: " + event, t);
                }
            }
        }
    }
}
