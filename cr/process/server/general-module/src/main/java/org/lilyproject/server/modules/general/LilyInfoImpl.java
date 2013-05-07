/*
 * Copyright 2012 NGDATA nv
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
package org.lilyproject.server.modules.general;

import javax.annotation.PostConstruct;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.lilyproject.util.LilyInfo;
import org.lilyproject.util.zookeeper.ZkUtil;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

public class LilyInfoImpl implements LilyInfo {
    private boolean indexerMaster;
    private boolean repositoryMaster;

    private final ZooKeeperItf zk;
    private final Watcher watcher = new RepositoryNodesWatcher();
    private Set<String> hostnames = Collections.emptySet();
    private final Log log = LogFactory.getLog(getClass());

    public LilyInfoImpl(ZooKeeperItf zk) {
        this.zk = zk;
    }

    @PostConstruct
    public void init() throws KeeperException, InterruptedException {
        ZkUtil.createPath(zk, "/lily/repositoryNodes");
        refresh();
    }

    private void refresh() throws KeeperException, InterruptedException {
        List<String> addresses = zk.getChildren("/lily/repositoryNodes", watcher);
        Set<String> hostNames = new HashSet<String>();
        for (String address : addresses) {
            int colonPos = address.indexOf(":");
            String hostName = address.substring(0, colonPos);
            hostNames.add(hostName);
        }
        this.hostnames = Collections.unmodifiableSet(hostNames);
    }

    private class RepositoryNodesWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            try {
                if (event.getState() != Event.KeeperState.SyncConnected) {
                    hostnames = Collections.emptySet();
                } else {
                    refresh();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (KeeperException.ConnectionLossException e) {
                hostnames = Collections.emptySet();
            } catch (Throwable t) {
                log.error("Error in ZooKeeper watcher.", t);
            }
        }
    }

    @Override
    public Set<String> getHostnames() {
        return hostnames;
    }

    @Override
    public String getVersion() {
        return "TODO";
    }

    @Override
    public boolean isIndexerMaster() {
        return indexerMaster;
    }

    @Override
    public void setIndexerMaster(boolean indexerMaster) {
        this.indexerMaster = indexerMaster;
    }

    @Override
    public void setRepositoryMaster(boolean repositoryMaster) {
        this.repositoryMaster = repositoryMaster;
    }

    @Override
    public boolean isRepositoryMaster() {
        return repositoryMaster;
    }
}
