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
package org.lilyproject.server.modules.repository;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.kauriproject.conf.Conf;
import org.lilyproject.repository.api.BlobManager;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.impl.BlobIncubatorMonitor;
import org.lilyproject.util.hbase.HBaseTableFactory;
import org.lilyproject.util.zookeeper.LeaderElectionSetupException;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

public class BlobIncubatorMonitorSetup {

    private BlobIncubatorMonitor blobIncubatorMonitor;
    private final ZooKeeperItf zookeeper;
    private final HBaseTableFactory hbaseTableFactory;
    private final BlobManager blobManager;
    private final TypeManager typeManager;
    private final Conf blobManagerConf;
    private final String hostName;

    public BlobIncubatorMonitorSetup(ZooKeeperItf zookeeper, HBaseTableFactory hbaseTableFactory,
                                     BlobManager blobManager, TypeManager typeManager, Conf blobManagerConf, String hostName) throws IOException {
        this.zookeeper = zookeeper;
        this.hbaseTableFactory = hbaseTableFactory;
        this.blobManager = blobManager;
        this.typeManager = typeManager;
        this.blobManagerConf = blobManagerConf;
        this.hostName = hostName;
    }

    @PostConstruct
    public void start() throws LeaderElectionSetupException, IOException, InterruptedException, KeeperException {
        long minimalAge = 1000 * blobManagerConf.getChild("blobIncubatorMonitor").getAttributeAsLong("minimalAge");
        long monitorDelay = blobManagerConf.getChild("blobIncubatorMonitor").getAttributeAsLong("monitorDelay");
        long runDelay = 1000 * blobManagerConf.getChild("blobIncubatorMonitor").getAttributeAsLong("runDelay");
        blobIncubatorMonitor = new BlobIncubatorMonitor(zookeeper, hbaseTableFactory, blobManager, typeManager, minimalAge, monitorDelay, runDelay);

        List<String> blobIncubatorNodes = Collections.EMPTY_LIST;
        Conf nodesConf = blobManagerConf.getChild("blobIncubatorMonitor").getChild("nodes");
        if (nodesConf != null) {
            String nodes = nodesConf.getValue("");
            if (!nodes.isEmpty()) {
                blobIncubatorNodes = Arrays.asList(nodes.split(","));
            }
        }
        if (blobIncubatorNodes.isEmpty() || blobIncubatorNodes.contains(hostName)) {
            blobIncubatorMonitor.start();
        }
    }

    @PreDestroy
    public void stop() {
        blobIncubatorMonitor.stop();
    }

}
