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
import javax.annotation.PreDestroy;
import java.io.IOException;

import com.ngdata.sep.SepModel;
import com.ngdata.sep.impl.SepConsumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.KeeperException;
import org.lilyproject.hbaseindex.IndexManager;
import org.lilyproject.hbaseindex.IndexNotFoundException;
import org.lilyproject.linkindex.LinkIndex;
import org.lilyproject.linkindex.LinkIndexUpdater;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.RepositoryManager;
import org.lilyproject.sep.LilyPayloadExtractor;
import org.lilyproject.sep.ZooKeeperItfAdapter;
import org.lilyproject.util.hbase.HBaseTableFactory;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.repo.PureRepository;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

public class LinkIndexSetup {
    private final SepModel sepModel;
    private final boolean linkIndexEnabled;
    private final int threads;
    private final RepositoryManager repositoryManager;
    private final Configuration hbaseConf;
    private final HBaseTableFactory tableFactory;
    private final ZooKeeperItf zk;
    private final String hostName;
    private SepConsumer sepConsumer;

    public LinkIndexSetup(SepModel sepModel, boolean linkIndexEnabled, int threads, RepositoryManager repositoryManager,
            Configuration hbaseConf, HBaseTableFactory tableFactory, ZooKeeperItf zk, String hostName) {
        this.sepModel = sepModel;
        this.linkIndexEnabled = linkIndexEnabled;
        this.threads = threads;
        this.repositoryManager = repositoryManager;
        this.hbaseConf = hbaseConf;
        this.tableFactory = tableFactory;
        this.zk = zk;
        this.hostName = hostName;
    }

    @PostConstruct
    public void start() throws InterruptedException, KeeperException, IOException,
            IndexNotFoundException, RepositoryException {

        if (linkIndexEnabled) {
            // assure the subscription exists
            sepModel.addSubscriptionSilent("LinkIndexUpdater");
        } else {
            // assure the subscription doesn't exist
            sepModel.removeSubscriptionSilent("LinkIndexUpdater");
        }

        if (linkIndexEnabled) {
            IndexManager indexManager = new IndexManager(hbaseConf, tableFactory);

            LinkIndex linkIndex = new LinkIndex(indexManager,
                    /* TODO multitenancy */ PureRepository.wrap((Repository)repositoryManager.getPublicRepository()));

            LinkIndexUpdater linkIndexUpdater = new LinkIndexUpdater(repositoryManager, linkIndex);

            sepConsumer = new SepConsumer("LinkIndexUpdater", 0L, linkIndexUpdater, threads, hostName,
                    new ZooKeeperItfAdapter(zk), hbaseConf, new LilyPayloadExtractor());
            sepConsumer.start();
        }
    }

    @PreDestroy
    public void stop() {
        Closer.close(sepConsumer);
    }
}
