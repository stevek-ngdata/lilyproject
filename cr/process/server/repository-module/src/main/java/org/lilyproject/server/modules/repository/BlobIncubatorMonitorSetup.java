package org.lilyproject.server.modules.repository;

import java.io.IOException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

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

    public BlobIncubatorMonitorSetup(ZooKeeperItf zookeeper, HBaseTableFactory hbaseTableFactory, BlobManager blobManager, TypeManager typeManager, Conf blobManagerConf) throws IOException {
        this.zookeeper = zookeeper;
        this.hbaseTableFactory = hbaseTableFactory;
        this.blobManager = blobManager;
        this.typeManager = typeManager;
        this.blobManagerConf = blobManagerConf;
    }

    @PostConstruct
    public void start() throws LeaderElectionSetupException, IOException, InterruptedException, KeeperException {
        long minimalAge = 1000 * blobManagerConf.getChild("blobIncubatorMonitor").getAttributeAsLong("minimalAge", 3600L);
        long monitorDelay = 1000 * blobManagerConf.getChild("blobIncubatorMonitor").getAttributeAsLong("monitorDelay", 60L);
        long runDelay = 1000 * blobManagerConf.getChild("blobIncubatorMonitor").getAttributeAsLong("runDelay", 1800L);
        blobIncubatorMonitor = new BlobIncubatorMonitor(zookeeper, hbaseTableFactory, blobManager, typeManager, minimalAge, monitorDelay, runDelay);
        blobIncubatorMonitor.start();
    }
    
    @PreDestroy
    public void stop() {
        blobIncubatorMonitor.stop();
    }
    
}
