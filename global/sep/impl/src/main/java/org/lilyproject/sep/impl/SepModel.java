package org.lilyproject.sep.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;
import org.lilyproject.util.zookeeper.ZkUtil;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

import java.io.IOException;
import java.util.UUID;

public class SepModel {
    private final ZooKeeperItf zk;
    private final Configuration hbaseConf;
    private ReplicationAdmin replicationAdmin;
    private Log log = LogFactory.getLog(getClass());

    public static final String HBASE_ROOT = "/lily/sep/hbase-slave";

    public SepModel(ZooKeeperItf zk, Configuration hbaseConf) {
        this.zk = zk;
        this.hbaseConf = hbaseConf;

        try {
            this.replicationAdmin = new ReplicationAdmin(hbaseConf); // TODO do we need to close this?
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void addSubscription(String name) throws InterruptedException, KeeperException, IOException {
        if (replicationAdmin.listPeers().containsKey(name)) {
            throw new IllegalStateException("There is already a subscription for name '" + name + "'.");
        }

        String basePath = HBASE_ROOT + "/" + name;
        UUID uuid = UUID.nameUUIDFromBytes(Bytes.toBytes(name)); // always gives the same uuid for the same name
        ZkUtil.createPath(zk, basePath + "/hbaseid", Bytes.toBytes(uuid.toString()));
        ZkUtil.createPath(zk, basePath + "/rs");

        // Let's assume we're all using the same ZooKeeper
        String zkQuorum = hbaseConf.get("hbase.zookeeper.quorum");
        String zkClientPort = hbaseConf.get("hbase.zookeeper.property.clientPort");
        replicationAdmin.addPeer(name, zkQuorum + ":" + zkClientPort + ":" + basePath);
    }

    public void removeSubscription(String name) throws IOException {
        if (!replicationAdmin.listPeers().containsKey(name)) {
            log.error("Requested to remove a subscription which does not exist, skipping silently: '" + name + "'");
        } else {
            replicationAdmin.removePeer(name);
        }
    }
}
