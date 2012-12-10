package org.lilyproject.sep.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;
import org.lilyproject.util.zookeeper.ZkUtil;
import org.lilyproject.util.zookeeper.ZooKeeperItf;
import org.lilyproject.sep.SepModel;

import java.io.IOException;
import java.util.UUID;

public class SepModelImpl implements SepModel {
    private final ZooKeeperItf zk;
    private final Configuration hbaseConf;
    private ReplicationAdmin replicationAdmin;
    private Log log = LogFactory.getLog(getClass());

    public SepModelImpl(ZooKeeperItf zk, Configuration hbaseConf) {
        this.zk = zk;
        this.hbaseConf = hbaseConf;

        try {
            this.replicationAdmin = new ReplicationAdmin(hbaseConf); // TODO do we need to close this?
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void addSubscription(String name) throws InterruptedException, KeeperException, IOException {
        if (!addSubscriptionSilent(name)) {
            throw new IllegalStateException("There is already a subscription for name '" + name + "'.");
        }
    }

    public boolean addSubscriptionSilent(String name) throws InterruptedException, KeeperException, IOException {
        if (replicationAdmin.listPeers().containsKey(name)) {
            return false;
        }

        String basePath = HBASE_ROOT + "/" + name;
        UUID uuid = UUID.nameUUIDFromBytes(Bytes.toBytes(name)); // always gives the same uuid for the same name
        ZkUtil.createPath(zk, basePath + "/hbaseid", Bytes.toBytes(uuid.toString()));
        ZkUtil.createPath(zk, basePath + "/rs");

        // Let's assume we're all using the same ZooKeeper
        String zkQuorum = hbaseConf.get("hbase.zookeeper.quorum");
        String zkClientPort = hbaseConf.get("hbase.zookeeper.property.clientPort");

        try {
            replicationAdmin.addPeer(name, zkQuorum + ":" + zkClientPort + ":" + basePath);
        } catch (IllegalArgumentException e) {
            if (e.getMessage().equals("Cannot add existing peer")) {
                return false;
            }
            throw e;
        }

        return true;
    }

    public void removeSubscription(String name) throws IOException {
        if (!removeSubscriptionSilent(name)) {
            throw new IllegalStateException("No subscription named '" + name + "'.");
        }
    }

    public boolean removeSubscriptionSilent(String name) throws IOException {
        if (!replicationAdmin.listPeers().containsKey(name)) {
            log.error("Requested to remove a subscription which does not exist, skipping silently: '" + name + "'");
            return false;
        } else {
            try {
                replicationAdmin.removePeer(name);
            } catch (IllegalArgumentException e) {
                if (e.getMessage().equals("Cannot remove inexisting peer")) { // see ReplicationZookeeper
                    return false;
                }
                throw e;
            }
        }
        return true;
    }

    public boolean hasSubscription(String name) {
       return replicationAdmin.listPeers().containsKey(name);
    }
}
