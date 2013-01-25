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
    
    // Replace '-' with unicode "CANADIAN SYLLABICS HYPHEN" character in zookeeper to avoid issues
    // with HBase replication naming conventions
    public static final char INTERNAL_HYPHEN_REPLACEMENT = '\u1400';
    
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

    @Override
    public void addSubscription(String name) throws InterruptedException, KeeperException, IOException {
        if (!addSubscriptionSilent(name)) {
            throw new IllegalStateException("There is already a subscription for name '" + name + "'.");
        }
    }

    @Override
    public boolean addSubscriptionSilent(String name) throws InterruptedException, KeeperException, IOException {
        
        String internalName = toInternalSubscriptionName(name);
        if (replicationAdmin.listPeers().containsKey(internalName)) {
            return false;
        }

        String basePath = HBASE_ROOT + "/" + internalName;
        UUID uuid = UUID.nameUUIDFromBytes(Bytes.toBytes(internalName)); // always gives the same uuid for the same name
        ZkUtil.createPath(zk, basePath + "/hbaseid", Bytes.toBytes(uuid.toString()));
        ZkUtil.createPath(zk, basePath + "/rs");

        // Let's assume we're all using the same ZooKeeper
        String zkQuorum = hbaseConf.get("hbase.zookeeper.quorum");
        String zkClientPort = hbaseConf.get("hbase.zookeeper.property.clientPort");

        try {
            replicationAdmin.addPeer(internalName, zkQuorum + ":" + zkClientPort + ":" + basePath);
        } catch (IllegalArgumentException e) {
            if (e.getMessage().equals("Cannot add existing peer")) {
                return false;
            }
            throw e;
        }

        return true;
    }

    @Override
    public void removeSubscription(String name) throws IOException {
        if (!removeSubscriptionSilent(name)) {
            throw new IllegalStateException("No subscription named '" + name + "'.");
        }
    }

    @Override
    public boolean removeSubscriptionSilent(String name) throws IOException {
        String internalName = toInternalSubscriptionName(name);
        if (!replicationAdmin.listPeers().containsKey(internalName)) {
            log.error("Requested to remove a subscription which does not exist, skipping silently: '" + name + "'");
            return false;
        } else {
            try {
                replicationAdmin.removePeer(internalName);
            } catch (IllegalArgumentException e) {
                if (e.getMessage().equals("Cannot remove inexisting peer")) { // see ReplicationZookeeper
                    return false;
                }
                throw e;
            }
            String basePath = HBASE_ROOT + "/" + internalName;
            try {
            ZkUtil.deleteNode(zk, basePath + "/hbaseid");
            ZkUtil.deleteNode(zk, basePath + "/rs");
            ZkUtil.deleteNode(zk, basePath);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(ie);
            } catch (KeeperException ke) {
                log.error("Cleanup in zookeeper failed on " + basePath, ke);
            }
        }
        return true;
    }

    @Override
    public boolean hasSubscription(String name) {
       String internalName = toInternalSubscriptionName(name);
       return replicationAdmin.listPeers().containsKey(internalName);
    }
    
        
    static String toInternalSubscriptionName(String subscriptionName) {
        if (subscriptionName.indexOf(INTERNAL_HYPHEN_REPLACEMENT, 0) != -1) {
            throw new IllegalArgumentException("Subscription name cannot contain character \\U001D");
        }
        return subscriptionName.replace('-', INTERNAL_HYPHEN_REPLACEMENT);
    }
}
