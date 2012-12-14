package org.lilyproject.hadooptestfw;

public interface ReplicationPeerUtilMBean {
    /**
     * After adding a new replication peer, this waits for the replication source in the region server to be started.
     */
    void waitOnReplicationPeerReady(String peerId);

    /**
     * After removing a replication peer, this waits for the replication source in the region server to be stopped,
     * and will as well unregister its mbean (a workaround because this is missing in hbase at the time of this
     * writing -- hbase 0.94.3)
     */
    void waitOnReplicationPeerStopped(String peerId);
}
