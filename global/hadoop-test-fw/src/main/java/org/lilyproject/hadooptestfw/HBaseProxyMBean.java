package org.lilyproject.hadooptestfw;

public interface HBaseProxyMBean {
    /**
     * After deleting a replication peer, this method will wait for the actual ReplicationSource
     * in the regionserver to be stopped and will as well unregister its mbean (a workaround
     * because this is missing in hbase at the time of this writing -- hbase 0.94.3)
     */
    void removeReplicationSource(String peerId);

    /**
     * After adding a replication peer, this method will wait for the actual ReplicationSource
     * in the regionserver to be running.
     */
    void waitForReplicationSource(String peerId);
}
