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
