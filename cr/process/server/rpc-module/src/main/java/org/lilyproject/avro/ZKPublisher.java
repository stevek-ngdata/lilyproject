/*
 * Copyright 2010 Outerthought bvba
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
package org.lilyproject.avro;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.util.json.JsonFormat;
import org.lilyproject.util.zookeeper.ZkUtil;
import org.lilyproject.util.zookeeper.ZooKeeperItf;
import org.lilyproject.util.zookeeper.ZooKeeperOperation;

/**
 * Publishes this Lily repository node to Zookeeper.
 */
public class ZKPublisher {
    private ZooKeeperItf zk;
    private String hostAddress;
    private int port;
    private String lilyPath = "/lily";
    private String nodesPath = lilyPath + "/repositoryNodes";
    private String hbaseConfPath = lilyPath + "/hbaseConfig";
    private Configuration hbaseConf;

    public ZKPublisher(ZooKeeperItf zk, String hostAddress, int port, Configuration hbaseConf) {
        this.zk = zk;
        this.hostAddress = hostAddress;
        this.port = port;
        this.hbaseConf = hbaseConf;
    }

    @PostConstruct
    public void start() throws IOException, InterruptedException, KeeperException {
        // Publish our address
        ZkUtil.createPath(zk, nodesPath);
        final String repoAddressAndPort = hostAddress + ":" + port;
        zk.retryOperation(new ZooKeeperOperation<Object>() {
            @Override
            public Object execute() throws KeeperException, InterruptedException {
                zk.create(nodesPath + "/" + repoAddressAndPort, null, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL);
                return null;
            }
        });

        // Publish HBase configuration for LilyClient use
        // Translate HBase conf into json 
        ObjectNode propertiesNode = JsonNodeFactory.instance.objectNode();
        for (Map.Entry<String, String> propertyEntry : hbaseConf) {
            if (!propertyEntry.getKey().equals(HConstants.HBASE_CLIENT_INSTANCE_ID)) {
                propertiesNode.put(propertyEntry.getKey(), propertyEntry.getValue());
            }
        }
        // TODO we could compare with current state and log a warn if its different
        ZkUtil.createPath(zk, hbaseConfPath, JsonFormat.serializeAsBytes(propertiesNode));
    }
}
