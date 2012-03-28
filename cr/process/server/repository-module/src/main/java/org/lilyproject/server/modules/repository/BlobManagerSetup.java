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
package org.lilyproject.server.modules.repository;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.KeeperException;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.kauriproject.conf.Conf;
import org.lilyproject.repository.api.BlobManager;
import org.lilyproject.repository.api.BlobStoreAccess;
import org.lilyproject.repository.impl.*;
import org.lilyproject.util.hbase.HBaseTableFactory;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.json.JsonFormat;
import org.lilyproject.util.repo.DfsUri;
import org.lilyproject.util.zookeeper.ZkUtil;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

import javax.annotation.PreDestroy;

public class BlobManagerSetup {
    private final String lilyPath = "/lily";
    private final String blobDfsUriPath = lilyPath + "/blobStoresConfig/dfsUri";
    private final String blobStoreAccessConfigPath = lilyPath + "/blobStoresConfig/accessConfig";

    private FileSystem fs;
    private BlobManager blobManager;

    public BlobManagerSetup(URI dfsUri, Configuration configuration, HBaseTableFactory tableFactory, ZooKeeperItf zk,
            Conf blobManagerConf) throws IOException, InterruptedException, KeeperException {
        fs = FileSystem.get(DfsUri.getBaseDfsUri(dfsUri), configuration);
        Path blobRootPath = new Path(DfsUri.getDfsPath(dfsUri));

        BlobStoreAccess dfsBlobStoreAccess = new DFSBlobStoreAccess(fs, blobRootPath);
        BlobStoreAccess hbaseBlobStoreAccess = new HBaseBlobStoreAccess(tableFactory);
        BlobStoreAccess inlineBlobStoreAccess = new InlineBlobStoreAccess();
        List<BlobStoreAccess> blobStoreAccesses = Arrays.asList(dfsBlobStoreAccess, hbaseBlobStoreAccess,
                inlineBlobStoreAccess);

        String defaultStoreName = blobManagerConf.getChild("blobStore").getAttribute("default");
        BlobStoreAccessConfig blobStoreAccessConfig = new BlobStoreAccessConfig(defaultStoreName);
        List<Conf> children = blobManagerConf.getChild("blobStore").getChildren("store");
        for (Conf access : children) {
            String accessName = access.getAttribute("name");
            long limit = access.getAttributeAsInteger("limit");
            blobStoreAccessConfig.setLimit(accessName, limit);
        }

        SizeBasedBlobStoreAccessFactory blobStoreAccessFactory = new SizeBasedBlobStoreAccessFactory(blobStoreAccesses,
                blobStoreAccessConfig);
        blobManager = new BlobManagerImpl(tableFactory, blobStoreAccessFactory, false);

        publishBlobAccessParams(zk, dfsUri.toString());
        publishBlobStoreAccessConfig(zk, blobStoreAccessConfig.toBytes());
    }

    @PreDestroy
    public void stop() {
        Closer.close(fs);
    }

    public BlobManager getBlobManager() {
        return blobManager;
    }

    private void publishBlobAccessParams(ZooKeeperItf zk, String dfsUri)
            throws IOException, InterruptedException, KeeperException {
        // The below serves as a stop-gap solution for the blob configuration: we store the information in ZK
        // that clients need to know how to access the blob store locations, but the actual setup of the
        // BlobStoreAccessFactory is currently hardcoded
        ZkUtil.createPath(zk, blobDfsUriPath, dfsUri.getBytes("UTF-8"));

        // Cleanup old config which existed in Lily up to version 1.1. This code can be removed
        // starting from Lily 1.3.
        if (zk.exists("/lily/blobStoresConfig/hbaseConfig", false) != null) {
            try {
                zk.delete("/lily/blobStoresConfig/hbaseConfig", -1);
            } catch (KeeperException.NoNodeException e) {
                // someone else must have deleted it, ignore
            }
        }
    }

    public void publishBlobStoreAccessConfig(ZooKeeperItf zk, byte[] blobStoreAccessConfig) throws InterruptedException,
            KeeperException {
        ZkUtil.createPath(zk, blobStoreAccessConfigPath, blobStoreAccessConfig);
    }
}
