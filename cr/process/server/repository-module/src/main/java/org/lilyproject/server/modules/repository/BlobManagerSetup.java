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
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.KeeperException;
import org.kauriproject.conf.Conf;
import org.lilyproject.repository.api.BlobManager;
import org.lilyproject.repository.api.BlobStoreAccess;
import org.lilyproject.repository.impl.*;
import org.lilyproject.util.hbase.HBaseTableFactory;
import org.lilyproject.util.repo.DfsUri;
import org.lilyproject.util.zookeeper.ZkUtil;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

public class BlobManagerSetup {
    static BlobManager get(URI dfsUri, Configuration configuration, HBaseTableFactory tableFactory, ZooKeeperItf zk, Conf blobManagerConf) throws IOException, InterruptedException, KeeperException {
        FileSystem fs = FileSystem.get(DfsUri.getBaseDfsUri(dfsUri), configuration);
        Path blobRootPath = new Path(DfsUri.getDfsPath(dfsUri));

        BlobStoreAccess dfsBlobStoreAccess = new DFSBlobStoreAccess(fs, blobRootPath);
        BlobStoreAccess hbaseBlobStoreAccess = new HBaseBlobStoreAccess(tableFactory);
        BlobStoreAccess inlineBlobStoreAccess = new InlineBlobStoreAccess();
        List<BlobStoreAccess> blobStoreAccesses = Arrays.asList(new BlobStoreAccess[]{dfsBlobStoreAccess, hbaseBlobStoreAccess, inlineBlobStoreAccess});
        
        BlobStoreAccessConfig blobStoreAccessConfig = new BlobStoreAccessConfig(blobManagerConf.getChild("blobStore").getAttribute("default"));
        List<Conf> children = blobManagerConf.getChild("blobStore").getChildren("store");
        for (Conf access : children) {
            String accessName = access.getAttribute("name");
            long limit = access.getAttributeAsInteger("limit");
            blobStoreAccessConfig.setLimit(accessName, limit);
        }
        
        ZkUtil.createPath(zk, "/lily/blobStoresConfig/accessConfig", blobStoreAccessConfig.toBytes());

        SizeBasedBlobStoreAccessFactory blobStoreAccessFactory = new SizeBasedBlobStoreAccessFactory(blobStoreAccesses, blobStoreAccessConfig);
        return new BlobManagerImpl(tableFactory, blobStoreAccessFactory, false);
    }
}
