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
package org.lilyproject.repository.impl.test;


import java.net.InetSocketAddress;

import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.Server;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.lilyproject.repository.api.BlobManager;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.avro.AvroConverter;
import org.lilyproject.repository.avro.AvroLily;
import org.lilyproject.repository.avro.AvroLilyImpl;
import org.lilyproject.repository.avro.LilySpecificResponder;
import org.lilyproject.repository.impl.BlobStoreAccessRegistry;
import org.lilyproject.repository.impl.HBaseRepository;
import org.lilyproject.repository.impl.HBaseTypeManager;
import org.lilyproject.repository.impl.IdGeneratorImpl;
import org.lilyproject.repository.impl.RemoteRepository;
import org.lilyproject.repository.impl.RemoteTypeManager;
import org.lilyproject.testfw.TestHelper;
import org.lilyproject.util.hbase.HBaseTableFactoryImpl;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.zookeeper.ZkUtil;

public class RemoteBlobStoreTest extends AbstractBlobStoreTest {

    
    private static HBaseRepository serverRepository;
    private static Server lilyServer;
    private static TypeManager serverTypeManager;
    

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging();
        HBASE_PROXY.start();
        IdGeneratorImpl idGenerator = new IdGeneratorImpl();
        configuration = HBASE_PROXY.getConf();
        zooKeeper = ZkUtil.connect(HBASE_PROXY.getZkConnectString(), 10000);
        hbaseTableFactory = new HBaseTableFactoryImpl(configuration);
        serverTypeManager = new HBaseTypeManager(idGenerator, configuration, zooKeeper, hbaseTableFactory);
        BlobManager serverBlobManager = setupBlobManager();
        setupWal();
        serverRepository = new HBaseRepository(serverTypeManager, idGenerator, wal, configuration, hbaseTableFactory, serverBlobManager);
     // Create a blobStoreAccessRegistry for testing purposes
        testBlobStoreAccessRegistry = new BlobStoreAccessRegistry(serverBlobManager);
        testBlobStoreAccessRegistry.setBlobStoreAccessFactory(blobStoreAccessFactory);
        
        AvroConverter serverConverter = new AvroConverter();
        serverConverter.setRepository(serverRepository);
        lilyServer = new NettyServer(
                new LilySpecificResponder(AvroLily.class, new AvroLilyImpl(serverRepository, serverConverter),
                        serverConverter), new InetSocketAddress(0));
        lilyServer.start();
        AvroConverter remoteConverter = new AvroConverter();
        typeManager = new RemoteTypeManager(new InetSocketAddress(lilyServer.getPort()),
                remoteConverter, idGenerator, zooKeeper);
        BlobManager blobManager = setupBlobManager();
        repository = new RemoteRepository(new InetSocketAddress(lilyServer.getPort()), remoteConverter,
                (RemoteTypeManager)typeManager, idGenerator, blobManager);
        remoteConverter.setRepository(repository);
        ((RemoteTypeManager)typeManager).start();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        Closer.close(rowLogConfMgr);
        Closer.close(typeManager);
        Closer.close(repository);
        lilyServer.close();
        lilyServer.join();
        Closer.close(serverTypeManager);
        Closer.close(serverRepository);
        Closer.close(zooKeeper);
        HBASE_PROXY.stop();
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    
}
