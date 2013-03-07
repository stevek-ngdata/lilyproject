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
package org.lilyproject.repotestfw;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.ngdata.sep.EventListener;
import com.ngdata.sep.SepModel;
import com.ngdata.sep.impl.SepConsumer;
import com.ngdata.sep.impl.SepModelImpl;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.Server;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.KeeperException;
import org.lilyproject.avro.AvroConverter;
import org.lilyproject.avro.AvroLily;
import org.lilyproject.avro.AvroLilyImpl;
import org.lilyproject.avro.LilySpecificResponder;
import org.lilyproject.hadooptestfw.HBaseProxy;
import org.lilyproject.repository.api.BlobManager;
import org.lilyproject.repository.api.BlobStoreAccess;
import org.lilyproject.repository.api.BlobStoreAccessFactory;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.RecordFactory;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.RepositoryManager;
import org.lilyproject.repository.api.RepositoryTableManager;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.impl.AbstractSchemaCache;
import org.lilyproject.repository.impl.BlobManagerImpl;
import org.lilyproject.repository.impl.BlobStoreAccessConfig;
import org.lilyproject.repository.impl.DFSBlobStoreAccess;
import org.lilyproject.repository.impl.HBaseBlobStoreAccess;
import org.lilyproject.repository.impl.HBaseRepository;
import org.lilyproject.repository.impl.HBaseRepositoryManager;
import org.lilyproject.repository.impl.HBaseTypeManager;
import org.lilyproject.repository.impl.InlineBlobStoreAccess;
import org.lilyproject.repository.impl.RecordFactoryImpl;
import org.lilyproject.repository.impl.RepositoryTableManagerImpl;
import org.lilyproject.repository.impl.SchemaCache;
import org.lilyproject.repository.impl.SizeBasedBlobStoreAccessFactory;
import org.lilyproject.repository.impl.id.IdGeneratorImpl;
import org.lilyproject.repository.remote.AvroLilyTransceiver;
import org.lilyproject.repository.remote.RemoteRepositoryManager;
import org.lilyproject.repository.remote.RemoteTypeManager;
import org.lilyproject.repository.spi.RecordUpdateHook;
import org.lilyproject.sep.LilyEventPublisherManager;
import org.lilyproject.sep.LilyPayloadExtractor;
import org.lilyproject.sep.ZooKeeperItfAdapter;
import org.lilyproject.util.hbase.HBaseTableFactory;
import org.lilyproject.util.hbase.HBaseTableFactoryImpl;
import org.lilyproject.util.hbase.LilyHBaseSchema.Table;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.zookeeper.ZkUtil;
import org.lilyproject.util.zookeeper.ZooKeeperItf;


/**
 * Helper class to instantiate and wire all the repository related services.
 */
public class RepositorySetup {
    private HBaseProxy hbaseProxy;
    private Configuration hadoopConf;
    private ZooKeeperItf zk;

    private HBaseTableFactory hbaseTableFactory;

    private IdGenerator idGenerator;
    private HBaseTypeManager typeManager;
    private RemoteTypeManager remoteTypeManager;
    private RepositoryManager repositoryManager;
    private RemoteRepositoryManager remoteRepositoryManager;
    private RepositoryTableManager tableManager;

    private Server lilyServer;

    private BlobStoreAccessFactory blobStoreAccessFactory;
    private BlobStoreAccessFactory remoteBlobStoreAccessFactory;

    private BlobManager blobManager;
    private BlobManager remoteBlobManager;

    private SepModel sepModel;
    private SepConsumer sepConsumer;
    private LilyEventPublisherManager eventPublisherManager;

    private boolean coreSetup;
    private boolean typeManagerSetup;
    private boolean repositoryManagerSetup;

    private long hbaseBlobLimit = -1;
    private long inlineBlobLimit = -1;

    private List<RecordUpdateHook> recordUpdateHooks = Collections.emptyList();

    private RemoteTestSchemaCache remoteSchemaCache;

    public void setRecordUpdateHooks(List<RecordUpdateHook> recordUpdateHooks) {
        this.recordUpdateHooks = recordUpdateHooks;
    }

    public void setupCore() throws Exception {
        if (coreSetup) {
            return;
        }
        hbaseProxy = new HBaseProxy();
        hbaseProxy.start();
        hadoopConf = hbaseProxy.getConf();
        zk = ZkUtil.connect(hbaseProxy.getZkConnectString(), 10000);

        hbaseTableFactory = new HBaseTableFactoryImpl(hadoopConf);

        coreSetup = true;
    }

    public void setupTypeManager() throws Exception {
        if (typeManagerSetup) {
            return;
        }

        idGenerator = new IdGeneratorImpl();
        typeManager = new HBaseTypeManager(idGenerator, hadoopConf, zk, hbaseTableFactory);

        typeManagerSetup = true;
    }

    public void setupRepository() throws Exception {
        if (repositoryManagerSetup) {
            return;
        }

        setupTypeManager();

        blobStoreAccessFactory = createBlobAccess();
        blobManager = new BlobManagerImpl(hbaseTableFactory, blobStoreAccessFactory, false);
        RecordFactory recordFactory = new RecordFactoryImpl(typeManager, idGenerator);

        repositoryManager = new HBaseRepositoryManager(typeManager, idGenerator, recordFactory, hbaseTableFactory, blobManager) {
            @Override
            protected Repository createRepository(String tableName) throws IOException, InterruptedException {
                HBaseRepository repository = (HBaseRepository)super.createRepository(tableName);
                repository.setRecordUpdateHooks(recordUpdateHooks);
                return repository;
            }
        };


        sepModel = new SepModelImpl(new ZooKeeperItfAdapter(zk), hadoopConf);
        eventPublisherManager = new LilyEventPublisherManager(hbaseTableFactory);

        tableManager = new RepositoryTableManagerImpl(hadoopConf, hbaseTableFactory);
        if (!tableManager.tableExists(Table.RECORD.name)) {
            tableManager.createTable(Table.RECORD.name);
        }

        repositoryManagerSetup = true;
    }

    private BlobStoreAccessFactory createBlobAccess() throws Exception {
        DFSBlobStoreAccess dfsBlobStoreAccess = new DFSBlobStoreAccess(hbaseProxy.getBlobFS(), new Path("/lily/blobs"));
        BlobStoreAccess hbaseBlobStoreAccess = new HBaseBlobStoreAccess(hadoopConf);
        BlobStoreAccess inlineBlobStoreAccess = new InlineBlobStoreAccess();

        BlobStoreAccessConfig blobStoreAccessConfig = new BlobStoreAccessConfig(dfsBlobStoreAccess.getId());

        if (hbaseBlobLimit != -1) {
            blobStoreAccessConfig.setLimit(hbaseBlobStoreAccess.getId(), hbaseBlobLimit);
        }

        if (inlineBlobLimit != -1) {
            blobStoreAccessConfig.setLimit(inlineBlobStoreAccess.getId(), inlineBlobLimit);
        }

        List<BlobStoreAccess> blobStoreAccesses =
                Arrays.asList(dfsBlobStoreAccess, hbaseBlobStoreAccess, inlineBlobStoreAccess);
        SizeBasedBlobStoreAccessFactory blobStoreAccessFactory =
                new SizeBasedBlobStoreAccessFactory(blobStoreAccesses, blobStoreAccessConfig);
        return blobStoreAccessFactory;
    }

    /**
     * Set the size limits for the inline and HBase blobs, set to -1 to disable one of these
     * stores.
     */
    public void setBlobLimits(long inlineBlobLimit, long hbaseBlobLimit) {
        this.inlineBlobLimit = inlineBlobLimit;
        this.hbaseBlobLimit = hbaseBlobLimit;
    }

    public void setupRemoteAccess() throws Exception {
        lilyServer = new NettyServer(
                new LilySpecificResponder(AvroLily.class, new AvroLilyImpl(repositoryManager, typeManager, null)
                        ), new InetSocketAddress(0));
        lilyServer.start();

        final AvroConverter avroConverter = new AvroConverter();
        final InetSocketAddress remoteAddr = new InetSocketAddress(lilyServer.getPort());

        remoteSchemaCache = new RemoteTestSchemaCache(zk);
        remoteTypeManager = new RemoteTypeManager(remoteAddr, avroConverter, idGenerator, zk, remoteSchemaCache);
        remoteSchemaCache.setTypeManager(remoteTypeManager);
        RecordFactory recordFactory = new RecordFactoryImpl(typeManager, idGenerator);



        remoteRepositoryManager = new RemoteRepositoryManager(remoteTypeManager, idGenerator, recordFactory,
                new AvroLilyTransceiver(remoteAddr), avroConverter, blobManager, hbaseTableFactory);
        avroConverter.setRepositoryManager(remoteRepositoryManager);

        remoteBlobStoreAccessFactory = createBlobAccess();
        remoteBlobManager = new BlobManagerImpl(hbaseTableFactory, remoteBlobStoreAccessFactory, false);

        remoteSchemaCache.start();
    }

    /**
     * Wait until all currently queued SEP events are processed.
     *
     * <p>New events that are generated after/during a call to this method will not be waited for.</p>
     */
    public void waitForSepProcessing() throws Exception {
        waitForSepProcessing(Table.RECORD.name);
    }

    /**
     * Wait until all currently queued SEP events are processed on a specific table.
     *
     * <p>New events that are generated after/during a call to this method will not be waited for.</p>
     */
    public void waitForSepProcessing(String tableName) throws Exception {
        long timeout = 60000L;
        boolean success = hbaseProxy.waitOnReplication(tableName, timeout);
        if (!success) {
            throw new Exception("Events were not processed within a timeout of " + timeout + "ms");
        }
    }

    public void stop() throws InterruptedException {

        Closer.close(sepConsumer);
        Closer.close(remoteSchemaCache);
        Closer.close(remoteTypeManager);

        Closer.close(typeManager);
        Closer.close(remoteRepositoryManager);
        Closer.close(repositoryManager);

        if (lilyServer != null) {
            lilyServer.close();
            lilyServer.join();
        }

        Closer.close(zk);
        Closer.close(hbaseProxy);
        coreSetup = false;
        repositoryManagerSetup = false;
        typeManagerSetup = false;
    }

    public ZooKeeperItf getZk() {
        return zk;
    }

    public TypeManager getRemoteTypeManager() {
        return remoteTypeManager;
    }

    public RepositoryManager getRepositoryManager() {
        return repositoryManager;
    }

    public RepositoryTableManager getTableManager() {
        return tableManager;
    }

    public RemoteRepositoryManager getRemoteRepositoryManager() {
        return remoteRepositoryManager;
    }


    public SepModel getSepModel() {
        return sepModel;
    }

    public LilyEventPublisherManager getEventPublisherManager() {
        return eventPublisherManager;
    }

    public void stopSepEventSlave() {
        if (sepConsumer == null || !sepConsumer.isRunning()) {
            throw new IllegalStateException("No SepEventSlave to stop");
        }
        sepConsumer.stop();
        sepConsumer = null;
    }

    /**
     * Start a SEP event processor. Only a single SEP event processor is supported within the
     * context of this class.
     *
     * @param subscriptionId The id of the subscription for which the slave will process events
     * @param eventListener The listener to handle incoming events
     */
    public void startSepEventSlave(String subscriptionId, EventListener eventListener) throws Exception {
        if (sepConsumer != null) {
            throw new IllegalStateException("There is already a SepEventSlave running, stop it first");
        }
        sepConsumer = new SepConsumer(subscriptionId, System.currentTimeMillis(), eventListener, 1, "localhost",
                new ZooKeeperItfAdapter(zk), hadoopConf, new LilyPayloadExtractor());
        sepConsumer.start();
    }

    /**
     * Returns a default typemanager.
     */
    public TypeManager getTypeManager() {
        return typeManager;
    }

    /**
     * Returns a new instance of a HBaseTypeManager, different than the default
     * typemanager.
     */
    public TypeManager getNewTypeManager() throws IOException, InterruptedException, KeeperException,
            RepositoryException {
        return new HBaseTypeManager(new IdGeneratorImpl(), hadoopConf, zk, hbaseTableFactory);
    }

    public IdGenerator getIdGenerator() {
        return idGenerator;
    }

    public Configuration getHadoopConf() {
        return hadoopConf;
    }

    public HBaseTableFactory getHbaseTableFactory() {
        return hbaseTableFactory;
    }

    public BlobStoreAccessFactory getBlobStoreAccessFactory() {
        return blobStoreAccessFactory;
    }

    public BlobStoreAccessFactory getRemoteBlobStoreAccessFactory() {
        return remoteBlobStoreAccessFactory;
    }

    public BlobManager getBlobManager() {
        return blobManager;
    }

    public BlobManager getRemoteBlobManager() {
        return remoteBlobManager;
    }

    public HBaseProxy getHBaseProxy() {
        return hbaseProxy;
    }

    private class RemoteTestSchemaCache extends AbstractSchemaCache implements SchemaCache {

        private TypeManager typeManager;

        public RemoteTestSchemaCache(ZooKeeperItf zooKeeper) {
            super(zooKeeper);
        }

        public void setTypeManager(TypeManager typeManager) {
            this.typeManager = typeManager;
        }

        @Override
        protected TypeManager getTypeManager() {
            return typeManager;
        }

    }
}
