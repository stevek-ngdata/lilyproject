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
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.Server;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Entry;
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
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.impl.AbstractSchemaCache;
import org.lilyproject.repository.impl.BlobManagerImpl;
import org.lilyproject.repository.impl.BlobStoreAccessConfig;
import org.lilyproject.repository.impl.DFSBlobStoreAccess;
import org.lilyproject.repository.impl.HBaseBlobStoreAccess;
import org.lilyproject.repository.impl.HBaseRepository;
import org.lilyproject.repository.impl.HBaseTypeManager;
import org.lilyproject.repository.impl.InlineBlobStoreAccess;
import org.lilyproject.repository.impl.SchemaCache;
import org.lilyproject.repository.impl.SizeBasedBlobStoreAccessFactory;
import org.lilyproject.repository.impl.id.IdGeneratorImpl;
import org.lilyproject.repository.remote.RemoteRepository;
import org.lilyproject.repository.remote.RemoteTypeManager;
import org.lilyproject.repository.spi.RecordUpdateHook;
import org.lilyproject.sep.impl.EventListener;
import org.lilyproject.sep.impl.EventPublisher;
import org.lilyproject.sep.impl.HBaseEventPublisher;
import org.lilyproject.sep.impl.SepEventSlave;
import org.lilyproject.sep.impl.SepModel;
import org.lilyproject.util.hbase.HBaseTableFactory;
import org.lilyproject.util.hbase.HBaseTableFactoryImpl;
import org.lilyproject.util.hbase.LilyHBaseSchema;
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
    private HBaseRepository repository;
    private RemoteRepository remoteRepository;

    private Server lilyServer;

    private BlobStoreAccessFactory blobStoreAccessFactory;
    private BlobStoreAccessFactory remoteBlobStoreAccessFactory;

    private BlobManager blobManager;
    private BlobManager remoteBlobManager;
    
    private SepModel sepModel;
    private WrappedSepEventSlave sepEventSlave;
    private WrappedEventPublisher eventPublisher;

    private boolean coreSetup;
    private boolean typeManagerSetup;
    private boolean repositorySetup;
    
    private long hbaseBlobLimit = -1;
    private long inlineBlobLimit = -1;

    private List<RecordUpdateHook> recordUpdateHooks = Collections.emptyList();

    private RemoteTestSchemaCache remoteSchemaCache;

    public void setRecordUpdateHooks(List<RecordUpdateHook> recordUpdateHooks) {
        this.recordUpdateHooks = recordUpdateHooks;
    }

    public void setupCore() throws Exception {
        if (coreSetup)
            return;
        hbaseProxy = new HBaseProxy();
        hbaseProxy.start();
        hadoopConf = hbaseProxy.getConf();
        zk = ZkUtil.connect(hbaseProxy.getZkConnectString(), 10000);

        hbaseTableFactory = new HBaseTableFactoryImpl(hadoopConf);

        coreSetup = true;
    }

    public void setupTypeManager() throws Exception {
        if (typeManagerSetup)
            return;

        idGenerator = new IdGeneratorImpl();
        typeManager = new HBaseTypeManager(idGenerator, hadoopConf, zk, hbaseTableFactory);

        typeManagerSetup = true;
    }

    public void setupRepository(/* FIXME ROWLOG REFACTORING */ boolean withWal) throws Exception {
        if (repositorySetup)
            return;

        setupTypeManager();

        blobStoreAccessFactory = createBlobAccess();
        blobManager = new BlobManagerImpl(hbaseTableFactory, blobStoreAccessFactory, false);

        repository = new HBaseRepository(typeManager, idGenerator, hbaseTableFactory, blobManager);
        repository.setRecordUpdateHooks(recordUpdateHooks);
        
        sepModel = new SepModel(zk, hadoopConf);
        eventPublisher = new WrappedEventPublisher( 
                            new HBaseEventPublisher(LilyHBaseSchema.getRecordTable(hbaseTableFactory)));

        repositorySetup = true;
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
        AvroConverter serverConverter = new AvroConverter();
        serverConverter.setRepository(repository);
        lilyServer = new NettyServer(
                new LilySpecificResponder(AvroLily.class, new AvroLilyImpl(repository, null, serverConverter),
                        serverConverter), new InetSocketAddress(0));
        lilyServer.start();

        final AvroConverter remoteConverter = new AvroConverter();
        final InetSocketAddress remoteAddr = new InetSocketAddress(lilyServer.getPort());

        remoteSchemaCache = new RemoteTestSchemaCache(zk);
        remoteTypeManager = new RemoteTypeManager(remoteAddr, remoteConverter, idGenerator, zk, remoteSchemaCache);
        remoteSchemaCache.setTypeManager(remoteTypeManager);

        remoteBlobStoreAccessFactory = createBlobAccess();
        remoteBlobManager = new BlobManagerImpl(hbaseTableFactory, remoteBlobStoreAccessFactory, false);

        remoteRepository =
                new RemoteRepository(remoteAddr, remoteConverter, remoteTypeManager, idGenerator, remoteBlobManager,
                        getHadoopConf());

        remoteConverter.setRepository(repository);
        remoteTypeManager.start();
        remoteSchemaCache.start();
    }

    public void setupMessageQueue(boolean withProcessor) throws Exception {
        setupMessageQueue(withProcessor, false);
    }

    /**
     * @param withManualProcessing if true, the MQ RowLog will be wrapped to keep track of added messages to allow
     *                             triggering a manual processing, see method {@link #processMQ}. Usually you will want
     *                             either this or withProcessor, not both.
     */
    public void setupMessageQueue(boolean withProcessor, boolean withManualProcessing) throws Exception {
        // FIXME ROWLOG REFACTORING
    }

    /**
     * Wait until all queued SEP procesing is complete. It is assumed that there is something to
     * wait for, as the SEP implementation doesn't provide information on the number of queued
     * events.
     */
    public void waitForSepProcessing() throws Exception {
        
        MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
        
        if (sepEventSlave != null && sepEventSlave.isRunning()) {
            for (int retry = 0; retry < 10; retry++) {
                long lastMutationTimestamp = (Long)platformMBeanServer.getAttribute(new ObjectName(
                        "Lily:service=Repository,name=hbaserepository"), "timestampLastMutation");
                long lastPublishedMessage = eventPublisher.getLastPublishedTimestamp();
                long lastProcessedSepEvent = sepEventSlave.getLastProcessedTimestamp();
                
                long lastOperation = Math.max(lastMutationTimestamp, lastPublishedMessage);
                
                if (Math.abs(lastOperation - lastProcessedSepEvent) < 50) {
                    return;
                }
                Thread.sleep(1000);
            }
        }

        throw new IllegalStateException(
                "Waited for 10 seconds, but no SEP events within 100 milliseconds were encountered");
    }

    public void stop() throws InterruptedException {

        Closer.close(sepEventSlave);
        Closer.close(remoteSchemaCache);
        Closer.close(remoteTypeManager);
        Closer.close(remoteRepository);

        Closer.close(typeManager);
        Closer.close(repository);

        if (lilyServer != null) {
            lilyServer.close();
            lilyServer.join();
        }

        Closer.close(zk);
        Closer.close(hbaseProxy);
        coreSetup = false;
        repositorySetup = false;
        typeManagerSetup = false;
    }

    public void waitForSubscription(String subscriptionId) throws InterruptedException {
        // FIXME ROWLOG REFACTORING
//        boolean subscriptionKnown = false;
//        int timeOut = 10000;
//        long waitUntil = System.currentTimeMillis() + timeOut;
//        while (!subscriptionKnown && System.currentTimeMillis() < waitUntil) {
//            if (rowLog.getSubscriptionIds().contains(subscriptionId)) {
//                subscriptionKnown = true;
//                break;
//            }
//            Thread.sleep(10);
//        }
//        Assert.assertTrue("Subscription '" + subscriptionId + "' not known to rowlog within timeout " + timeOut + "ms",
//                subscriptionKnown);
    }

    public ZooKeeperItf getZk() {
        return zk;
    }

    public TypeManager getRemoteTypeManager() {
        return remoteTypeManager;
    }

    public Repository getRepository() {
        return repository;
    }

    public Repository getRemoteRepository() {
        return remoteRepository;
    }
    
    
    public SepModel getSepModel() {
        return sepModel;
    }
    
    public EventPublisher getEventPublisher() {
        return eventPublisher;
    }
    
    public void stopSepEventSlave() {
        if (sepEventSlave == null || !sepEventSlave.isRunning()) {
            throw new IllegalStateException("No SepEventSlave to stop");
        } 
        sepEventSlave.stop();
        sepEventSlave = null;
    }
    
    /**
     * Start a SEP event processor. Only a single SEP event processor is supported within the
     * context of this class.
     * 
     * @param subscriptionId The id of the subscription for which the slave will process events
     * @param eventListener The listener to handle incoming events
     */
    public void startSepEventSlave(String subscriptionId, EventListener eventListener) throws Exception {
        if (sepEventSlave != null) {
            throw new IllegalStateException("There is already a SepEventSlave running, stop it first");
        }
        sepEventSlave = new WrappedSepEventSlave(subscriptionId, System.currentTimeMillis(), 
                                                    eventListener, 1, "localhost", zk, hadoopConf);
        sepEventSlave.start();
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
    
    /**
     * Wrapper around SepEventSlave to allow keeping track of whether or not events for a given point
     * in time have been processed.
     */
    private static class WrappedSepEventSlave extends SepEventSlave {

        private long lastProcessedTimestamp;
        
        public WrappedSepEventSlave(String subscriptionId, long subscriptionTimestamp, EventListener listener, 
                int threadCnt, String hostName, ZooKeeperItf zk, Configuration hbaseConf) {
            super(subscriptionId, subscriptionTimestamp, listener, threadCnt, hostName, zk, hbaseConf);
        }
        
        @Override
        public void replicateLogEntries(Entry[] entries) throws IOException {
            super.replicateLogEntries(entries);
            if (entries.length > 0){
                lastProcessedTimestamp = entries[entries.length - 1].getKey().getWriteTime();
            }
        }
        
        /**
         * Get the write timestamp of the last processed HLog entry.
         */
        public long getLastProcessedTimestamp() {
            return lastProcessedTimestamp;
        }
        
    }
    
    private static class WrappedEventPublisher implements EventPublisher {
        
        private EventPublisher delegate;
        private long lastPublishedTimestamp;
        
        public WrappedEventPublisher(EventPublisher delegate) {
            this.delegate = delegate;
        }
        
        public long getLastPublishedTimestamp() {
            return lastPublishedTimestamp;
        }
        
        @Override
        public boolean publishMessage(byte[] row, byte[] payload) throws IOException {
            boolean result = delegate.publishMessage(row, payload);
            if (result) {
                lastPublishedTimestamp = System.currentTimeMillis();
            }
            return result;
            
        }
    }
}
