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
package org.lilyproject.client;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.lilyproject.repository.api.IdGenerator;

import org.lilyproject.repository.api.TypeManager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.avro.AvroConverter;
import org.lilyproject.indexer.Indexer;
import org.lilyproject.indexer.RemoteIndexer;
import org.lilyproject.repository.api.BlobManager;
import org.lilyproject.repository.api.BlobStoreAccess;
import org.lilyproject.repository.api.RecordFactory;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.RepositoryManager;
import org.lilyproject.repository.api.RepositoryTableManager;
import org.lilyproject.repository.impl.BlobManagerImpl;
import org.lilyproject.repository.impl.BlobStoreAccessConfig;
import org.lilyproject.repository.impl.DFSBlobStoreAccess;
import org.lilyproject.repository.impl.HBaseBlobStoreAccess;
import org.lilyproject.repository.impl.InlineBlobStoreAccess;
import org.lilyproject.repository.impl.RecordFactoryImpl;
import org.lilyproject.repository.impl.RepositoryTableManagerImpl;
import org.lilyproject.repository.impl.SizeBasedBlobStoreAccessFactory;
import org.lilyproject.repository.impl.id.IdGeneratorImpl;
import org.lilyproject.repository.remote.AvroLilyTransceiver;
import org.lilyproject.repository.remote.RemoteRepositoryManager;
import org.lilyproject.repository.remote.RemoteTypeManager;
import org.lilyproject.util.hbase.HBaseTableFactory;
import org.lilyproject.util.hbase.HBaseTableFactoryImpl;
import org.lilyproject.util.hbase.LilyHBaseSchema.Table;
import org.lilyproject.util.hbase.LocalHTable;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.json.JsonFormat;
import org.lilyproject.util.repo.DfsUri;
import org.lilyproject.util.zookeeper.ZkConnectException;
import org.lilyproject.util.zookeeper.ZkUtil;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

/**
 * Provides remote repository implementations.
 *
 * <p>Connects to zookeeper to find out available repository nodes.
 */
public class LilyClient implements Closeable, RepositoryManager {
    private ZooKeeperItf zk;
    private boolean managedZk;
    private List<ServerNode> servers = Collections.synchronizedList(new ArrayList<ServerNode>());
    private Set<String> serverAddresses = new HashSet<String>();
    private Set<String> lilyHosts = Collections.emptySet();
    private RetryConf retryConf = new RetryConf();
    private static final String nodesPath = "/lily/repositoryNodes";
    private static final String hbaseConfigPath = "/lily/hbaseConfig";
    private static final String blobDfsUriPath = "/lily/blobStoresConfig/dfsUri";
    private static final String blobStoreAccessConfigPath = "/lily/blobStoresConfig/accessConfig";
    private static AtomicInteger hbaseConfCounter = new AtomicInteger();

    private Log log = LogFactory.getLog(getClass());

    private ZkWatcher watcher = new ZkWatcher();

    private BalancingAndRetryingLilyConnection balancingAndRetryingLilyConnection =
            BalancingAndRetryingLilyConnection.getInstance(this);
    private RemoteSchemaCache schemaCache;
    private HBaseConnections hbaseConnections = new HBaseConnections();

    private boolean isClosed = true;
    
    /**
     * @throws NoServersException if the znode under which the repositories are published does not exist
     */
    public LilyClient(String zookeeperConnectString, int sessionTimeout) throws IOException, InterruptedException,
            KeeperException, ZkConnectException, NoServersException, RepositoryException {
        this(ZkUtil.connect(zookeeperConnectString, sessionTimeout));
        managedZk = true;
    }

    public LilyClient(ZooKeeperItf zk) throws IOException, InterruptedException, KeeperException, ZkConnectException,
            NoServersException, RepositoryException {
        this.zk = zk;
        schemaCache = new RemoteSchemaCache(zk, this);
        init();
    }



    private void init() throws InterruptedException, KeeperException, NoServersException, RepositoryException {
        this.isClosed = false; // needs to be before refreshServers and schemaCache.start()
        zk.addDefaultWatcher(watcher);
        refreshServers();
        schemaCache.start();
    }

    @Override
    public void close() throws IOException {
        this.isClosed = true;

        zk.removeDefaultWatcher(watcher);

        schemaCache.close();

        synchronized (this) {
            for (ServerNode node : servers) {
                node.close();
            }
            servers.clear();
        }

        if (managedZk && zk != null) {
            zk.close();
        }

        // Close pools related to the Configuration objects managed by this LilyClient instance
        for (Configuration config : hbaseConnections.getConfigurations()) {
            LocalHTable.closePool(config);
        }

        // Close HBase connections created by [only] this LilyClient instance.
        // This will almost always contain only one connection, if not we would need a more
        // advanced connection mgmt so that these connections don't stay open for the lifetime
        // of LilyClient.
        Closer.close(hbaseConnections);
    }

    /**
     * Returns if the connection to the lily server has been closed.
     */
    public boolean isClosed() {
        return isClosed;
    }


    /**
     * Returns the default plain repository.
     * @return the default plain repository
     * @see #getPlainRepository(String)
     */
    public Repository getPlainRepository() throws IOException, NoServersException, InterruptedException,
            KeeperException, RepositoryException {
        return getPlainRepository(Table.RECORD.name);
    }
    
    /**
     * Returns a Repository that uses one of the available Lily servers (randomly selected).
     * This repository instance will not automatically retry operations and to balance requests
     * over multiple Lily servers, you need to recall this method regularly to retrieve other
     * repository instances. Most of the time, you will rather use {@link #getRepository(String)}.
     */
    public Repository getPlainRepository(String tableName) throws IOException, InterruptedException, NoServersException, RepositoryException, KeeperException {
        if (isClosed) {
            throw new IllegalStateException("This LilyClient is closed.");
        }

        return getServerNode().repoMgr.getRepository(tableName);
    }
    
    /**
     * Get a {@link RepositoryTableManager} for handling the lifecycle of repository tables.
     */
    public RepositoryTableManager getTableManager() {
        Configuration conf = getHBaseConfiguration(zk);
        return new RepositoryTableManagerImpl(conf, new HBaseTableFactoryImpl(conf));
    }

    private synchronized ServerNode getServerNode() throws NoServersException, RepositoryException, IOException,
            InterruptedException, KeeperException {
        if (servers.size() == 0) {
            throw new NoServersException("No servers available");
        }

        int pos = (int) Math.floor(Math.random() * servers.size());
        ServerNode server = servers.get(pos);
        if (server.repoMgr == null) {
            server.repoMgr = constructRepositoryManager(server);
        }
        if (server.indexer == null) {
            constructIndexer(server);
        }

        return server;
    }

    /**
     * Returns a repository instance which will automatically balance requests over the available
     * Lily servers, and will retry operations according to what is specified in {@link RetryConf}.
     *
     * <p>To see some information when the client goes into retry mode, enable INFO logging for
     * the category org.lilyproject.client.
     */
    public Repository getRepository() {
        return getRepository(Table.RECORD.name);
    }
    
    /**
     * Returns a repository instance that is bound to a non-default storage table. The returned
     * repository will automatically balance requests over the available Lily servers, and will
     * retry operations according to what is specified in {@link RetryConf}.
     * @param tableName name of the storage table for the requested repository
     */
    @Override
    public Repository getRepository(String tableName) {
        if (isClosed) {
            throw new IllegalStateException("This LilyClient is closed.");
        }
        return balancingAndRetryingLilyConnection.getRepository(tableName);
    }
    
    
    @Override
    public Repository getDefaultRepository() throws IOException, InterruptedException {
        return balancingAndRetryingLilyConnection.getDefaultRepository();
    }
    
    @Override
    public RecordFactory getRecordFactory() {
        return balancingAndRetryingLilyConnection.getRecordFactory();
    }
    
    @Override
    public TypeManager getTypeManager() {
        return balancingAndRetryingLilyConnection.getTypeManager();
    }
    
    @Override
    public IdGenerator getIdGenerator() {
        return balancingAndRetryingLilyConnection.getIdGenerator();
    }
    

    /**
     * Returns an Indexer that uses one of the available Lily servers (randomly selected).
     * This indexer instance will not automatically retry operations and to balance requests
     * over multiple Lily servers, you need to recall this method regularly to retrieve other
     * indexer instances. Most of the time, you will rather use {@link #getIndexer()}.
     */
    public Indexer getPlainIndexer() throws IOException, NoServersException, InterruptedException,
            KeeperException, RepositoryException {
        if (isClosed) {
            throw new IllegalStateException("This LilyClient is closed.");
        }

        return getServerNode().indexer;
    }

    /**
     * Returns an indexer instance which will automatically balance requests over the available
     * Lily servers, and will retry operations according to what is specified in {@link RetryConf}.
     *
     * <p>To see some information when the client goes into retry mode, enable INFO logging for
     * the category org.lilyproject.client.
     */
    public Indexer getIndexer() {
        if (isClosed) {
            throw new IllegalStateException("This LilyClient is closed.");
        }
        return balancingAndRetryingLilyConnection.getIndexer();
    }

    public RetryConf getRetryConf() {
        return retryConf;
    }

    public void setRetryConf(RetryConf retryConf) {
        this.retryConf = retryConf;
    }

    private RepositoryManager constructRepositoryManager(ServerNode server) throws IOException, InterruptedException {
        
        IdGeneratorImpl idGenerator = new IdGeneratorImpl();
        Configuration hbaseConf = getNewOrExistingConfiguration(zk);
        BlobManager blobManager = getBlobManager(zk, hbaseConf);
        InetSocketAddress lilySocketAddr = parseAddressAndPort(server.lilyAddressAndPort);
        AvroLilyTransceiver transceiver = new AvroLilyTransceiver(lilySocketAddr);
        HBaseTableFactoryImpl tableFactory = new HBaseTableFactoryImpl(hbaseConf);
        AvroConverter avroConverter = new AvroConverter();
        RemoteTypeManager remoteTypeManager = new RemoteTypeManager(lilySocketAddr, avroConverter, idGenerator, zk, schemaCache);
        RecordFactory recordFactory = new RecordFactoryImpl(remoteTypeManager, idGenerator);
        RepositoryManager repositoryManager = new RemoteRepositoryManager(remoteTypeManager, idGenerator, recordFactory,
                transceiver, avroConverter, blobManager, tableFactory);
        avroConverter.setRepositoryManager(repositoryManager);
        return repositoryManager;
    }

    public static BlobManager getBlobManager(ZooKeeperItf zk, Configuration configuration) throws IOException, InterruptedException {
        HBaseTableFactory hbaseTableFactory = new HBaseTableFactoryImpl(configuration);

        URI dfsUri = getDfsUri(zk);
        FileSystem fs = FileSystem.get(DfsUri.getBaseDfsUri(dfsUri), configuration);
        Path blobRootPath = new Path(DfsUri.getDfsPath(dfsUri));

        BlobStoreAccess dfsBlobStoreAccess = new DFSBlobStoreAccess(fs, blobRootPath);
        BlobStoreAccess hbaseBlobStoreAccess = new HBaseBlobStoreAccess(configuration, true);
        BlobStoreAccess inlineBlobStoreAccess = new InlineBlobStoreAccess();
        List<BlobStoreAccess> blobStoreAccesses =
                Arrays.asList(dfsBlobStoreAccess, hbaseBlobStoreAccess, inlineBlobStoreAccess);

        SizeBasedBlobStoreAccessFactory blobStoreAccessFactory =
                new SizeBasedBlobStoreAccessFactory(blobStoreAccesses, getBlobStoreAccessConfig(zk));

        return new BlobManagerImpl(hbaseTableFactory, blobStoreAccessFactory, true);
    }

    private static BlobStoreAccessConfig getBlobStoreAccessConfig(ZooKeeperItf zk) {
        try {
            return new BlobStoreAccessConfig(zk.getData(blobStoreAccessConfigPath, false, new Stat()));
        } catch (Exception e) {
            throw new RuntimeException(
                    "Blob stores config lookup: failed to get blob store access config from ZooKeeper", e);
        }
    }

    private static URI getDfsUri(ZooKeeperItf zk) {
        try {
            return new URI(new String(zk.getData(blobDfsUriPath, false, new Stat())));
        } catch (Exception e) {
            throw new RuntimeException("Blob stores config lookup: failed to get DFS URI from ZooKeeper", e);
        }
    }

    public static Configuration getHBaseConfiguration(ZooKeeperItf zk) {
        try {
            Configuration configuration = HBaseConfiguration.create();

            // Make this configuration object 'owned' by us: this way we can close the associated
            // connection completely without worrying we close someone else's connection (e.g.
            // when running two LilyClient's in one JVM, or when running LilyClient from within
            // the lily-server JVM, which is the case when we launch a batch build job)
            configuration.set(HConstants.HBASE_CLIENT_INSTANCE_ID, String.valueOf("lilyclient-" + hbaseConfCounter.incrementAndGet()));

            byte[] data = zk.getData(hbaseConfigPath, false, new Stat());
            ObjectNode propertiesNode = (ObjectNode) JsonFormat.deserializeSoft(data, "HBase configuration");
            Iterator<Map.Entry<String, JsonNode>> it = propertiesNode.getFields();
            while (it.hasNext()) {
                Map.Entry<String, JsonNode> entry = it.next();
                configuration.set(entry.getKey(), entry.getValue().getTextValue());
            }
            return configuration;
        } catch (Exception e) {
            throw new RuntimeException("Failed to get HBase configuration from ZooKeeper", e);
        }
    }

    private Configuration getNewOrExistingConfiguration(ZooKeeperItf zk) {
        return hbaseConnections.getExisting(getHBaseConfiguration(zk));
    }

    private void constructIndexer(ServerNode server) throws IOException, InterruptedException, KeeperException,
            RepositoryException {

        server.indexer = new RemoteIndexer(parseAddressAndPort(server.lilyAddressAndPort), new AvroConverter());
    }

    private InetSocketAddress parseAddressAndPort(String addressAndPort) {
        int colonPos = addressAndPort.indexOf(":");
        if (colonPos == -1) {
            // since these are produced by the server nodes, this should never occur
            throw new RuntimeException("Unexpected situation: invalid addressAndPort: " + addressAndPort);
        }

        String address = addressAndPort.substring(0, colonPos);
        int port = Integer.parseInt(addressAndPort.substring(colonPos + 1));

        return new InetSocketAddress(address, port);
    }

    private class ServerNode {
        private String lilyAddressAndPort;
        private RepositoryManager repoMgr;
        private Indexer indexer;

        public ServerNode(String lilyAddressAndPort) {
            this.lilyAddressAndPort = lilyAddressAndPort;
        }

        public void close() {
            Closer.close(repoMgr);
            Closer.close(indexer);
        }
    }

    private synchronized void refreshServers() throws InterruptedException, KeeperException {
        if (isClosed()) {
            return;
        }

        Set<String> currentServers = new HashSet<String>();

        boolean retry;
        do {
            retry = false;
            try {
                currentServers.addAll(zk.getChildren(nodesPath, true));
            } catch (KeeperException.NoNodeException e) {
                // The path does not exist: this can happen if the client is started before
                // any Lily server has ever been started, or when using the LilyLauncher
                // from the test framework and calling its resetLilyState JMX operation.
                // In this case, put a watcher to be notified when the path is created.
                Stat stat = zk.exists(nodesPath, true);
                if (stat == null) {
                    if (log.isInfoEnabled()) {
                        log.info("The path with Lily servers does not exist in ZooKeeper: " + nodesPath);
                    }
                    clearServers();
                    return;
                } else {
                    // The node was created in between the getChildren and exists calls: retry
                    retry = true;
                }
            }
        } while (retry);

        Set<String> removedServers = new HashSet<String>();
        removedServers.addAll(serverAddresses);
        removedServers.removeAll(currentServers);

        Set<String> newServers = new HashSet<String>();
        newServers.addAll(currentServers);
        newServers.removeAll(serverAddresses);

        if (log.isDebugEnabled()) {
            log.debug("# current servers in ZK: " + currentServers.size() + ", # added servers: " +
                    newServers.size() + ", # removed servers: " + removedServers.size());
        }

        // Remove removed servers
        Iterator<ServerNode> serverIt = servers.iterator();
        while (serverIt.hasNext()) {
            ServerNode server = serverIt.next();
            if (removedServers.contains(server.lilyAddressAndPort)) {
                serverIt.remove();
                server.close();
            }
        }
        serverAddresses.removeAll(removedServers);

        // Add new servers
        for (String server : newServers) {
            servers.add(new ServerNode(server));
            serverAddresses.add(server);
        }

        Set<String> lilyHosts = new HashSet<String>();
        for (String address : serverAddresses) {
            int colonPos = address.indexOf(":");
            String hostName = address.substring(0, colonPos);
            lilyHosts.add(hostName);
        }
        this.lilyHosts = Collections.unmodifiableSet(lilyHosts);

        if (log.isInfoEnabled()) {
            log.info("Current Lily servers = " + serverAddresses.toString());
        }
    }

    private synchronized void clearServers() {
        Iterator<ServerNode> serverIt = servers.iterator();
        while (serverIt.hasNext()) {
            ServerNode server = serverIt.next();
            serverIt.remove();
            server.close();
        }

        serverAddresses.clear();
    }

    public synchronized Set<String> getLilyHostnames() {
        return lilyHosts;
    }

    private class ZkWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            try {
                if (event.getState() != Event.KeeperState.SyncConnected) {
                    if (log.isInfoEnabled()) {
                        log.info("Not connected to ZooKeeper, will clear list of servers.");
                    }
                    clearServers();
                } else {
                    // We refresh the servers not only when /lily/repositoryNodes has changed, but also
                    // when we get a SyncConnected event, since our watcher might not be installed anymore
                    // (we do not have to check if it was still installed: ZK ignores double registration
                    // of the same watcher object)
                    refreshServers();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (KeeperException.ConnectionLossException e) {
                clearServers();
            } catch (Throwable t) {
                log.error("Error in ZooKeeper watcher.", t);
            }
        }
    }
}
