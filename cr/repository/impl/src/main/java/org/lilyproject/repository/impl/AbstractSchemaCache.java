/*
 * Copyright 2011 Outerthought bvba
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
package org.lilyproject.repository.impl;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import javax.annotation.PreDestroy;

import com.google.common.collect.Sets;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.logging.Log;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;
import org.lilyproject.repository.api.*;
import org.lilyproject.util.Logs;
import org.lilyproject.util.Pair;
import org.lilyproject.util.zookeeper.ZkUtil;
import org.lilyproject.util.zookeeper.ZooKeeperItf;


public abstract class AbstractSchemaCache implements SchemaCache {

    protected ZooKeeperItf zooKeeper;
    protected Log log;

    private final CacheRefresher cacheRefresher = new CacheRefresher();

    private FieldTypes fieldTypesSnapshot;
    private FieldTypesCache fieldTypesCache = new FieldTypesCache();
    private volatile boolean updatedFieldTypes = false;

    private RecordTypesCache recordTypes = new RecordTypesCache();

    private Set<CacheWatcher> cacheWatchers = new HashSet<CacheWatcher>();
    private Map<String, Integer> bucketVersions = new HashMap<String, Integer>();
    private ParentWatcher parentWatcher = new ParentWatcher();
    private Integer parentVersion = null;

    protected static final String CACHE_INVALIDATION_PATH = "/lily/typemanager/cache/invalidate";
    protected static final String CACHE_REFRESHENABLED_PATH = "/lily/typemanager/cache/enabled";
    private static final String LILY_NODES_PATH = "/lily/repositoryNodes";
    
    /**
     * Paths we need to watch for existence, since we need to re-initialize after they are recreated.
     * Normally this doesn't happen, but it can happen with the resetLilyState() call of Lily's test
     * framework.
     */
    private static final Set<String> EXISTENCE_PATHS =
            Sets.newHashSet(CACHE_INVALIDATION_PATH, CACHE_REFRESHENABLED_PATH, LILY_NODES_PATH);

    public AbstractSchemaCache(ZooKeeperItf zooKeeper) {
        this.zooKeeper = zooKeeper;
        cacheRefresher.start();
    }

    /**
     * Used to build output as Hex
     */
    private static final char[] DIGITS_LOWER = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd',
            'e', 'f' };
    private ConnectionWatcher connectionWatcher;
    private LilyNodesWatcher lilyNodesWatcher;

    /**
     * Simplified version of {@link Hex#encodeHex(byte[])}
     * <p>
     * In this version we avoid having to create a new byte[] to give to
     * {@link Hex#encodeHex(byte[])}
     */
    public static String encodeHex(byte[] data) {
        char[] out = new char[2];
        // two characters form the hex value.
        out[0] = DIGITS_LOWER[(0xF0 & data[0]) >>> 4];
        out[1] = DIGITS_LOWER[0x0F & data[0]];
        return new String(out);
    }

    /**
     * Decodes a string containing 2 characters representing a hex value.
     * <p>
     * The returned byte[] contains the byte represented by the string and the
     * next byte.
     * <p>
     * This code is based on {@link Hex#decodeHex(char[])}
     * <p>
     * 
     */
    public static byte[] decodeHexAndNextHex(String data) {
        byte[] out = new byte[2];

        // two characters form the hex value.
        int f = Character.digit(data.charAt(0), 16) << 4;
        f = f | Character.digit(data.charAt(1), 16);
        out[0] = (byte) (f & 0xFF);
        out[1] = (byte) ((f + 1) & 0xFF);

        return out;
    }
    
    public static byte[] decodeNextHex(String data) {
        byte[] out = new byte[1];

        // two characters form the hex value.
        int f = Character.digit(data.charAt(0), 16) << 4;
        f = f | Character.digit(data.charAt(1), 16);
        f++;
        out[0] = (byte) (f & 0xFF);

        return out;
    }

    public void start() throws InterruptedException, KeeperException, RepositoryException {
        ZkUtil.createPath(zooKeeper, CACHE_INVALIDATION_PATH);
        for (int i = 0; i < 16; i++) {
            for (int j = 0; j < 16; j++) {
                String bucket = "" + DIGITS_LOWER[i] + DIGITS_LOWER[j];
                ZkUtil.createPath(zooKeeper, bucketPath(bucket));
                cacheWatchers.add(new CacheWatcher(bucket));
            }
        }
        ZkUtil.createPath(zooKeeper, CACHE_REFRESHENABLED_PATH);
        connectionWatcher = new ConnectionWatcher();
        zooKeeper.addDefaultWatcher(connectionWatcher);
        readRefreshingEnabledState();
        refreshAll();
    }

    private String bucketPath(String bucket) {
        return CACHE_INVALIDATION_PATH + "/" + bucket;
    }

    @Override
    @PreDestroy
    public void close() throws IOException {
        try {
            zooKeeper.removeDefaultWatcher(connectionWatcher);
            cacheRefresher.stop();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.debug("Interrupted", e);
        }
    }


    /**
     * Returns the typeManager which the cache can use to request for field
     * types and record types from HBase instead of the cache.
     */
    protected abstract TypeManager getTypeManager();

    @Override
    public FieldTypes getFieldTypesSnapshot() throws InterruptedException {
        if (!updatedFieldTypes)
            return fieldTypesSnapshot;
        synchronized (this) {
            if (updatedFieldTypes) {
                fieldTypesSnapshot = fieldTypesCache.getSnapshot();
                updatedFieldTypes = false;
            }
            return fieldTypesSnapshot;
        }
    }
    
    public void updateFieldType(FieldType fieldType) throws TypeException, InterruptedException {
        fieldTypesCache.update(fieldType);
        updatedFieldTypes = true;
    }

    public void updateRecordType(RecordType recordType) throws TypeException, InterruptedException {
        recordTypes.update(recordType);
    }

    public Collection<RecordType> getRecordTypes() throws InterruptedException {
        return recordTypes.getRecordTypes();
    }

    public RecordType getRecordType(QName name) throws InterruptedException {
        return recordTypes.getRecordType(name);
    }

    public RecordType getRecordType(SchemaId id) {
        return recordTypes.getRecordType(id);
    }

    public FieldType getFieldType(QName name) throws InterruptedException, TypeException {
        return fieldTypesCache.getFieldType(name);
    }

    public FieldType getFieldType(SchemaId id) throws TypeException, InterruptedException {
        return fieldTypesCache.getFieldType(id);
    }

    public List<FieldType> getFieldTypes() throws TypeException, InterruptedException {
        return fieldTypesCache.getFieldTypes();
    }

    public boolean fieldTypeExists(QName name) throws InterruptedException {
        return fieldTypesCache.fieldTypeExists(name);
    }

    public FieldType getFieldTypeByNameReturnNull(QName name) throws InterruptedException {
        return fieldTypesCache.getFieldTypeByNameReturnNull(name);
    }

    protected void readRefreshingEnabledState() {
        // Should only do something on the HBaseTypeManager, not on the
        // RemoteTypeManager
    }

    /**
     * Refresh the caches and put the cacheWatcher again on the cache
     * invalidation zookeeper-node.
     */
    private void refreshAll() throws InterruptedException, RepositoryException {

        watchPathsForExistence();

        // Set a watch on the parent path, in case everything needs to be
        // refreshed
        try {
            Stat stat = new Stat();
            ZkUtil.getData(zooKeeper, CACHE_INVALIDATION_PATH, parentWatcher, stat);
            if (parentVersion == null || (stat.getVersion() != parentVersion)) {
                // An explicit refresh was triggered
                parentVersion = stat.getVersion();
                bucketVersions.clear();
            }
        } catch (KeeperException e) {
            if (Thread.currentThread().isInterrupted()) {
                if (log.isDebugEnabled())
                    log.debug("Failed to put parent watcher on " + CACHE_INVALIDATION_PATH + " : thread interrupted");
            } else {
                log.warn("Failed to put parent watcher on " + CACHE_INVALIDATION_PATH, e);
                // Failed to put our watcher.
                // Relying on the ConnectionWatcher to put it again and
                // initialize the caches.
            }
        }
        
        if (bucketVersions.isEmpty()) {
            // All buckets need to be refreshed

            if (log.isDebugEnabled())
                log.debug("Refreshing all types in the schema cache, no bucket versions known yet");
            // Set a watch again on all buckets
            for (CacheWatcher watcher : cacheWatchers) {
                String bucketId = watcher.getBucket();
                String bucketPath = bucketPath(bucketId);
                Stat stat = new Stat();
                try {
                    ZkUtil.getData(zooKeeper, bucketPath, watcher, stat);
                    bucketVersions.put(bucketId, stat.getVersion());
                } catch (KeeperException e) {
                    if (Thread.currentThread().isInterrupted()) {
                        if (log.isDebugEnabled())
                            log.debug("Failed to put watcher on bucket " + bucketPath + " : thread interrupted");
                    } else {
                        log.warn("Failed to put watcher on bucket " + bucketPath
                                + " - Relying on connection watcher to reinitialize cache", e);
                        // Failed to put our watcher.
                        // Relying on the ConnectionWatcher to put it again and
                        // initialize the caches.
                    }
                }
            }
            // Read all types in one go
            Pair<List<FieldType>, List<RecordType>> types = getTypeManager().getTypesWithoutCache();
            fieldTypesCache.refreshFieldTypes(types.getV1());
            updatedFieldTypes = true;
            recordTypes.refreshRecordTypes(types.getV2());
        } else {
            // Only the changed buckets need to be refreshed.
            // Upon a re-connection event it could be that some updates were
            // missed and the watches were not triggered.
            // By checking the version number of the buckets we know which
            // buckets to refresh.

            Map<String, Integer> newBucketVersions = new HashMap<String, Integer>();
            // Set a watch again on all buckets
            for (CacheWatcher watcher : cacheWatchers) {
                String bucketId = watcher.getBucket();
                String bucketPath = bucketPath(bucketId);
                Stat stat = new Stat();
                try {
                    ZkUtil.getData(zooKeeper, bucketPath, watcher, stat);
                    Integer oldVersion = bucketVersions.get(bucketId); 
                    if (oldVersion == null || (oldVersion != stat.getVersion()))
                        newBucketVersions.put(bucketId, stat.getVersion());
                } catch (KeeperException e) {
                    if (Thread.currentThread().isInterrupted()) {
                        if (log.isDebugEnabled())
                            log.debug("Failed to put watcher on bucket " + bucketPath + " : thread is interrupted");
                    } else {
                        log.warn("Failed to put watcher on bucket " + bucketPath
                                + " - Relying on connection watcher to reinitialize cache", e);
                        // Failed to put our watcher.
                        // Relying on the ConnectionWatcher to put it again and
                        // initialize the caches.
                    }
                }
            }
            if (log.isDebugEnabled())
                log.debug("Refreshing all types in the schema cache, limiting to buckets" + newBucketVersions.keySet());
            for (Entry<String, Integer> entry : newBucketVersions.entrySet()) {
                bucketVersions.put(entry.getKey(), entry.getValue());
                TypeBucket typeBucket = getTypeManager().getTypeBucketWithoutCache(entry.getKey());
                fieldTypesCache.refreshFieldTypeBucket(typeBucket);
                updatedFieldTypes = true;
                recordTypes.refreshRecordTypeBucket(typeBucket);
            }
        }
    }

    /**
     * Refresh the caches for the buckets identified by the watchers and put the
     * cacheWatcher again on the cache invalidation zookeeper-node.
     */
    private void refresh(Set<CacheWatcher> watchers) throws InterruptedException, RepositoryException {
        // Only update one bucket at a time
        // Meanwhile updates on the other buckets could happen.
        // Since the watchers for those other buckets are not set back again
        // this will not trigger extra refreshes.
        boolean first = true;
        for (CacheWatcher watcher : watchers) {
            // Throttle the refreshing. When a burst of updates occur, delaying
            // the refreshing a bit allows for the updates to be performed
            // faster
            if (first) {
                first = false;
                Thread.sleep(50);
            }
            String bucketId = watcher.getBucket();
            String bucketPath = bucketPath(watcher.getBucket());
            Stat stat = new Stat();
            try {
                ZkUtil.getData(zooKeeper, bucketPath, watcher, stat);
                if (stat.getVersion() == bucketVersions.get(bucketId))
                        continue; // The bucket is up to date
            } catch (KeeperException e) {
                if (Thread.currentThread().isInterrupted()) {
                    if (log.isDebugEnabled())
                        log.debug("Failed to put watcher on bucket " + bucketPath + " : thread is interrupted");
                } else {
                    log.warn("Failed to put watcher on bucket " + bucketPath
                            + " - Relying on connection watcher to reinitialize cache", e);
                    // Failed to put our watcher.
                    // Relying on the ConnectionWatcher to put it again and
                    // initialize the caches.
                }
            }
            if (log.isDebugEnabled())
                log.debug("Refreshing schema cache bucket: " + bucketId);

            // Avoid updating the cache while refreshing the buckets
            bucketVersions.put(bucketId, stat.getVersion());
            TypeBucket typeBucket = getTypeManager().getTypeBucketWithoutCache(bucketId);
            fieldTypesCache.refreshFieldTypeBucket(typeBucket);
            recordTypes.refreshRecordTypeBucket(typeBucket);
        }
        updatedFieldTypes = true;
    }

    private void watchPathsForExistence() throws InterruptedException {
        for (String path : EXISTENCE_PATHS) {
            try {
                zooKeeper.exists(path, true);
            } catch (KeeperException e) {
                if (Thread.currentThread().isInterrupted()) {
                    if (log.isDebugEnabled())
                        log.debug("Failed to put existence watcher on " + path + " : thread is interrupted");
                } else {
                    log.warn("Failed to put existence watcher on " + path, e);
                }
            }
        }
    }

    /**
     * Cache refresher refreshes the cache when flagged.
     * <p>
     * The {@link CacheWatcher} monitors the cache invalidation flag on
     * Zookeeper. When this flag changes it will call {@link #needsRefresh} on
     * the CacheRefresher, setting the needsRefresh flag. This is the only thing
     * the CacheWatcher does. Thereby it can return quickly when it received an
     * event.<br/>
     * 
     * The CacheRefresher in its turn will notice the needsRefresh flag being
     * set and will refresh the cache. It runs in a separate thread so that we
     * can avoid that the refresh work would be done in the thread of the
     * watcher.
     */
    private class CacheRefresher implements Runnable {
        private volatile boolean needsRefresh;
        private volatile boolean needsRefreshAll;
        private volatile boolean lilyNodesChanged;
        private volatile boolean stop;
        private final Object needsRefreshLock = new Object();
        private Set<CacheWatcher> needsRefreshWatchers = new HashSet<CacheWatcher>();
        private Thread thread;
        private List<String> knownLilyNodes = new ArrayList<String>();

        public void start() {
            thread = new Thread(this, "TypeManager cache refresher");
            thread.setDaemon(true); // Since this might be used in clients
            thread.start();
        }

        public void stop() throws InterruptedException {
            stop = true;
            if (thread != null) {
                thread.interrupt();
                Logs.logThreadJoin(thread);
                thread.join();
                thread = null;
            }
        }

        public void needsRefreshAll() {
            synchronized (needsRefreshLock) {
                needsRefreshAll = true;
                needsRefreshLock.notifyAll();
            }
        }

        public void needsRefresh(CacheWatcher watcher) {
            synchronized (needsRefreshLock) {
                needsRefresh = true;
                needsRefreshWatchers.add(watcher);
                needsRefreshLock.notifyAll();
            }
        }

        public void lilyNodesChanged() {
            synchronized (needsRefreshLock) {
                lilyNodesChanged = true;
                needsRefreshLock.notifyAll();
            }
        }

        private List<String> getLilyNodes() throws KeeperException, InterruptedException {
            LilyNodesWatcher watcher = lilyNodesWatcher;
            if (watcher == null) {
                watcher = new LilyNodesWatcher();
            }
            List<String> lilyNodes = null;
            try {
                lilyNodes = zooKeeper.getChildren(LILY_NODES_PATH, watcher);
                lilyNodesWatcher = watcher;
            } catch (KeeperException e) {
                if (log.isDebugEnabled())
                    log.debug("Failed getting lilyNodes from Zookeeper", e);
                // The path does not exist yet.
                // Set the lilyNodesWatcher to null so that we retry
                // setting the watcher in the next iteration.
                lilyNodesWatcher = null;
            }
            return lilyNodes;
        }

        @Override
        public void run() {
            while (!stop && !Thread.interrupted()) {
                try {
                    // Check if the lily nodes changed
                    // or if the LilyNodesWatcher has not been set yet
                    if (lilyNodesChanged || lilyNodesWatcher == null) {
                        synchronized (needsRefreshLock) {
                            List<String> lilyNodes = getLilyNodes();
                            if (lilyNodes != null) {
                                if (knownLilyNodes.isEmpty()) {
                                    knownLilyNodes.addAll(lilyNodes);
                                } else {
                                    if (!lilyNodes.containsAll(knownLilyNodes)) {
                                        // One or more of the nodes disappeared.
                                        // There is a chance that a refresh
                                        // trigger was not sent out by that
                                        // node.
                                        needsRefreshAll = true;
                                        // Set parentVersion to null to
                                        // explicitly refresh all buckets.
                                        // Otherwise only buckets that got an
                                        // update trigger will be refreshed.
                                        // But because such a trigger could have
                                        // been missed is why we are here.
                                        parentVersion = null;
                                        if (log.isDebugEnabled())
                                            log.debug("One or more LilyNodes stopped. "
                                                    + "Refreshing cache to cover possibly missed refresh triggers");
                                    }
                                    knownLilyNodes.clear();
                                    knownLilyNodes.addAll(lilyNodes);
                                }
                            } else if (!knownLilyNodes.isEmpty()) {
                                needsRefreshAll = true;
                                knownLilyNodes.clear();
                            }
                            lilyNodesChanged = false;
                        }
                    }

                    // Check if something needs to be refreshed
                    if (needsRefresh || needsRefreshAll) {
                        Set<CacheWatcher> watchers = null;
                        boolean refreshAll = false;
                        synchronized (needsRefreshLock) {
                            if (needsRefreshAll) {
                                refreshAll = true;
                            } else {
                                watchers = new HashSet<CacheWatcher>(needsRefreshWatchers);
                            }
                            needsRefreshWatchers.clear();
                            needsRefresh = false;
                            needsRefreshAll = false;
                        }
                        // Do the actual refresh outside the synchronized block
                        if (refreshAll) {
                            refreshAll();
                        } else {
                            refresh(watchers);
                        }
                    }

                    synchronized (needsRefreshLock) {
                        if (!needsRefresh && !needsRefreshAll && !stop) {
                            needsRefreshLock.wait();
                        }
                    }
                } catch (InterruptedException e) {
                    return;
                } catch (Throwable t) {
                    // Sometimes InterruptedException is wrapped in IOException (from hbase)
                    // or RemoteException (from avro), and sometimes these by themselves are again
                    // wrapped in a TypeException of Lily.
                    Throwable cause = t.getCause();
                    while (cause != null) {
                        if (cause instanceof InterruptedException) {
                            return;
                        }
                        cause = cause.getCause();
                    }
                    log.error("Error refreshing type manager cache. Cache is possibly out of date!", t);
                }
            }
        }
    }

    /**
     * The Cache watcher monitors events on the cache invalidation flag.
     */
    private class CacheWatcher implements Watcher {
        private final String bucket;

        public CacheWatcher(String bucket) {
            this.bucket = bucket;
        }

        public String getBucket() {
            return bucket;
        }

        @Override
        public void process(WatchedEvent event) {
            if (EventType.NodeDataChanged.equals(event.getType())) {
                cacheRefresher.needsRefresh(this);
            }
        }
    }

    /**
     * This watcher will be triggered when an explicit refresh is requested.
     * <p>
     * It is put on the parent path: CACHE_INVALIDATION_PATH
     * 
     * @see {@link TypeManager#triggerSchemaCacheRefresh()}
     */
    private class ParentWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            if (EventType.NodeDataChanged.equals(event.getType())) {
                cacheRefresher.needsRefreshAll();
            }
        }
    }

    /**
     * The ConnectionWatcher monitors for zookeeper (re-)connection events.
     * <p>
     * It will set the cache invalidation flag if needed and refresh the caches
     * since events could have been missed while being disconnected.
     */
    protected class ConnectionWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            if (EventType.None.equals(event.getType()) && KeeperState.SyncConnected.equals(event.getState())) {
                readRefreshingEnabledState();
                cacheRefresher.needsRefreshAll();
            } else if (EXISTENCE_PATHS.contains(event.getPath())) {
                if (EventType.NodeCreated.equals(event.getType())) {
                    if (event.getPath().equals(CACHE_REFRESHENABLED_PATH)) {
                        readRefreshingEnabledState();
                    }
                    if (event.getPath().equals(CACHE_INVALIDATION_PATH)) {
                        // Handle things to survive resetLilyState:
                        //  - ZK bucket version numbers won't be relevant anymore
                        bucketVersions.clear();

                        //  - Since the cache invalidation path is new, the schema situation is new,
                        //    forget what is currently in the caches
                        fieldTypesCache.clear();
                        recordTypes.clear();

                        cacheRefresher.needsRefreshAll();
                    }
                    if (event.getPath().equals(LILY_NODES_PATH)) {
                        lilyNodesWatcher = null;
                        cacheRefresher.needsRefreshAll();
                    }
                } else if (EventType.NodeDeleted.equals(event.getType())) {
                    try {
                        watchPathsForExistence();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    protected class LilyNodesWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            cacheRefresher.lilyNodesChanged();
        }
    }
}
