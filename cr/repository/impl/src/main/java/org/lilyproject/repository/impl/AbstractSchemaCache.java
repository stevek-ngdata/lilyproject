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

import javax.annotation.PreDestroy;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.logging.Log;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.lilyproject.repository.api.*;
import org.lilyproject.util.Logs;
import org.lilyproject.util.Pair;
import org.lilyproject.util.zookeeper.ZkUtil;
import org.lilyproject.util.zookeeper.ZooKeeperItf;


public abstract class AbstractSchemaCache implements SchemaCache {

    protected ZooKeeperItf zooKeeper;
    protected Log log;

    private final CacheRefresher cacheRefresher = new CacheRefresher();

    private FieldTypesImpl fieldTypes;
    private RecordTypesImpl recordTypes = new RecordTypesImpl();
    private FieldTypesImpl updatingFieldTypes = new FieldTypesImpl();
    private boolean updatedFieldTypes = false;

    private Set<CacheWatcher> cacheWatchers = new HashSet<CacheWatcher>();
    private AllCacheWatcher allCacheWatcher;

    protected static final String CACHE_INVALIDATION_PATH = "/lily/typemanager/cache/invalidate";
    protected static final String CACHE_REFRESHENABLED_PATH = "/lily/typemanager/cache/enabled";

    public AbstractSchemaCache(ZooKeeperItf zooKeeper) {
        this.zooKeeper = zooKeeper;
        cacheRefresher.start();
    }

    /**
     * Used to build output as Hex
     */
    private static final char[] DIGITS_LOWER = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd',
            'e', 'f' };

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
        allCacheWatcher = new AllCacheWatcher();
        for (int i = 0; i < 16; i++) {
            for (int j = 0; j < 16; j++) {
                String bucket = "" + DIGITS_LOWER[i] + DIGITS_LOWER[j];
                ZkUtil.createPath(zooKeeper, bucketPath(bucket));
                cacheWatchers.add(new CacheWatcher(bucket));
            }
        }
        ZkUtil.createPath(zooKeeper, CACHE_REFRESHENABLED_PATH);
        zooKeeper.addDefaultWatcher(new ConnectionWatcher());
        readRefreshingEnabledState();
        refreshAll();
    }

    private String bucketPath(String bucket) {
        return CACHE_INVALIDATION_PATH + "/" + bucket;
    }

    @PreDestroy
    public void close() throws IOException {
        try {
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
    public synchronized FieldTypesImpl getFieldTypesSnapshot() {
        if (updatedFieldTypes) {
            fieldTypes = updatingFieldTypes.clone();
            updatedFieldTypes = false;
        }
        return fieldTypes;
    }
    
    public synchronized void updateFieldType(FieldType fieldType) throws TypeException, InterruptedException {
        updatingFieldTypes.update(fieldType);
        updatedFieldTypes = true;
    }

    public synchronized void updateRecordType(RecordType recordType) throws TypeException, InterruptedException {
        recordTypes.update(recordType);
    }

    public synchronized Collection<RecordType> getRecordTypes() {
        return recordTypes.getRecordTypes();
    }

    public synchronized RecordType getRecordType(QName name) {
        return recordTypes.getRecordType(name);
    }

    public synchronized RecordType getRecordType(SchemaId id) {
        return recordTypes.getRecordType(id);
    }

    public FieldType getFieldType(QName name) throws FieldTypeNotFoundException {
        return getFieldTypesSnapshot().getFieldType(name);
    }

    public FieldType getFieldType(SchemaId id) throws FieldTypeNotFoundException {
        return getFieldTypesSnapshot().getFieldType(id);
    }

    public List<FieldType> getFieldTypes() {
        return getFieldTypesSnapshot().getFieldTypes();
    }

    protected void readRefreshingEnabledState() {
        // Should only do something on the HBaseTypeManager, not on the
        // RemoteTypeManager
    }

    protected void cacheInvalidationReconnected() throws InterruptedException, TypeException {
        // Should only do something on the HBaseTypeManager, not on the
        // RemoteTypeManager
    }

    /**
     * Refresh the caches and put the cacheWatcher again on the cache
     * invalidation zookeeper-node.
     */
    private synchronized void refreshAll() throws InterruptedException, RepositoryException {
        if (log.isDebugEnabled())
            log.debug("Refreshing all types in the schema cache");

        List<String> buckets = null;
        // Set a watch again on all buckets
        for (CacheWatcher watcher : cacheWatchers) {
            try {
                zooKeeper.getData(bucketPath(watcher.getBucket()), watcher, null);
            } catch (KeeperException e) {
                // Failed to put our watcher.
                // Relying on the ConnectionWatcher to put it again and
                // initialize the caches.
            }
        }
        // Set a watch on the parent path, in case everything needs to be
        // refreshed
        try {
            zooKeeper.getData(CACHE_INVALIDATION_PATH, allCacheWatcher, null);
        } catch (KeeperException e) {
            // Failed to put our watcher.
            // Relying on the ConnectionWatcher to put it again and
            // initialize the caches.
        }
        Pair<List<FieldType>, List<RecordType>> types = getTypeManager().getTypesWithoutCache();
        updatingFieldTypes.refreshFieldTypes(types.getV1());
        updatedFieldTypes = true;
        recordTypes.refreshRecordTypes(types.getV2());
    }

    /**
     * Refresh the caches for the buckets identified by the watchers and put the
     * cacheWatcher again on the cache invalidation zookeeper-node.
     */
    private synchronized void refresh(Set<CacheWatcher> watchers) throws InterruptedException, RepositoryException {
        List<String> buckets = new ArrayList<String>(watchers.size());
        for (CacheWatcher watcher : watchers) {
            buckets.add(watcher.getBucket());
            try {
                zooKeeper.getData(bucketPath(watcher.getBucket()), watcher, null);
            } catch (KeeperException e) {
                // Failed to put our watcher.
                // Relying on the ConnectionWatcher to put it again and
                // initialize the caches.
            }
        }
        if (log.isDebugEnabled())
            log.debug("Refreshing schema cache buckets: " + Arrays.toString(buckets.toArray()));

        List<TypeBucket> typeBuckets = getTypeManager().getTypeBucketsWithoutCache(buckets);
        updatingFieldTypes.refreshFieldTypeBuckets(typeBuckets);
        updatedFieldTypes = true;
        recordTypes.refreshRecordTypeBuckets(typeBuckets);
    }

    /**
     * Cache refresher refreshes the cache when flagged.
     * <p>
     * The {@link CacheWatcher} monitors the cache invalidation flag on
     * Zookeeper. When this flag changes it will call {@link #needsRefresh()} on
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
        private volatile boolean stop;
        private final Object needsRefreshLock = new Object();
        private Set<CacheWatcher> needsRefreshWatchers = new HashSet<CacheWatcher>();
        private Thread thread;

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
                needsRefresh = true;
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

        @Override
        public void run() {
            while (!stop && !Thread.interrupted()) {
                try {
                    if (needsRefresh) {
                        Set<CacheWatcher> watchers = null;
                        synchronized (needsRefreshLock) {
                            if (!needsRefreshAll) {
                                watchers = new HashSet<CacheWatcher>(needsRefreshWatchers);
                            }
                            needsRefreshWatchers.clear();
                            needsRefresh = false;
                            needsRefreshAll = false;
                        }
                        if (watchers == null || watchers.isEmpty()) {
                            refreshAll();
                        } else {
                            refresh(watchers);
                        }
                    }

                    synchronized (needsRefreshLock) {
                        if (!needsRefresh && !stop) {
                            needsRefreshLock.wait();
                        }
                    }
                } catch (InterruptedException e) {
                    return;
                } catch (Throwable t) {
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
            cacheRefresher.needsRefresh(this);
        }
    }

    private class AllCacheWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            cacheRefresher.needsRefreshAll();
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
                try {
                    cacheInvalidationReconnected();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (TypeException e) {
                    // TODO
                }
                readRefreshingEnabledState();
                cacheRefresher.needsRefreshAll();
            }
        }
    }
}
