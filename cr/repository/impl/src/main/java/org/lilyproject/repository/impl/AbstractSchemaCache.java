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

import org.apache.commons.logging.Log;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.lilyproject.repository.api.*;
import org.lilyproject.util.Logs;
import org.lilyproject.util.zookeeper.ZkUtil;
import org.lilyproject.util.zookeeper.ZooKeeperItf;


public abstract class AbstractSchemaCache implements SchemaCache {

    protected ZooKeeperItf zooKeeper;
    protected Log log;

    private FieldTypesImpl fieldTypes;
    private Map<QName, RecordType> recordTypeNameCache = new HashMap<QName, RecordType>();
    private Map<SchemaId, RecordType> recordTypeIdCache = new HashMap<SchemaId, RecordType>();
    private FieldTypesImpl updatingFieldTypes = new FieldTypesImpl();
    private boolean updatedFieldTypes = false;

    private final CacheWatcher cacheWatcher = new CacheWatcher();
    private final CacheRefresher cacheRefresher = new CacheRefresher();

    protected static final String CACHE_INVALIDATION_PATH = "/lily/typemanager/cache/invalidate";
    protected static final String CACHE_REFRESHENABLED_PATH = "/lily/typemanager/cache/enabled";

    public AbstractSchemaCache(ZooKeeperItf zooKeeper) {
        this.zooKeeper = zooKeeper;
        cacheRefresher.start();
    }

    public void start() throws InterruptedException, KeeperException, RepositoryException {
        ZkUtil.createPath(zooKeeper, CACHE_INVALIDATION_PATH);
        ZkUtil.createPath(zooKeeper, CACHE_REFRESHENABLED_PATH);
        zooKeeper.addDefaultWatcher(new ConnectionWatcher());
        readRefreshingEnabledState();
        refresh();
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
        RecordType oldType = getRecordType(recordType.getId());
        if (oldType != null) {
            recordTypeNameCache.remove(oldType.getName());
            recordTypeIdCache.remove(oldType.getId());
        }
        recordTypeNameCache.put(recordType.getName(), recordType);
        recordTypeIdCache.put(recordType.getId(), recordType);
    }

    public synchronized Collection<RecordType> getRecordTypes() {
        List<RecordType> recordTypes = new ArrayList<RecordType>();
        for (RecordType recordType : recordTypeNameCache.values()) {
            recordTypes.add(recordType.clone());
        }
        return recordTypes;
    }

    public synchronized RecordType getRecordType(QName name) {
        return recordTypeNameCache.get(name);
    }

    public synchronized RecordType getRecordType(SchemaId id) {
        return recordTypeIdCache.get(id);
    }

    public synchronized FieldType getFieldType(QName name) throws FieldTypeNotFoundException {
        return getFieldTypesSnapshot().getFieldType(name);
    }

    public synchronized FieldType getFieldType(SchemaId id) throws FieldTypeNotFoundException {
        return getFieldTypesSnapshot().getFieldType(id);
    }

    public synchronized List<FieldType> getFieldTypes() {
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
    private synchronized void refresh() throws InterruptedException, RepositoryException {
        try {
            zooKeeper.getData(CACHE_INVALIDATION_PATH, cacheWatcher, null);
        } catch (KeeperException e) {
            // Failed to put our watcher.
            // Relying on the ConnectionWatcher to put it again and initialize
            // the caches.
        }
        refreshFieldTypeCache();
        refreshRecordTypeCache();
    }

    private void refreshFieldTypeCache() throws RepositoryException, InterruptedException {
        updatingFieldTypes.refresh(getTypeManager().getFieldTypesWithoutCache());
        updatedFieldTypes = true;
    }

    private synchronized void refreshRecordTypeCache() throws RepositoryException, InterruptedException {
        Map<QName, RecordType> newRecordTypeNameCache = new HashMap<QName, RecordType>();
        Map<SchemaId, RecordType> newRecordTypeIdCache = new HashMap<SchemaId, RecordType>();
        List<RecordType> recordTypes = getTypeManager().getRecordTypesWithoutCache();
        for (RecordType recordType : recordTypes) {
            newRecordTypeNameCache.put(recordType.getName(), recordType);
            newRecordTypeIdCache.put(recordType.getId(), recordType);
        }
        recordTypeNameCache = newRecordTypeNameCache;
        recordTypeIdCache = newRecordTypeIdCache;
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
        private volatile boolean stop;
        private final Object needsRefreshLock = new Object();
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

        public void needsRefresh() {
            synchronized (needsRefreshLock) {
                needsRefresh = true;
                needsRefreshLock.notifyAll();
            }
        }

        @Override
        public void run() {
            while (!stop && !Thread.interrupted()) {
                try {
                    if (needsRefresh) {
                        synchronized (needsRefreshLock) {
                            needsRefresh = false;
                        }
                        refresh();
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
        @Override
        public void process(WatchedEvent event) {
            cacheRefresher.needsRefresh();
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
                cacheRefresher.needsRefresh();
            }
        }
    }

}
