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

import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.lilyproject.repository.api.*;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

public class LocalSchemaCache extends AbstractSchemaCache implements SchemaCache {
    // Indicates if the cache refresh flag should be updated
    // on zookeeper when an update on the schema happens
    private boolean cacheRefreshingEnabled = true;
    private final TypeManager typeManager;

    public LocalSchemaCache(ZooKeeperItf zooKeeper, TypeManager typeManager) {
        super(zooKeeper);
        this.typeManager = typeManager;
        log = LogFactory.getLog(getClass());
    }

    protected TypeManager getTypeManager() {
        return typeManager;
    }

    public synchronized void updateFieldType(FieldType fieldType) throws TypeException, InterruptedException {
        super.updateFieldType(fieldType);
        triggerRefresh(false);
    }

    public synchronized void updateRecordType(RecordType recordType) throws TypeException, InterruptedException {
        super.updateRecordType(recordType);
        triggerRefresh(false);
    }

    @Override
    protected void cacheInvalidationReconnected() throws TypeException, InterruptedException {
        // A previous notify might have failed because of a disconnection
        // To be sure we try to send a notify again
        triggerRefresh(false);
    }

    /**
     * Sets the cache refresh flag on Zookeeper. This triggers the caches to
     * refresh their data.
     * 
     * @param force
     *            if true, it is ignored if cache refreshing is enabled or not.
     */
    public void triggerRefresh(boolean force) throws TypeException, InterruptedException {
        if (force || cacheRefreshingEnabled) {
            try {
                zooKeeper.setData(CACHE_INVALIDATION_PATH, null, -1);
            } catch (KeeperException e) {
                throw new TypeException("Exception while triggering cache refresh", e);
            }
        }
    }

    //
    //
    // Enable / disable cache refreshing
    //
    //

    /**
     * Cache refreshing enabled watcher monitors the state of the enabled
     * boolean on Zookeeper.
     */
    private class CacheRefreshingEnabledWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            readRefreshingEnabledState();
        }
    }

    /**
     * Reads the refreshing enabled state on zookeeper and puts a new watch
     */
    protected void readRefreshingEnabledState() {
        byte[] data;
        try {
            data = zooKeeper.getData(CACHE_REFRESHENABLED_PATH, new CacheRefreshingEnabledWatcher(), new Stat());
            if (data == null || data.length == 0 || data[0] == (byte) 1) {
                cacheRefreshingEnabled = true;
            } else {
                cacheRefreshingEnabled = false;
            }
        } catch (KeeperException e) {
            // Rely on connectionwatcher to put watcher again
        } catch (InterruptedException e) {
            // Stop processing
        }
    }

    public void enableSchemaCacheRefresh() throws TypeException, InterruptedException {
        try {
            zooKeeper.setData(CACHE_REFRESHENABLED_PATH, new byte[] { (byte) 1 }, -1);
            cacheRefreshingEnabled = true;
            triggerRefresh();
        } catch (KeeperException e) {
            throw new TypeException("Enabling cache refreshing failed", e);
        }
    }

    public void disableSchemaCacheRefresh() throws TypeException, InterruptedException {
        try {
            zooKeeper.setData(CACHE_REFRESHENABLED_PATH, new byte[] { (byte) 0 }, -1);
            cacheRefreshingEnabled = false;
        } catch (KeeperException e) {
            throw new TypeException("Enabling schema cache refreshing failed", e);
        }
    }

    public void triggerRefresh() throws TypeException, InterruptedException {
        triggerRefresh(true);
    }

    public boolean isRefreshEnabled() {
        return cacheRefreshingEnabled;
    }
}
