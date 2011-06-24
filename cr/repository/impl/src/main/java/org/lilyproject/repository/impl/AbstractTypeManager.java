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
package org.lilyproject.repository.impl;

import java.io.IOException;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.lilyproject.repository.api.*;
import org.lilyproject.repository.impl.primitivevaluetype.*;
import org.lilyproject.util.ArgumentValidator;
import org.lilyproject.util.Logs;
import org.lilyproject.util.zookeeper.ZkUtil;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

import javax.annotation.PreDestroy;

public abstract class AbstractTypeManager implements TypeManager {
    protected Log log;

    protected CacheRefresher cacheRefresher = new CacheRefresher();
    
    protected Map<String, PrimitiveValueType> primitiveValueTypes = new HashMap<String, PrimitiveValueType>();
    protected IdGenerator idGenerator;
    
    //
    // Caching
    //
    protected ZooKeeperItf zooKeeper;
    private FieldTypesImpl fieldTypes;
    private FieldTypesImpl updatingFieldTypes = new FieldTypesImpl();
    private boolean updatedFieldTypes = false;
    private Map<QName, RecordType> recordTypeNameCache = new HashMap<QName, RecordType>();
    private Map<SchemaId, RecordType> recordTypeIdCache = new HashMap<SchemaId, RecordType>();
    private final CacheWatcher cacheWatcher = new CacheWatcher();
    protected static final String CACHE_INVALIDATION_PATH = "/lily/typemanager/cache";
    
    public AbstractTypeManager(ZooKeeperItf zooKeeper) {
        this.zooKeeper = zooKeeper;
        cacheRefresher.start();
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
    
    private class CacheWatcher implements Watcher {
        public void process(WatchedEvent event) {
            cacheRefresher.needsRefresh();
        }
    }

    protected class ConnectionWatcher implements Watcher {
        public void process(WatchedEvent event) {
            if (EventType.None.equals(event.getType()) && KeeperState.SyncConnected.equals(event.getState())) {
                try {
                    cacheInvalidationReconnected();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                cacheRefresher.needsRefresh();
            }
        }
    }

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

        public void run() {
            while (!stop && !Thread.interrupted()) {
                try {
                    if (needsRefresh) {
                        synchronized (needsRefreshLock) {
                            needsRefresh = false;
                        }
                        refreshCaches();
                    }

                    synchronized (needsRefreshLock) {
                        if (!needsRefresh && !stop) {
                            needsRefreshLock.wait();
                        }
                    }
                } catch (InterruptedException e) {
                    return;
                } catch (Throwable t) {
                    log.error("Error refreshing type manager cache.", t);
                }
            }
        }
    }

    protected void cacheInvalidationReconnected() throws InterruptedException {
    }

    protected void setupCaches() throws InterruptedException, KeeperException {
        ZkUtil.createPath(zooKeeper, CACHE_INVALIDATION_PATH);
        zooKeeper.addDefaultWatcher(new ConnectionWatcher());
        refreshCaches();
    }

    private synchronized void refreshCaches() throws InterruptedException {
        try {
            zooKeeper.getData(CACHE_INVALIDATION_PATH, cacheWatcher, null);
        } catch (KeeperException e) {
            // Failed to put our watcher.
            // Relying on the ConnectionWatcher to put it again and initialize
            // the caches.
        }
        try {
            updatingFieldTypes.refresh(getFieldTypesWithoutCache());
            updatedFieldTypes = true;
        } catch (Exception e) {
            // We keep on working with the old cache
            log.warn("Exception while refreshing RecordType cache. Cache is possibly out of date.", e);
        } 
        refreshRecordTypeCache();
    }

    private synchronized void refreshRecordTypeCache() {
        Map<QName, RecordType> newRecordTypeNameCache = new HashMap<QName, RecordType>();
        Map<SchemaId, RecordType> newRecordTypeIdCache = new HashMap<SchemaId, RecordType>();
        try {
            List<RecordType> recordTypes = getRecordTypesWithoutCache();
            for (RecordType recordType : recordTypes) {
                newRecordTypeNameCache.put(recordType.getName(), recordType);
                newRecordTypeIdCache.put(recordType.getId(), recordType);
            }
            recordTypeNameCache = newRecordTypeNameCache;
            recordTypeIdCache = newRecordTypeIdCache;
        } catch (Exception e) {
            // We keep on working with the old cache
            log.warn("Exception while refreshing RecordType cache. Cache is possibly out of date.", e);
        } 
    }

    public synchronized FieldTypesImpl getFieldTypesSnapshot() {
        if (updatedFieldTypes) {
            fieldTypes = updatingFieldTypes.clone();
            updatedFieldTypes = false;
        }
        return fieldTypes;
    }
    
    abstract public List<FieldType> getFieldTypesWithoutCache() throws RepositoryException, InterruptedException;
    abstract public List<RecordType> getRecordTypesWithoutCache() throws RepositoryException, InterruptedException;
    
    protected synchronized void updateFieldTypeCache(FieldType fieldType) {
        updatingFieldTypes.update(fieldType);
        updatedFieldTypes = true;
    }
    
    protected synchronized void updateRecordTypeCache(RecordType recordType) {
        RecordType oldType = getRecordTypeFromCache(recordType.getId());
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
    
    public synchronized List<FieldType> getFieldTypes() {
        return getFieldTypesSnapshot().getFieldTypes();
    }

    
    
    protected synchronized RecordType getRecordTypeFromCache(QName name) {
        return recordTypeNameCache.get(name);
    }

    protected synchronized RecordType getRecordTypeFromCache(SchemaId id) {
        return recordTypeIdCache.get(id);
    }
    
    public RecordType getRecordTypeById(SchemaId id, Long version) throws RecordTypeNotFoundException, TypeException, RepositoryException, InterruptedException {
        ArgumentValidator.notNull(id, "id");
        RecordType recordType = getRecordTypeFromCache(id);
        if (recordType == null) {
            throw new RecordTypeNotFoundException(id, version);
        }
        // The cache only keeps the latest (known) RecordType
        if (version != null && !version.equals(recordType.getVersion())) {
            recordType = getRecordTypeByIdWithoutCache(id, version);
        }
        if (recordType == null) {
            throw new RecordTypeNotFoundException(id, version);
        }
        return recordType.clone();
    }
    
    public RecordType getRecordTypeByName(QName name, Long version) throws RecordTypeNotFoundException, TypeException, RepositoryException, InterruptedException {
        ArgumentValidator.notNull(name, "name");
        RecordType recordType = getRecordTypeFromCache(name);
        if (recordType == null) {
            throw new RecordTypeNotFoundException(name, version);
        }
        // The cache only keeps the latest (known) RecordType
        if (version != null && !version.equals(recordType.getVersion())) {
            recordType = getRecordTypeByIdWithoutCache(recordType.getId(), version);
        }
        if (recordType == null) {
            throw new RecordTypeNotFoundException(name, version);
        }
        return recordType.clone();
    }
    
    abstract protected RecordType getRecordTypeByIdWithoutCache(SchemaId id, Long version) throws RepositoryException, InterruptedException;
    
    public FieldType getFieldTypeById(SchemaId id) throws FieldTypeNotFoundException {
        return getFieldTypesSnapshot().getFieldTypeById(id);
    }
    
    public FieldType getFieldTypeByName(QName name) throws FieldTypeNotFoundException {
        return getFieldTypesSnapshot().getFieldTypeByName(name);
    }
    
    //
    // Object creation methods
    //
    public RecordType newRecordType(QName name) {
        return new RecordTypeImpl(null, name);
    }
    
    public RecordType newRecordType(SchemaId recordTypeId, QName name) {
        ArgumentValidator.notNull(name, "name");
        return new RecordTypeImpl(recordTypeId, name);
    }

    public FieldType newFieldType(ValueType valueType, QName name, Scope scope) {
        return newFieldType(null, valueType, name, scope);
    }

    public FieldTypeEntry newFieldTypeEntry(SchemaId fieldTypeId, boolean mandatory) {
        ArgumentValidator.notNull(fieldTypeId, "fieldTypeId");
        ArgumentValidator.notNull(mandatory, "mandatory");
        return new FieldTypeEntryImpl(fieldTypeId, mandatory);
    }


    public FieldType newFieldType(SchemaId id, ValueType valueType, QName name,
            Scope scope) {
                ArgumentValidator.notNull(valueType, "valueType");
                ArgumentValidator.notNull(name, "name");
                ArgumentValidator.notNull(scope, "scope");
                return new FieldTypeImpl(id, valueType, name, scope);
            }

    //
    // Primitive value types
    //
    public void registerPrimitiveValueType(PrimitiveValueType primitiveValueType) {
        primitiveValueTypes.put(primitiveValueType.getName(), primitiveValueType);
    }

    public ValueType getValueType(String primitiveValueTypeName, boolean multivalue, boolean hierarchy) {
        PrimitiveValueType type = primitiveValueTypes.get(primitiveValueTypeName);
        if (type == null) {
            throw new IllegalArgumentException("Primitive value type does not exist: " + primitiveValueTypeName);
        }
        return new ValueTypeImpl(type, multivalue, hierarchy);
    }

    @Override
    public ValueType getValueType(String primitiveValueTypeName) throws RepositoryException, InterruptedException {
        return getValueType(primitiveValueTypeName, false, false);
    }

    // TODO get this from some configuration file
    protected void registerDefaultValueTypes() {
        //
        // Important:
        //
        // When adding a type below, please update the list of built-in
        // types in the javadoc of the method TypeManager.getValueType.
        //

        registerPrimitiveValueType(new StringValueType());
        registerPrimitiveValueType(new IntegerValueType());
        registerPrimitiveValueType(new LongValueType());
        registerPrimitiveValueType(new DoubleValueType());
        registerPrimitiveValueType(new DecimalValueType());
        registerPrimitiveValueType(new BooleanValueType());
        registerPrimitiveValueType(new DateValueType());
        registerPrimitiveValueType(new DateTimeValueType());
        registerPrimitiveValueType(new LinkValueType(idGenerator));
        registerPrimitiveValueType(new BlobValueType());
        registerPrimitiveValueType(new UriValueType());
    }
}
