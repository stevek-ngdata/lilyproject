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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.TypeBucket;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

public class RecordTypesCache {
    private Log log = LogFactory.getLog(getClass());

    // A lock on the monitor needs to be taken when changes are to be made on
    // the nameCache, on the count variable, on the nameCacheOutOfDate boolean
    // or if a bucket monitor needs to be added.
    private final Object monitor = new Object();
    // A lock on a bucket monitor needs to be taken when changes are to be made
    // on a bucket.
    private final Map<String, Object> bucketMonitors = new HashMap<String, Object>();
    // The nameCacheOutOfData should be set to true when an update happens on
    // a bucket. This means that if the nameCache is requested, it should be
    // refreshed first. Once it is refreshed it can be put back to false.
    private volatile boolean nameCacheOutOfDate = false;
    private volatile boolean childRecordTypesOutOfDate = false;
    // The count indicates how many buckets are being updated. As long as the
    // count is higher than 0, the nameCache can not be updated since this could
    // lead to an inconsistent state (two types could get the same name).
    private volatile int count = 0;

    /**
     * name -> record type version -> record type
     */
    private Map<QName, Map<Long, RecordType>> nameCache;

    /**
     * Normally a record type points to the record types from which it extends, i.e. to their parent type.
     * This map allows to traverse the reverse relation: from parent to child.
     */
    private Map<SchemaId, Set<SchemaId>> childRecordTypes;

    /**
     * bucket -> record type id -> record type version -> record type
     */
    private Map<String, Map<SchemaId, Map<Long, RecordType>>> buckets;

    private ConcurrentHashMap<String, Map<SchemaId, Map<Long, RecordType>>> localUpdateBuckets =
            new ConcurrentHashMap<String, Map<SchemaId, Map<Long, RecordType>>>();

    public RecordTypesCache() {
        nameCache = new HashMap<QName, Map<Long, RecordType>>();
        buckets = new ConcurrentHashMap<String, Map<SchemaId, Map<Long, RecordType>>>();
    }

    private Map<QName, Map<Long, RecordType>> getNameCache() throws InterruptedException {
        // First check if the name cache is out of date
        if (nameCacheOutOfDate) {
            synchronized (monitor) {
                // Wait until no buckets are being updated
                while (count > 0) {
                    monitor.wait();
                }
                if (nameCacheOutOfDate) {
                    // Re-initialize the nameCache
                    Map<QName, Map<Long, RecordType>> newNameCache = new HashMap<QName, Map<Long, RecordType>>();
                    for (Map<SchemaId, Map<Long, RecordType>> bucket : buckets.values()) {
                        for (Map<Long, RecordType> recordTypesByVersion : bucket.values()) {
                            for (RecordType recordType : recordTypesByVersion.values()) {
                                if (!newNameCache.containsKey(recordType.getName())) {
                                    newNameCache.put(recordType.getName(), new HashMap<Long, RecordType>());
                                }
                                newNameCache.get(recordType.getName()).put(recordType.getVersion(), recordType);
                            }
                        }
                    }
                    nameCache = newNameCache;
                    nameCacheOutOfDate = false;
                }
            }
        }
        return nameCache;
    }

    private Map<SchemaId, Set<SchemaId>> getChildRecordTypes() throws InterruptedException {
        // First check if the childRecordTypes is out of date
        if (childRecordTypesOutOfDate) {
            synchronized (monitor) {
                // Wait until no buckets are being updated
                while (count > 0) {
                    monitor.wait();
                }
                if (childRecordTypesOutOfDate) {
                    // Re-initialize the childRecordTypes
                    Map<SchemaId, Set<SchemaId>> newChildRecordTypes = new HashMap<SchemaId, Set<SchemaId>>() {
                        @Override
                        public Set<SchemaId> get(Object key) {
                            Set<SchemaId> set = super.get(key);
                            if (set == null) {
                                set = new HashSet<SchemaId>();
                                super.put((SchemaId) key, set);
                            }
                            return set;
                        }
                    };
                    for (Map<SchemaId, Map<Long, RecordType>> bucket : buckets.values()) {
                        for (Map<Long, RecordType> recordTypeByVersion : bucket.values()) {
                            // TODO: we only look at the last record type here, not sure why
                            RecordType lastRecordType = getRecordTypeWithVersion(recordTypeByVersion, null);
                            for (SchemaId parent : lastRecordType.getSupertypes().keySet()) {
                                newChildRecordTypes.get(parent).add(lastRecordType.getId());
                            }
                        }
                    }
                    childRecordTypes = newChildRecordTypes;
                    childRecordTypesOutOfDate = false;
                }
            }
        }
        return childRecordTypes;
    }

    /**
     * Increment the number of buckets being updated
     */
    private void incCount() {
        synchronized (monitor) {
            count++;
            monitor.notify();
        }
    }

    /**
     * Decrement the number of buckets being updated and mark the nameCache out
     * of date.
     */
    private void decCount() {
        synchronized (monitor) {
            count--;
            nameCacheOutOfDate = true;
            childRecordTypesOutOfDate = true;
            monitor.notify();
        }
    }

    /**
     * Return the monitor of a bucket and create it if it does not exist yet.
     */
    private Object getBucketMonitor(String bucketId) {
        Object bucketMonitor = bucketMonitors.get(bucketId);
        if (bucketMonitor == null) {
            // If the bucket does not exist yet we need to create it
            // Take the lock on the monitor to avoid that another call would
            // created it at the same time
            synchronized (monitor) {
                // Make sure it hasn't been created meanwhile (= between
                // checking for null and taking the lock on the monitor)
                bucketMonitor = bucketMonitors.get(bucketId);
                if (bucketMonitor == null) {
                    bucketMonitor = new Object();
                    bucketMonitors.put(bucketId, bucketMonitor);
                }
            }
        }
        return bucketMonitor;
    }

    /**
     * Return all record types in the cache. To avoid inconsistencies between
     * buckets, we get the nameCache first.
     */
    public Collection<RecordType> getRecordTypes() throws InterruptedException {
        List<RecordType> recordTypes = new ArrayList<RecordType>();
        for (Map<Long, RecordType> recordTypesByVersion : getNameCache().values()) {
            for (RecordType recordType : recordTypesByVersion.values()) {
                recordTypes.add(recordType.clone());
            }
        }
        return recordTypes;
    }

    /**
     * Return the record type based on its name
     */
    public RecordType getRecordType(QName name, Long version) throws InterruptedException {
        Map<Long, RecordType> recordTypesByVersion = getNameCache().get(name);
        return getRecordTypeWithVersion(recordTypesByVersion, version);
    }

    public Set<SchemaId> findDirectSubTypes(SchemaId recordTypeId) throws InterruptedException {
        Set<SchemaId> childTypes = getChildRecordTypes().get(recordTypeId);
        return childTypes != null ? childTypes : Collections.<SchemaId>emptySet();
    }

    /**
     * Get the record type based on its id
     */
    public RecordType getRecordType(SchemaId id, Long version) {
        String bucketId = AbstractSchemaCache.encodeHex(id.getBytes());
        Map<SchemaId, Map<Long, RecordType>> bucket = buckets.get(bucketId);
        if (bucket == null) {
            return null;
        }
        Map<Long, RecordType> recordTypesByVersion = bucket.get(id);
        return getRecordTypeWithVersion(recordTypesByVersion, version);
    }

    private RecordType getRecordTypeWithVersion(Map<Long, RecordType> recordTypesByVersion, Long version) {
        if (recordTypesByVersion == null)
            return null;
        else if (version != null)
            return recordTypesByVersion.get(version);
        else
            return recordTypesByVersion.get(new TreeSet<Long>(recordTypesByVersion.keySet()).last());
    }

    /**
     * Refreshes the whole cache to contain the given list of record types.
     */
    public void refreshRecordTypes(List<RecordType> recordTypes) throws InterruptedException {
        // Since we will update all buckets, we take the lock on the monitor for
        // the whole operation
        synchronized (monitor) {
            while (count > 0) {
                monitor.wait();
            }
            nameCacheOutOfDate = true;
            childRecordTypesOutOfDate = true;
            // One would expect that existing buckets need to be cleared first.
            // But since record types cannot be deleted we will just overwrite
            // them.
            for (RecordType recordType : recordTypes) {
                String bucketId = AbstractSchemaCache.encodeHex(recordType.getId().getBytes());
                // Only update if it was not updated locally
                // If it was updated locally either this is the refresh of that
                // update,
                // or the refresh for this update will follow.
                if (!removeFromLocalUpdateBucket(recordType, bucketId)) {
                    Map<SchemaId, Map<Long, RecordType>> bucket = buckets.get(bucketId);
                    if (bucket == null) {
                        bucket = new ConcurrentHashMap<SchemaId, Map<Long, RecordType>>();
                        buckets.put(bucketId, bucket);
                    }
                    putRecordTypeInBucket(bucket, recordType);
                }
            }
        }
    }

    /**
     * Refresh one bucket with the record types contained in the TypeBucket
     */
    public void refreshRecordTypeBucket(TypeBucket typeBucket) {
        String bucketId = typeBucket.getBucketId();

        // First increment the number of buckets that are being updated
        incCount();
        // Get a lock on the bucket to be updated
        synchronized (getBucketMonitor(bucketId)) {
            List<RecordType> recordTypes = typeBucket.getRecordTypes();
            Map<SchemaId, Map<Long, RecordType>> bucket = buckets.get(bucketId);
            // One would expect that an existing bucket need to be cleared
            // first.
            // But since record types cannot be deleted we will just overwrite
            // them.
            if (bucket == null) {
                bucket = new ConcurrentHashMap<SchemaId, Map<Long, RecordType>>(Math.min(recordTypes.size(), 8), .75f, 1);
                buckets.put(bucketId, bucket);
            }
            // Fill the bucket with the new record types
            for (RecordType recordType : recordTypes) {
                if (!removeFromLocalUpdateBucket(recordType, bucketId)) {
                    putRecordTypeInBucket(bucket, recordType);
                }
            }
        }
        // Decrement the number of buckets that are being updated again.
        decCount();
    }

    private void putRecordTypeInBucket(Map<SchemaId, Map<Long, RecordType>> bucket, RecordType recordType) {
        if (!bucket.containsKey(recordType.getId())) {
            bucket.put(recordType.getId(), new HashMap<Long, RecordType>());
        }
        bucket.get(recordType.getId()).put(recordType.getVersion(), recordType);
    }

    /**
     * Update the cache to contain the new recordType
     */
    public void update(RecordType recordType) {
        // Clone the RecordType to avoid changes to it while it is in the cache
        RecordType rtToCache = recordType.clone();
        SchemaId id = rtToCache.getId();
        String bucketId = AbstractSchemaCache.encodeHex(id.getBytes());
        // First increment the number of buckets that are being updated
        incCount();
        // Get a lock on the bucket to be updated
        synchronized (getBucketMonitor(bucketId)) {
            Map<SchemaId, Map<Long, RecordType>> bucket = buckets.get(bucketId);
            // If the bucket does not exist yet, create it
            if (bucket == null) {
                bucket = new ConcurrentHashMap<SchemaId, Map<Long, RecordType>>(8, .75f, 1);
                buckets.put(bucketId, bucket);
            }
            putRecordTypeInBucket(bucket, rtToCache);
            // Mark that this recordType is updated locally
            // and that the next refresh can be ignored
            // since this refresh can contain an old recordType
            addToLocalUpdateBucket(recordType, bucketId);
        }
        // Decrement the number of buckets that are being updated again.
        decCount();
    }

    // Add the id of a record type that has been updated locally
    // in a bucket. This record type will be skipped in a next
    // cache refresh sequence.
    // This avoids that a locally updated record type will be
    // overwritten by old data by a cache refresh.
    private void addToLocalUpdateBucket(RecordType recordType, String bucketId) {
        Map<SchemaId, Map<Long, RecordType>> localUpdateBucket = localUpdateBuckets.get(bucketId);
        if (localUpdateBucket == null) {
            localUpdateBucket = new HashMap<SchemaId, Map<Long, RecordType>>();
            localUpdateBuckets.put(bucketId, localUpdateBucket);
        }
        putRecordTypeInBucket(localUpdateBucket, recordType);
    }

    // Check if the record type is present in the local update bucket.
    // If so, return true and remove it, in which case the refresh
    // should skip it to avoid replacing the record type with old data.
    private boolean removeFromLocalUpdateBucket(RecordType recordType, String bucketId) {
        Map<SchemaId, Map<Long, RecordType>> localUpdateBucket = localUpdateBuckets.get(bucketId);
        if (localUpdateBucket == null) {
            return false;
        }
        Map<Long, RecordType> recordTypesByVersion = localUpdateBucket.get(recordType.getId());
        if (recordTypesByVersion == null) {
            return false;
        }

        RecordType localRt = recordTypesByVersion.remove(recordType.getVersion());
        return localRt != null;
    }

    public void clear() {
        nameCache.clear();

        for (Map bucket : buckets.values()) {
            bucket.clear();
        }

        for (Map<SchemaId, Map<Long, RecordType>> bucket : localUpdateBuckets.values()) {
            bucket.clear();
        }
    }

}
