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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.TypeBucket;

public class RecordTypesCache {
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
    // The count indicates how many buckets are being updated. As long as the
    // count is higher than 0, the nameCache can not be updated since this could
    // lead to an inconsistent state (two types could get the same name).
    private volatile int count = 0;

    private Map<QName, RecordType> nameCache;
    private Map<String, Map<SchemaId, RecordType>> buckets;
    private ConcurrentHashMap<String, Set<SchemaId>> localUpdateBuckets = new ConcurrentHashMap<String, Set<SchemaId>>();

    public RecordTypesCache() {
        nameCache = new HashMap<QName, RecordType>();
        buckets = new ConcurrentHashMap<String, Map<SchemaId, RecordType>>();
    }

    private Map<QName, RecordType> getNameCache() throws InterruptedException {
        // First check if the name cache is out of date
        if (nameCacheOutOfDate) {
            synchronized (monitor) {
                // Wait until no buckets are being updated
                while (count > 0) {
                    monitor.wait();
                }
                if (nameCacheOutOfDate) {
                    // Re-initialize the nameCache
                    Map<QName, RecordType> newNameCache = new HashMap<QName, RecordType>();
                    for (Map<SchemaId, RecordType> bucket : buckets.values()) {
                        for (RecordType recordType : bucket.values()) {
                            newNameCache.put(recordType.getName(), recordType);
                        }
                    }
                    nameCache = newNameCache;
                    nameCacheOutOfDate = false;
                }
            }
        }
        return nameCache;
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
            monitor.notify();
        }
    }

    /**
     * Return the monitor of a bucket and create it if it does not exist yet.
     *
     * @param bucketId
     * @return
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
     *
     * @return
     * @throws InterruptedException
     */
    public Collection<RecordType> getRecordTypes() throws InterruptedException {
        List<RecordType> recordTypes = new ArrayList<RecordType>();
        for (RecordType recordType : getNameCache().values()) {
            recordTypes.add(recordType.clone());
        }
        return recordTypes;
    }

    /**
     * Return the record type based on its name
     *
     * @param name
     * @return
     * @throws InterruptedException
     */
    public RecordType getRecordType(QName name) throws InterruptedException {
        return getNameCache().get(name);
    }

    /**
     * Get the record type based on its id
     *
     * @param id
     * @return
     */
    public RecordType getRecordType(SchemaId id) {
        String bucketId = AbstractSchemaCache.encodeHex(id.getBytes());
        Map<SchemaId, RecordType> bucket = buckets.get(bucketId);
        if (bucket == null) {
            return null;
        }
        return bucket.get(id);
    }

    /**
     * Refreshes the whole cache to contain the given list of record types.
     *
     * @param recordTypes
     * @throws InterruptedException
     */
    public void refreshRecordTypes(List<RecordType> recordTypes) throws InterruptedException {
        // Since we will update all buckets, we take the lock on the monitor for
        // the whole operation
        synchronized (monitor) {
            while (count > 0) {
                monitor.wait();
            }
            nameCacheOutOfDate = true;
            // One would expect that existing buckets need to be cleared first.
            // But since record types cannot be deleted we will just overwrite
            // them.
            for (RecordType recordType : recordTypes) {
                String bucketId = AbstractSchemaCache.encodeHex(recordType.getId().getBytes());
                // Only update if it was not updated locally
                // If it was updated locally either this is the refresh of that
                // update,
                // or the refresh for this update will follow.
                if (!removeFromLocalUpdateBucket(recordType.getId(), bucketId)) {
                    Map<SchemaId, RecordType> bucket = buckets.get(bucketId);
                    if (bucket == null) {
                        bucket = new ConcurrentHashMap<SchemaId, RecordType>();
                        buckets.put(bucketId, bucket);
                    }
                    bucket.put(recordType.getId(), recordType);
                }
            }
        }
    }

    /**
     * Refresh one bucket with the record types contained in the TypeBucket
     *
     * @param typeBucket
     */
    public void refreshRecordTypeBucket(TypeBucket typeBucket) {
        String bucketId = typeBucket.getBucketId();

        // First increment the number of buckets that are being updated
        incCount();
        // Get a lock on the bucket to be updated
        synchronized (getBucketMonitor(bucketId)) {
            List<RecordType> recordTypes = typeBucket.getRecordTypes();
            Map<SchemaId, RecordType> bucket = buckets.get(bucketId);
            // One would expect that an existing bucket need to be cleared
            // first.
            // But since record types cannot be deleted we will just overwrite
            // them.
            if (bucket == null) {
                bucket = new ConcurrentHashMap<SchemaId, RecordType>(Math.min(recordTypes.size(), 8), .75f, 1);
                buckets.put(bucketId, bucket);
            }
            // Fill the bucket with the new record types
            for (RecordType recordType : recordTypes) {
                if (!removeFromLocalUpdateBucket(recordType.getId(), bucketId)) {
                    bucket.put(recordType.getId(), recordType);
                }
            }
        }
        // Decrement the number of buckets that are being updated again.
        decCount();
    }

    /**
     * Update the cache to contain the new recordType
     *
     * @param recordType
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
            Map<SchemaId, RecordType> bucket = buckets.get(bucketId);
            // If the bucket does not exist yet, create it
            if (bucket == null) {
                bucket = new ConcurrentHashMap<SchemaId, RecordType>(8, .75f, 1);
                buckets.put(bucketId, bucket);
            }
            bucket.put(id, rtToCache);
            // Mark that this recordType is updated locally
            // and that the next refresh can be ignored
            // since this refresh can contain an old recordType
            addToLocalUpdateBucket(id, bucketId);
        }
        // Decrement the number of buckets that are being updated again.
        decCount();
    }

    // Add the id of a record type that has been updated locally
    // in a bucket. This record type will be skipped in a next
    // cache refresh sequence.
    // This avoids that a locally updated record type will be
    // overwritten by old data by a cache refresh.
    private void addToLocalUpdateBucket(SchemaId id, String bucketId) {
        Set<SchemaId> localUpdateBucket = localUpdateBuckets.get(bucketId);
        if (localUpdateBucket == null) {
            localUpdateBucket = new HashSet<SchemaId>();
            localUpdateBuckets.put(bucketId, localUpdateBucket);
        }
        localUpdateBucket.add(id);
    }

    // Check if the record type is present in the local update bucket.
    // If so, return true and remove it, in which case the refresh
    // should skip it to avoid replacing the record type with old data.
    private boolean removeFromLocalUpdateBucket(SchemaId id, String bucketId) {
        Set<SchemaId> localUpdateBucket = localUpdateBuckets.get(bucketId);
        if (localUpdateBucket == null) {
            return false;
        }
        return localUpdateBucket.remove(id);
    }

    public void clear() {
        nameCache.clear();

        for (Map bucket : buckets.values()) {
            bucket.clear();
        }

        for (Set<SchemaId> bucket : localUpdateBuckets.values()) {
            bucket.clear();
        }
    }

}
