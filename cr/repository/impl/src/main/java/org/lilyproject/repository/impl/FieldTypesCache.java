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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.FieldTypes;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.TypeBucket;

public class FieldTypesCache extends FieldTypesImpl implements FieldTypes {
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

    private ConcurrentHashMap<String, Set<SchemaId>> localUpdateBuckets =
            new ConcurrentHashMap<String, Set<SchemaId>>();

    private Log log = LogFactory.getLog(getClass());

    public FieldTypesCache() {
        super();
        for (String bucketId : buckets.keySet()) {
            bucketMonitors.put(bucketId, new Object());
        }
    }

    /**
     * When accessing the nameCache, this method should always be used instead
     * of using the variable immediately
     */
    protected Map<QName, FieldType> getNameCache() throws InterruptedException {
        // First check if the name cache is out of date
        if (nameCacheOutOfDate) {
            synchronized (monitor) {
                // Wait until no buckets are being updated
                while (count > 0) {
                    monitor.wait();
                }
                if (nameCacheOutOfDate) {
                    // Re-initialize the nameCache
                    Map<QName, FieldType> newNameCache = new HashMap<QName, FieldType>();
                    for (Map<SchemaId, FieldType> bucket : buckets.values()) {
                        for (FieldType fieldType : bucket.values())
                            newNameCache.put(fieldType.getName(), fieldType);
                    }
                    nameCache = newNameCache;
                    nameCacheOutOfDate = false;
                }
            }
        }
        return nameCache;
    }

    /**
     * Increment the number of buckets being updated.
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
     * Take a snapshot of the cache and return it. This snapshot cannot be
     * updated.
     *
     * @return the FieldTypes snapshot
     * @throws InterruptedException
     */
    public FieldTypes getSnapshot() throws InterruptedException {
        synchronized (monitor) {
            while (count > 0) {
                monitor.wait();
            }
            FieldTypesImpl newFieldTypes = new FieldTypesImpl();
            for (Entry<String, Map<SchemaId, FieldType>> bucketEntry : buckets.entrySet()) {
                Map<SchemaId, FieldType> fieldTypeIdBucket = new HashMap<SchemaId, FieldType>();
                fieldTypeIdBucket.putAll(bucketEntry.getValue());
                newFieldTypes.buckets.put(bucketEntry.getKey(), fieldTypeIdBucket);
                for (FieldType fieldType : bucketEntry.getValue().values()) {
                    newFieldTypes.nameCache.put(fieldType.getName(), fieldType);
                }
            }
            return newFieldTypes;
        }
    }

    /**
     * Refreshes the whole cache to contain the given list of field types.
     *
     * @param fieldTypes
     * @throws InterruptedException
     */
    public void refreshFieldTypes(List<FieldType> fieldTypes) throws InterruptedException {
        // Since we will update all buckets, we take the lock on the monitor for
        // the whole operation
        synchronized (monitor) {
            while (count > 0) {
                monitor.wait();
            }
            nameCacheOutOfDate = true;
            // One would expect that existing buckets need to be cleared first.
            // But since field types cannot be deleted we will just overwrite
            // them.
            for (FieldType fieldType : fieldTypes) {
                String bucketId = AbstractSchemaCache.encodeHex(fieldType.getId().getBytes());
                // Only update if it was not updated locally
                // If it was updated locally either this is the refresh of that
                // update,
                // or the refresh for this update will follow.
                if (!removeFromLocalUpdateBucket(fieldType.getId(), bucketId)) {
                    Map<SchemaId, FieldType> bucket = buckets.get(bucketId);
                    if (bucket == null) {
                        bucket = new ConcurrentHashMap<SchemaId, FieldType>(8, .75f, 1);
                        buckets.put(bucketId, bucket);
                    }
                    bucket.put(fieldType.getId(), fieldType);
                }
            }
        }
    }

    /**
     * Refresh one bucket with the field types contained in the TypeBucket
     *
     * @param typeBucket
     */
    public void refreshFieldTypeBucket(TypeBucket typeBucket) {
        String bucketId = typeBucket.getBucketId();

        // First increment the number of buckets that are being updated
        incCount();
        // Get a lock on the bucket to be updated
        synchronized (getBucketMonitor(bucketId)) {
            List<FieldType> fieldTypes = typeBucket.getFieldTypes();
            Map<SchemaId, FieldType> bucket = buckets.get(bucketId);
            // One would expect that an existing bucket need to be cleared
            // first.
            // But since field types cannot be deleted we will just overwrite
            // them.
            if (bucket == null) {
                bucket = new ConcurrentHashMap<SchemaId, FieldType>(Math.min(fieldTypes.size(), 8), .75f, 1);
                buckets.put(bucketId, bucket);
            }
            // Fill the bucket with the new field types
            for (FieldType fieldType : fieldTypes) {
                if (!removeFromLocalUpdateBucket(fieldType.getId(), bucketId)) {
                    bucket.put(fieldType.getId(), fieldType);
                }
            }
        }
        // Decrement the number of buckets that are being updated again.
        decCount();
    }

    /**
     * Update the cache to contain the new fieldType
     *
     * @param fieldType
     */
    public void update(FieldType fieldType) {
        SchemaId id = fieldType.getId();
        String bucketId = AbstractSchemaCache.encodeHex(id.getBytes());
        // First increment the number of buckets that are being updated
        incCount();
        // Get a lock on the bucket to be updated
        synchronized (getBucketMonitor(bucketId)) {
            Map<SchemaId, FieldType> bucket = buckets.get(bucketId);
            // If the bucket does not exist yet, create it
            if (bucket == null) {
                bucket = new ConcurrentHashMap<SchemaId, FieldType>(8, .75f, 1);
                buckets.put(bucketId, bucket);
            }
            bucket.put(id, fieldType);
            // Mark that this fieldType is updated locally
            // and that the next refresh can be ignored
            // since this refresh can contain an old fieldType
            addToLocalUpdateBucket(id, bucketId);
        }
        // Decrement the number of buckets that are being updated again.
        decCount();
    }

    // Add the id of a field type that has been updated locally
    // in a bucket. This field type will be skipped in a next
    // cache refresh sequence.
    // This avoids that a locally updated field type will be
    // overwritten by old data by a cache refresh.
    private void addToLocalUpdateBucket(SchemaId id, String bucketId) {
        Set<SchemaId> localUpdateBucket = localUpdateBuckets.get(bucketId);
        if (localUpdateBucket == null) {
            localUpdateBucket = new HashSet<SchemaId>();
            localUpdateBuckets.put(bucketId, localUpdateBucket);
        }
        localUpdateBucket.add(id);
    }

    // Check if the field type is present in the local update bucket.
    // If so, return true and remove it, in which case the refresh
    // should skip it to avoid replacing the field type with old data.
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
