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

import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.repository.api.*;

public class RecordTypesImpl {
    private Log log = LogFactory.getLog(getClass());

    private Map<QName, RecordType> nameCache;
    private Map<String, Map<SchemaId, RecordType>> buckets;

    public RecordTypesImpl() {
        nameCache = new HashMap<QName, RecordType>();
        buckets = new HashMap<String, Map<SchemaId, RecordType>>();
    }

    public synchronized Collection<RecordType> getRecordTypes() {
        List<RecordType> recordTypes = new ArrayList<RecordType>();
        for (RecordType recordType : nameCache.values()) {
            recordTypes.add(recordType.clone());
        }
        return recordTypes;
    }

    public synchronized RecordType getRecordType(QName name) {
        return nameCache.get(name);
    }

    public synchronized RecordType getRecordType(SchemaId id) {
        String bucketId = AbstractSchemaCache.encodeHex(id.getBytes());
        Map<SchemaId, RecordType> bucket = buckets.get(bucketId);
        if (bucket == null)
            return null;
        return bucket.get(id);
    }

    public synchronized void refreshRecordTypes(List<RecordType> recordTypes) {
        nameCache = new HashMap<QName, RecordType>(recordTypes.size());
        buckets = new HashMap<String, Map<SchemaId, RecordType>>();
        for (RecordType recordType : recordTypes) {
            nameCache.put(recordType.getName(), recordType);
            String bucketId = AbstractSchemaCache.encodeHex(recordType.getId().getBytes());
            Map<SchemaId, RecordType> bucket = buckets.get(bucketId);
            if (bucket == null) {
                bucket = new HashMap<SchemaId, RecordType>();
                buckets.put(bucketId, bucket);
            }
            bucket.put(recordType.getId(), recordType);
        }
    }

    public synchronized void refreshRecordTypeBuckets(List<TypeBucket> typeBuckets) {
        for (TypeBucket typeBucket : typeBuckets) {
            refresh(typeBucket.getBucketId(), typeBucket.getRecordTypes());
        }
    }

    public synchronized void refresh(String bucketId, List<RecordType> recordTypes) {
        Map<SchemaId, RecordType> newBucket = new HashMap<SchemaId, RecordType>(recordTypes.size());
        Map<SchemaId, RecordType> oldBucket = buckets.get(bucketId);
        if (oldBucket == null) {
            oldBucket = new HashMap<SchemaId, RecordType>();
        }
        // Fill a new id bucket and remove found fieldTypes from the old bucket
        for (RecordType recordType : recordTypes) {
            newBucket.put(recordType.getId(), recordType);
            oldBucket.remove(recordType.getId());
        }
        // Remaining record types in the old bucket need to be removed from the
        // name cache
        for (RecordType recordTypeToRemove : oldBucket.values()) {
            nameCache.remove(recordTypeToRemove.getName());
        }
        // Fill nameCache with new record types
        for (RecordType recordType : recordTypes) {
            nameCache.put(recordType.getName(), recordType);
        }
        buckets.put(bucketId, newBucket);
    }

    public synchronized void update(RecordType recordType) {
        SchemaId id = recordType.getId();
        String bucketId = AbstractSchemaCache.encodeHex(id.getBytes());

        Map<SchemaId, RecordType> bucket = buckets.get(bucketId);
        // If the bucket does not exist yet, create it
        if (bucket == null) {
            bucket = new HashMap<SchemaId, RecordType>();
            buckets.put(bucketId, bucket);
        }
        RecordType oldRecordType = bucket.get(id);

        // Remove the old field type
        // We don't remove it from the id cache since we will put (overwrite)
        // the new field type there anyway
        if (oldRecordType != null) {
            nameCache.remove(oldRecordType.getName());
        }
        // Add the new field type
        nameCache.put(recordType.getName(), recordType);
        bucket.put(id, recordType);
    }

}
