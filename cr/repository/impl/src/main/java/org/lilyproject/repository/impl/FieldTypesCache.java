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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.repository.api.*;

public class FieldTypesCache extends FieldTypesImpl implements FieldTypes {
    private Log log = LogFactory.getLog(getClass());

    public synchronized FieldTypesImpl getSnapshot() {
        FieldTypesImpl newFieldTypes = new FieldTypesImpl();
        newFieldTypes.nameCache.putAll(nameCache);
        for (Entry<String, Map<SchemaId, FieldType>> bucketEntry : buckets.entrySet()) {
            Map<SchemaId, FieldType> fieldTypeIdBucket = new HashMap<SchemaId, FieldType>();
            fieldTypeIdBucket.putAll(bucketEntry.getValue());
            newFieldTypes.buckets.put(bucketEntry.getKey(), fieldTypeIdBucket);
        }
        return newFieldTypes;
    }

    public synchronized void refreshFieldTypes(List<FieldType> fieldTypes) {
        nameCache = new HashMap<QName, FieldType>(fieldTypes.size());
        buckets = new HashMap<String, Map<SchemaId, FieldType>>();
        for (FieldType fieldType : fieldTypes) {
            nameCache.put(fieldType.getName(), fieldType);
            String bucketId = AbstractSchemaCache.encodeHex(fieldType.getId().getBytes());
            Map<SchemaId, FieldType> bucket = buckets.get(bucketId);
            if (bucket == null) {
                bucket = new HashMap<SchemaId, FieldType>();
                buckets.put(bucketId, bucket);
            }
            bucket.put(fieldType.getId(), fieldType);
        }
    }

    public synchronized void refreshFieldTypeBuckets(List<TypeBucket> typeBuckets) {
        for (TypeBucket typeBucket : typeBuckets) {
            refresh(typeBucket.getBucketId(), typeBucket.getFieldTypes());
        }
    }

    private void refresh(String bucketId, List<FieldType> fieldTypes) {
        Map<SchemaId, FieldType> newBucket = new HashMap<SchemaId, FieldType>(fieldTypes.size());
        Map<SchemaId, FieldType> oldBucket = buckets.get(bucketId);
        if (oldBucket == null) {
            oldBucket = new HashMap<SchemaId, FieldType>();
        }
        // Fill a new id bucket and remove found fieldTypes from the old bucket
        for (FieldType fieldType : fieldTypes) {
            newBucket.put(fieldType.getId(), fieldType);
            oldBucket.remove(fieldType.getId());
        }
        // Remaining fieldTypes in the old bucket need to be removed from the
        // name cache
        for (FieldType fieldTypeToRemove : oldBucket.values()) {
            nameCache.remove(fieldTypeToRemove.getName());
        }
        // Fill nameCache with the new fieldTypes
        for (FieldType fieldType : fieldTypes) {
            nameCache.put(fieldType.getName(), fieldType);
        }
        buckets.put(bucketId, newBucket);
    }

    public synchronized void update(FieldType fieldType) {
        SchemaId id = fieldType.getId();
        String bucketId = AbstractSchemaCache.encodeHex(id.getBytes());

        Map<SchemaId, FieldType> bucket = buckets.get(bucketId);
        // If the bucket does not exist yet, create it
        if (bucket == null) {
            bucket = new HashMap<SchemaId, FieldType>();
            buckets.put(bucketId, bucket);
        }
        FieldType oldFieldType = bucket.get(id);

        // Remove the old field type
        // We don't remove it from the id cache since we will put (overwrite)
        // the new field type there anyway
        if (oldFieldType != null) {
            nameCache.remove(oldFieldType.getName());
        }
        // Add the new field type
        nameCache.put(fieldType.getName(), fieldType);
        bucket.put(id, fieldType);
    }

    
    // 
    // FieldTypes API
    // 

    @Override
    public synchronized boolean fieldTypeExists(QName name) {
        return super.fieldTypeExists(name);
    }

    @Override
    public synchronized FieldType getFieldType(SchemaId id) throws FieldTypeNotFoundException {
        return super.getFieldType(id);
    }

    @Override
    public synchronized FieldType getFieldType(QName name) throws FieldTypeNotFoundException {
        return super.getFieldType(name);
    }

    @Override
    public synchronized FieldType getFieldTypeByNameReturnNull(QName name) {
        return super.getFieldTypeByNameReturnNull(name);
    }

    @Override
    public synchronized List<FieldType> getFieldTypes() {
        return super.getFieldTypes();
    }
}
