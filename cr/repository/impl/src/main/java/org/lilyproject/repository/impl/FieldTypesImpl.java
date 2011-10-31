package org.lilyproject.repository.impl;

import java.util.*;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.repository.api.*;
import org.lilyproject.util.ArgumentValidator;

public class FieldTypesImpl implements FieldTypes {
    private Log log = LogFactory.getLog(getClass());
    

    private Map<QName, FieldType> nameCache;
    private Map<String, Map<SchemaId, FieldType>> buckets;

    public FieldTypesImpl() {
        nameCache = new HashMap<QName, FieldType>();
        buckets = new HashMap<String, Map<SchemaId, FieldType>>();
    }

    @Override
    public synchronized FieldTypesImpl clone() {
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

    public List<FieldType> getFieldTypes() {
        List<FieldType> fieldTypes = new ArrayList<FieldType>();
        for (FieldType fieldType : nameCache.values()) {
            fieldTypes.add(fieldType.clone());
        }
        return fieldTypes;
    }
    
    // No synchronize needed since these methods will only be called on a cloned
    // FieldTypes
    // On this cloned FieldTypes no refresh or update methods will be called
    @Override
    public FieldType getFieldType(SchemaId id) throws FieldTypeNotFoundException {
        ArgumentValidator.notNull(id, "id");
        String bucket = AbstractSchemaCache.encodeHex(id.getBytes());
        
        Map<SchemaId, FieldType> fieldTypeIdCacheBucket = buckets.get(bucket);
        if (fieldTypeIdCacheBucket == null) {
            throw new FieldTypeNotFoundException(id);
        }
        FieldType fieldType = fieldTypeIdCacheBucket.get(id);
        if (fieldType == null) {
            throw new FieldTypeNotFoundException(id);
        }
        return fieldType.clone();
    }

    @Override
    public FieldType getFieldType(QName name) throws FieldTypeNotFoundException {
        ArgumentValidator.notNull(name, "name");
        FieldType fieldType = nameCache.get(name);
        if (fieldType == null) {
            throw new FieldTypeNotFoundException(name);
        }
        return fieldType.clone();
    }

    public FieldType getFieldTypeByNameReturnNull(QName name) {
        ArgumentValidator.notNull(name, "name");
        FieldType fieldType = nameCache.get(name);
        return fieldType != null ? fieldType.clone() : null;
    }

    public boolean fieldTypeExists(QName name) {
        return nameCache.containsKey(name);
    }
}
