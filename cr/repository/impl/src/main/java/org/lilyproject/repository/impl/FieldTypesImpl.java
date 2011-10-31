package org.lilyproject.repository.impl;

import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.repository.api.*;
import org.lilyproject.util.ArgumentValidator;

public class FieldTypesImpl implements FieldTypes {
    private Log log = LogFactory.getLog(getClass());
    
    protected Map<QName, FieldType> nameCache;
    protected Map<String, Map<SchemaId, FieldType>> buckets;

    public FieldTypesImpl() {
        nameCache = new HashMap<QName, FieldType>();
        buckets = new HashMap<String, Map<SchemaId, FieldType>>();
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
