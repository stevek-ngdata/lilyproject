package org.lilyproject.repository.impl;

import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.repository.api.*;
import org.lilyproject.util.ArgumentValidator;

public class FieldTypesImpl implements FieldTypes {
    private Log log = LogFactory.getLog(getClass());
    
    // Always use getNameCache() instead of this variable directly, to make sure
    // this is the up-to-date nameCache (in case of FieldTypesCache).
    protected Map<QName, FieldType> nameCache;
    protected Map<String, Map<SchemaId, FieldType>> buckets;

    public FieldTypesImpl() {
        nameCache = new HashMap<QName, FieldType>();
        buckets = new HashMap<String, Map<SchemaId, FieldType>>();
    }

    protected Map<QName, FieldType> getNameCache() throws InterruptedException {
        return nameCache;
    }

    @Override
    public List<FieldType> getFieldTypes() throws InterruptedException {
        List<FieldType> fieldTypes = new ArrayList<FieldType>();
        for (FieldType fieldType : getNameCache().values()) {
            fieldTypes.add(fieldType.clone());
        }
        return fieldTypes;
    }
    
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
    public FieldType getFieldType(QName name) throws FieldTypeNotFoundException, InterruptedException {
        ArgumentValidator.notNull(name, "name");
        FieldType fieldType = getNameCache().get(name);
        if (fieldType == null) {
            throw new FieldTypeNotFoundException(name);
        }
        return fieldType.clone();
    }

    public FieldType getFieldTypeByNameReturnNull(QName name) throws InterruptedException {
        ArgumentValidator.notNull(name, "name");
        FieldType fieldType = getNameCache().get(name);
        return fieldType != null ? fieldType.clone() : null;
    }

    public boolean fieldTypeExists(QName name) throws InterruptedException {
        return getNameCache().containsKey(name);
    }
}
