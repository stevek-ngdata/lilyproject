package org.lilyproject.repository.impl;

import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.repository.api.*;
import org.lilyproject.util.ArgumentValidator;

public class FieldTypesImpl implements FieldTypes {
    private Log log = LogFactory.getLog(getClass());
    
    private Map<QName, FieldType> fieldTypeNameCache = new HashMap<QName, FieldType>();
    private Map<SchemaId, FieldType> fieldTypeIdCache = new HashMap<SchemaId, FieldType>();

    @Override
    public synchronized FieldTypesImpl clone() {
        FieldTypesImpl newFieldTypes = new FieldTypesImpl();
        newFieldTypes.fieldTypeNameCache.putAll(fieldTypeNameCache);
        newFieldTypes.fieldTypeIdCache.putAll(fieldTypeIdCache);
        return newFieldTypes;
    }
    
    public synchronized void refresh(List<FieldType> fieldTypes) {
        Map<QName, FieldType> newFieldTypeNameCache = new HashMap<QName, FieldType>();
        Map<SchemaId, FieldType> newFieldTypeIdCache = new HashMap<SchemaId, FieldType>();
        try {
            for (FieldType fieldType : fieldTypes) {
                newFieldTypeNameCache.put(fieldType.getName(), fieldType);
                newFieldTypeIdCache.put(fieldType.getId(), fieldType);
            }
            fieldTypeNameCache = newFieldTypeNameCache;
            fieldTypeIdCache = newFieldTypeIdCache;
        } catch (Exception e) {
            // We keep on working with the old cache
            log.warn("Exception while refreshing FieldType cache. Cache is possibly out of date.", e);
        }
    }
    
    public synchronized void update(FieldType fieldType) {
        FieldType oldFieldType = fieldTypeIdCache.get(fieldType.getId());
        if (oldFieldType != null) {
            fieldTypeNameCache.remove(oldFieldType.getName());
            fieldTypeIdCache.remove(oldFieldType.getId());
        }
        fieldTypeNameCache.put(fieldType.getName(), fieldType);
        fieldTypeIdCache.put(fieldType.getId(), fieldType);
    }
    
    public synchronized List<FieldType> getFieldTypes() {
        List<FieldType> fieldTypes = new ArrayList<FieldType>();
        for (FieldType fieldType : fieldTypeNameCache.values()) {
            fieldTypes.add(fieldType.clone());
        }
        return fieldTypes;
    }
    
    @Override
    public FieldType getFieldType(SchemaId id) throws FieldTypeNotFoundException {
        ArgumentValidator.notNull(id, "id");
        FieldType fieldType = fieldTypeIdCache.get(id);
        if (fieldType == null) {
            throw new FieldTypeNotFoundException(id);
        }
        return fieldType.clone();
    }

    @Override
    public FieldType getFieldType(QName name) throws FieldTypeNotFoundException {
        ArgumentValidator.notNull(name, "name");
        FieldType fieldType = fieldTypeNameCache.get(name);
        if (fieldType == null) {
            throw new FieldTypeNotFoundException(name);
        }
        return fieldType.clone();
    }

    public FieldType getFieldTypeByNameReturnNull(QName name) {
        ArgumentValidator.notNull(name, "name");
        FieldType fieldType = fieldTypeNameCache.get(name);
        return fieldType != null ? fieldType.clone() : null;
    }

    public boolean fieldTypeExists(QName name) {
        return fieldTypeNameCache.containsKey(name);
    }
}
