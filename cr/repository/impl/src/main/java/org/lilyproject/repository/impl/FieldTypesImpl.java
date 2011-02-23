package org.lilyproject.repository.impl;

import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.repository.api.*;
import org.lilyproject.util.ArgumentValidator;
import org.lilyproject.util.ByteArrayKey;

public class FieldTypesImpl implements FieldTypes {
    private Log log = LogFactory.getLog(getClass());
    
    private Map<QName, FieldType> fieldTypeNameCache = new HashMap<QName, FieldType>();
    private Map<String, FieldType> fieldTypeIdCache = new HashMap<String, FieldType>();
    private Map<ByteArrayKey, FieldType> fieldTypeBytesIdCache = new HashMap<ByteArrayKey, FieldType>();

    public synchronized FieldTypesImpl clone() {
        FieldTypesImpl newFieldTypes = new FieldTypesImpl();
        newFieldTypes.fieldTypeNameCache.putAll(fieldTypeNameCache);
        newFieldTypes.fieldTypeIdCache.putAll(fieldTypeIdCache);
        newFieldTypes.fieldTypeBytesIdCache.putAll(fieldTypeBytesIdCache);
        return newFieldTypes;
    }
    
    public synchronized void refresh(List<FieldType> fieldTypes) {
        Map<QName, FieldType> newFieldTypeNameCache = new HashMap<QName, FieldType>();
        Map<String, FieldType> newFieldTypeIdCache = new HashMap<String, FieldType>();
        Map<ByteArrayKey, FieldType> newFieldTypeBytesIdCache = new HashMap<ByteArrayKey, FieldType>();
        try {
            for (FieldType fieldType : fieldTypes) {
                newFieldTypeNameCache.put(fieldType.getName(), fieldType);
                newFieldTypeIdCache.put(fieldType.getId(), fieldType);
                newFieldTypeBytesIdCache.put(new ByteArrayKey(HBaseTypeManager.idToBytes(fieldType.getId())), fieldType);
            }
            fieldTypeNameCache = newFieldTypeNameCache;
            fieldTypeIdCache = newFieldTypeIdCache;
            fieldTypeBytesIdCache = newFieldTypeBytesIdCache;
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
            fieldTypeBytesIdCache.remove(new ByteArrayKey(HBaseTypeManager.idToBytes(oldFieldType.getId())));
        }
        fieldTypeNameCache.put(fieldType.getName(), fieldType);
        fieldTypeIdCache.put(fieldType.getId(), fieldType);
        fieldTypeBytesIdCache.put(new ByteArrayKey(HBaseTypeManager.idToBytes(fieldType.getId())), fieldType);
    }
    
    public synchronized List<FieldType> getFieldTypes() {
        List<FieldType> fieldTypes = new ArrayList<FieldType>();
        for (FieldType fieldType : fieldTypeNameCache.values()) {
            fieldTypes.add(fieldType.clone());
        }
        return fieldTypes;
    }
    
    public FieldType getFieldTypeById(String id) throws FieldTypeNotFoundException {
        ArgumentValidator.notNull(id, "id");
        FieldType fieldType = fieldTypeIdCache.get(id);
        if (fieldType == null) {
            throw new FieldTypeNotFoundException(id);
        }
        return fieldType.clone();
    }
    
    public FieldType getFieldTypeById(byte[] id) throws FieldTypeNotFoundException {
        ArgumentValidator.notNull(id, "id");
        FieldType fieldType = fieldTypeBytesIdCache.get(new ByteArrayKey(id));
        if (fieldType == null) {
            throw new FieldTypeNotFoundException(HBaseTypeManager.idFromBytes(id));
        }
        return fieldType.clone();
    }

    public FieldType getFieldTypeByName(QName name) throws FieldTypeNotFoundException {
        ArgumentValidator.notNull(name, "name");
        FieldType fieldType = fieldTypeNameCache.get(name);
        if (fieldType == null) {
            throw new FieldTypeNotFoundException(name);
        }
        return fieldType.clone();
    }
}
