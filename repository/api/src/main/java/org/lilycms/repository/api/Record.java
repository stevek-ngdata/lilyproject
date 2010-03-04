package org.lilycms.repository.api;

import java.util.Map;
import java.util.Set;

public interface Record {
    void setRecordType(String recordTypeName, long recordTypeVersion);
    String getRecordTypeName();
    long getRecordTypeVersion();
    void addField(Field field);
    Field getField(String fieldName) throws FieldNotFoundException;
    Set<Field> getFields();
    void setRecordId(String recordId);
    String getRecordId();
    void deleteField(String fieldName);
    Set<String> getDeleteFields();
    void addVariantProperty(String dimension, String dimensionValue);
    void addVariantProperties(Map<String, String>variantProperties);
    Map<String, String> getVariantProperties();
}
