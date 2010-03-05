package org.lilycms.repository.api;

import java.util.Map;
import java.util.Set;

public interface Record {
    String getRecordId();
    void setRecordVersion(long version);
    long getRecordVersion();
    void setRecordType(String recordTypeId, long recordTypeVersion);
    String getRecordTypeId();
    long getRecordTypeVersion();
    void addField(Field field);
    Field getField(String fieldId) throws FieldNotFoundException;
    Set<Field> getFields();
    void deleteField(String fieldId);
    Set<String> getDeleteFields();
    void addVariantProperty(String dimension, String dimensionValue);
    void addVariantProperties(Map<String, String>variantProperties);
    Map<String, String> getVariantProperties();
}
