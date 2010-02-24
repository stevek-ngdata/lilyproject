package org.lilycms.repository.api;

import java.util.Set;

public interface Record {
    void addField(Field field);
    Field getField(String fieldName);
    Set<Field> getFields();
    void setRecordId(String recordId);
    String getRecordId();
    void deleteField(String fieldName);
    Set<String> getDeleteFields();
}
