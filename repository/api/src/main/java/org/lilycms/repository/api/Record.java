/*
 * Copyright 2010 Outerthought bvba
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
package org.lilycms.repository.api;

import java.util.List;
import java.util.Map;

/**
 * An object to be used as input for {@link Repository#create} and
 * {@link Repository#update} operations, or as result of a {link
 * Repository#read} operation.
 * 
 * <p>
 * The {@link RecordType} and its version define the schema of the record.
 * 
 * <p>
 * For an {@link Repository#update} only the fields to be changed need to be
 * given.
 * 
 * <p>
 * Fields to be deleted need to be added explicitly with their fieldId
 * 
 */
public interface Record {
    public static enum Scope {
        NON_VERSIONABLE, VERSIONABLE, VERSIONABLE_MUTABLE 
    }
    
    void setId(RecordId recordId);

    RecordId getId();

    void setVersion(Long version);

    Long getVersion();

    void setRecordType(String id, Long version);

    String getRecordTypeId();

    Long getRecordTypeVersion();
    
    void setRecordType(Scope scope, String id, Long version);
    
    String getRecordTypeId(Scope scope);
    
    Long getRecordTypeVersion(Scope scope);
    
    void setField(Scope scope, String fieldId, Object value);

    Object getField(Scope scope, String fieldId) throws FieldNotFoundException;

    Map<String, Object> getFields(Scope scope);

    void addFieldsToDelete(Scope scope, List<String> fieldIds);
    
    void removeFieldsToDelete(Scope scope, List<String> fieldIds);

    List<String> getFieldsToDelete(Scope scope);
    
    Record clone();
    
    boolean equals(Object obj);
}
