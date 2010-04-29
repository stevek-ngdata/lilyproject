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

import org.lilycms.repository.api.exception.FieldNotFoundException;

/**
 * A Record is the core entity managed by the {@link Repository}.
 *
 * A Record object is used as input for the {@link Repository#create} and {@link Repository#update} operations,
 * or as result of a {@link Repository#read} operation.
 *
 * <p>A Record object is not necessarily a representation of a complete record. When {@link Repository#read reading}
 * a record, you can specify to only read certain fields. Likewise, when {@link Repository#update updating}, you
 * only need to put the fields in this object that you want to be updated. But it is not necessary to remove
 * unchanged fields from this object, the repository will compare with the current situation and ignore
 * unchanged fields.
 *
 * <p>Since for an update, this object only needs to contain the fields you want to update, fields that are
 * not in this object will not be automatically removed from the record. Rather, you have to say explicitly which
 * fields should be deleted by adding them to the {@link #getFieldsToDelete() fields-to-delete} list. If the
 * fields-to-delete list contains field names that do not exist in the record, then these will be ignored upon
 * update, rather than causing an exception.
 * 
 * <p>The {@link RecordType} and its version define the schema of the record.
 * 
 */
public interface Record {
    void setId(RecordId recordId);

    RecordId getId();

    void setVersion(Long version);

    /**
     * TODO what returns this method if the record has no versions? what does it return when the record has versions
     *      but no versioned fields were read?
     */
    Long getVersion();

    void setRecordType(String id, Long version);

    String getRecordTypeId();

    Long getRecordTypeVersion();
    
    void setRecordType(Scope scope, String id, Long version);
    
    String getRecordTypeId(Scope scope);
    
    Long getRecordTypeVersion(Scope scope);
    
    void setField(QName fieldName, Object value);

    /**
     * Deletes a field from this object and optionally adds it to the list of fields to delete upon save.
     *
     * <p>If the field is not present, this does not throw an exception. In this case, the field will still
     * be added to the list of fields to delete. Use {@link #hasField} if you want to check if a field is
     * present in this Record instance.
     *
     * @param addToFieldsToDelete if false, the field will only be removed from this value object, but not be added
     *                            to the {@link #getFieldsToDelete}.
     */
    void delete(QName fieldName, boolean addToFieldsToDelete);

    /**
     *
     * @throws FieldNotFoundException if the field is not present in this Record object. To avoid the exception,
     *         use {@link #hasField}.
     */
    Object getField(QName fieldName) throws FieldNotFoundException;

    boolean hasField(QName fieldName);

    Map<QName, Object> getFields();

    void addFieldsToDelete(List<QName> fieldNames);
    
    void removeFieldsToDelete(List<QName> fieldNames);

    List<QName> getFieldsToDelete();
    
    Record clone();
    
    boolean equals(Object obj);
}
