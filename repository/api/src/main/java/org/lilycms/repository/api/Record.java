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

import java.util.Map;
import java.util.Set;

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
 * <p>
 * A record can either be an ordinary record or a variant record, in which case
 * the variantProperties need to be provided, and the {@link RecordId} should
 * point to the master record.
 */
public interface Record {
    void setId(RecordId recordId);

    RecordId getId();

    void setVersion(Long version);

    Long getVersion();

    void setRecordType(String recordTypeId, long recordTypeVersion);

    String getRecordTypeId();

    long getRecordTypeVersion();

    void setField(String fieldId, Object value);

    Object getField(String fieldId) throws FieldNotFoundException;

    Map<String, Object> getFields();

    void deleteField(String fieldId);

    Set<String> getDeleteFields();
}
