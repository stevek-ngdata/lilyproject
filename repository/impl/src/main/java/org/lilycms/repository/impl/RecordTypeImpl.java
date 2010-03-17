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
package org.lilycms.repository.impl;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.lilycms.repository.api.FieldDescriptor;
import org.lilycms.repository.api.RecordType;

public class RecordTypeImpl implements RecordType {
    
    private final String recordTypeId;
    private Map<String, FieldDescriptor> fieldDescriptors = new HashMap<String, FieldDescriptor>();
    private Long version;

    /**
     * This constructor should not be called directly.
     * @use {@link TypeManager#newRecordType} instead
     */
    public RecordTypeImpl(String recordTypeId) {
        this.recordTypeId = recordTypeId;
    }
    
    public void addFieldDescriptor(FieldDescriptor fieldDescriptor) {
        fieldDescriptors.put(fieldDescriptor.getId(), fieldDescriptor);
    }
    
    public void removeFieldDescriptor(String fieldDescriptorId) {
        fieldDescriptors.remove(fieldDescriptorId);
    }

    public FieldDescriptor getFieldDescriptor(String fieldDescriptorId) {
        return fieldDescriptors.get(fieldDescriptorId);
    }
    
    public Collection<FieldDescriptor> getFieldDescriptors() {
        return fieldDescriptors.values();
    }

    public String getId() {
        return recordTypeId;
    }

    public Long getVersion() {
        return version;
    }
    
    public void setVersion(Long version){
        this.version = version;
    }
}
