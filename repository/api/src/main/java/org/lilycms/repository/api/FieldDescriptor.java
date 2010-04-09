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

/**
 * Describes the properties of a {@link Field}.
 * 
 * <p>
 * Multiple versions of a {@link FieldDescriptor} can exist.
 * 
 * <p>
 * A {@link FieldDescriptor} cannot exist on its own, but is always part of a
 * {@link RecordType}. It's id must be unique within the context of the
 * {@link RecordType}.
 * 
 * 
 */
public interface FieldDescriptor {

    void setId(String string);
    /**
     * The id of the {@link FieldDescriptor} is also the id to be used by the corresponding {@link Field}
     * The id is system generated and unique.  
     * @return the id of the {@link FieldDescriptor}
     */
    String getId();

    /**
     * Set the version of the {@link FieldDescriptor}
     */
    void setVersion(Long version);
    
    /**
     * @return the version of the {@link FieldDescriptor}
     */
    Long getVersion();

    /**
     * Set the global unique name of the {@link FieldDescriptor}.
     * The name can be chose by the user.
     */
    void setName(String name);
    
    /**
     * @return the global unique name of the {@link FieldDescriptor}
     */
    String getName();
    
    /**
     * Set the {@link ValueType} of the {@link Field}
     * @param valueType
     */
    void setValueType(ValueType valueType);
    
    /**
     * @return the {@link ValueType} of the {@link Field}
     */
    ValueType getValueType();

    boolean equals(Object obj);

    FieldDescriptor clone();

}
