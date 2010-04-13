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
 * Multiple versions of a {@link FieldType} can exist.
 * 
 * <p>
 * A {@link FieldType} cannot exist on its own, but is always part of a
 * {@link RecordType}. It's id must be unique within the context of the
 * {@link RecordType}.
 * 
 */
public interface FieldType {

    void setId(String string);
    /**
     * The id of the {@link FieldType} is also the id to be used by the corresponding {@link Field}
     * The id is system generated and unique.  
     * @return the id of the {@link FieldType}
     */
    String getId();

    /**
     * Set the global unique name of the {@link FieldType}.
     * The name can be chose by the user.
     */
    void setName(QName name);
    
    /**
     * @return the global unique name of the {@link FieldType}
     */
    QName getName();
    
    /**
     * Set the {@link ValueType} of the {@link Field}
     * @param valueType
     */
    void setValueType(ValueType valueType);
    
    /**
     * @return the {@link ValueType} of the {@link Field}
     */
    ValueType getValueType();
    
    void setScope(Scope scope);
    
    Scope getScope();

    boolean equals(Object obj);

    FieldType clone();

}
