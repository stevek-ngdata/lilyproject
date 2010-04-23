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
 * A FieldType is a value object describing the properties of a field, and is
 * used in a RecordType to describe the fields a Record can consist of, see
 * {@link RecordType} and {@link FieldTypeEntry}
 * 
 * <p>
 * A FieldType has an immutable system-generated unique id and a unique but
 * mutable qualified name {@link QName}, both uniquely identifying the
 * FieldType.
 * <p>
 * An immutable {@link ValueType} describes the underlying primitive type of the
 * FieldType and if it is a multivalue field, or hierarchical.
 * <p>
 * The immutable {@link Scope} descibes to which Scope the field belongs.
 * 
 * <p>
 * A FieldType object should be instantiated through
 * {@link TypeManager#newFieldType(ValueType, QName, Scope)}.
 * <p>
 * A new FieldType can be created on the repository by calling
 * {@link TypeManager#createFieldType(FieldType)}. After creating a new
 * FieldType it will be assigned a system-generated unique id.
 */
public interface FieldType {

    /**
     * Sets the id of the FieldType on the value object.
     */
    void setId(String id);

    /**
     * The id is unique, immutable and system-generated.
     *
     * @return the id of the FieldType
     */
    String getId();

    /**
     * Sets the name of the FieldType on the value object.
     * @param name the qualified name to be used, see {@link QName}
     */
    void setName(QName name);

    /**
     * The name is unique, user-provided and can be changed by calling {@link TypeManager#updateFieldType(FieldType)}.
     * @return the name of the FieldType
     */
    QName getName();

    /**
     * Sets the valueType of the FieldType on the value object.
     * @param valueType the valueType to be used, see {@link ValueType}
     *
     */
    void setValueType(ValueType valueType);

    /**
     * The valueType describes the primitive type and if it is multivalue or hierarchical, see {@link ValueType}
     * @return the valueType of the FieldType
     */
    ValueType getValueType();

    /**
     * Sets the scope of the FieldType on the value object.
     * @param scope the scope to be used, see {@link Scope}
     */
    void setScope(Scope scope);

    /**
     * The scope defines to which scope a FieldType (and related fields) belong (e.g. Non-Versioned, Versioned, Versioned-Mutable), see {@link Scope}
     * @return the scope of the FieldType
     */
    Scope getScope();

    boolean equals(Object obj);

    FieldType clone();
}
