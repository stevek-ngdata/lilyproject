/*
 * Copyright 2012 NGDATA nv
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
package org.lilyproject.repository.api;

public interface FieldTypeBuilder {
    /**
     * Sets the id of the field type, only needed when updating a field type.
     */
    FieldTypeBuilder id(SchemaId id);

    /**
     * Sets the name for the field type.
     */
    FieldTypeBuilder name(QName name);

    /**
     * Sets the name for the field type.
     */
    FieldTypeBuilder name(String namespace, String name);

    /**
     * Sets the value type for the field type.
     */
    FieldTypeBuilder type(String valueType) throws RepositoryException, InterruptedException;

    /**
     * Sets the value type for the field type.
     */
    FieldTypeBuilder type(ValueType valueType) throws RepositoryException, InterruptedException;

    /**
     * Sets the scope for the field type.
     */
    FieldTypeBuilder scope(Scope scope);

    /**
     * Creates a new field type and returns it.
     */
    FieldType create() throws RepositoryException, InterruptedException;

    /**
     * Updates a field type and returns it.
     */
    FieldType update() throws RepositoryException, InterruptedException;

    /**
     * Creates or updates the field type and returns it.
     *
     * @see {@link TypeManager#createOrUpdateFieldType(FieldType)}
     */
    FieldType createOrUpdate() throws RepositoryException, InterruptedException;

    /**
     * Creates a field type object without creating the field type within the repository.
     */
    FieldType build() throws RepositoryException, InterruptedException;
}
