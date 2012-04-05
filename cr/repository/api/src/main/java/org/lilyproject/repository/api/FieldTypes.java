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

import java.util.List;

/**
 * The FieldTypes contains a snapshot of the field types in the Lily Schema. 
 * 
 * <p> To be used when a consistent snapshot is needed while performing CRUD operations.
 */
public interface FieldTypes {
    /**
     * Gets a FieldType from the FieldTypes.
     *
     * @throws FieldTypeNotFoundException when no fieldType with the given ID exists
     * @throws RepositoryException when an unexpected exception occurs on the repository
     */
    FieldType getFieldType(SchemaId id) throws TypeException, InterruptedException;
    
    /**
     * Gets a FieldType from the FieldTypes.
     *
     * @throws FieldTypeNotFoundException when no fieldType with the given name exists
     * @throws RepositoryException when an unexpected exception occurs on the repository
     */
    FieldType getFieldType(QName name) throws TypeException, InterruptedException;

    List<FieldType> getFieldTypes() throws TypeException, InterruptedException;

    /**
     * Gets a FieldType from the FieldTypes.
     * <p>
     * Does not throw a FieldTypeNotFoundException when the field type is not
     * known, but returns null instead.
     * 
     * @return the field type or null if not known
     */
    FieldType getFieldTypeByNameReturnNull(QName name) throws InterruptedException;

    /**
     * Checks if a field type is known by the FieldTypes
     * 
     * @return true if the field type is known
     */
    boolean fieldTypeExists(QName name) throws InterruptedException;


}
