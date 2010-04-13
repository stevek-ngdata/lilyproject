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
 * Repository is the API for all CRUD operations on {@link RecordType}.
 * 
 * <p>
 */
public interface TypeManager {
    
    // Record Types
    /**
     * Creates a new {@link RecordType} object.
     */
    RecordType newRecordType(String recordTypeId);

    /**
     * Creates a {@link RecordType} on the repository with the properties defined in the {@link RecordType} object.
     * @throws RecordTypeExistsException when a recordType with the same id already exists on the repository 
     * @throws RecordTypeNotFoundException when a mixin of the recordType refers to a non-existing {@link RecordType} 
     * @throws FieldTypeNotFoundException 
     * @throws RepositoryException when an unexpected exception occurs on the repository
     */
    RecordType createRecordType(RecordType recordType) throws RecordTypeExistsException, RecordTypeNotFoundException, FieldTypeNotFoundException, RepositoryException;
    
    /**
     * Retrieves the latest version of a {@link RecordType} from the repository.
     * @throws RecordTypeNotFoundException when the recordType does not exist
     * @throws RepositoryException when an unexpected exception occurs on the repository
     */
    RecordType getRecordType(String recordTypeId, Long recordTypeVersion) throws RecordTypeNotFoundException, RepositoryException;

    /**
     * A new version of the {@link RecordType} is created. The new verion number
     * is placed in the recordType object. If a {@link FieldType} should
     * be deleted it should be left out of the {@link RecordType}'s list of
     * {@link FieldType}s.
     * @throws RecordTypeNotFoundException when the recordType to be updated does not exist 
     * @throws FieldTypeNotFoundException 
     * @throws RepositoryException when an unexpected exception occurs on the repository
     */
    RecordType updateRecordType(RecordType recordType) throws  RecordTypeNotFoundException, FieldTypeNotFoundException, RepositoryException;
    
    FieldTypeEntry newFieldTypeEntry(String fieldTypeId, boolean mandatory);
    
    // Field Types
    /**
     * Creates a new {@link FieldType} object.
     */
    FieldType newFieldType(ValueType valueType, QName name, Scope scope);
    
    FieldType newFieldType(String id, ValueType valueType, QName name, Scope scope);
    
    /**
     * Creates a {@link FieldType} on the repository with the properties defined in the {@link FieldType} object.
     * @return a {@link FieldType} 
     * @throws RepositoryException when an unexpected exception occurs on the repository
     * @throws FieldTypeExistsException 
     */
    FieldType createFieldType(FieldType fieldType) throws FieldTypeExistsException, RepositoryException;

    /**
     * Updates a {@link FieldType} on the repository with the properties defined in the {@link FieldType} object.
     * @return a {@link FieldType} 
     * @throws FieldTypeNotFoundException when no fieldType with id and version exists
     * @throws FieldTypeUpdateException an exception occured while updating the FieldType 
     * @throws RepositoryException when an unexpected exception occurs on the repository
     */
    FieldType updateFieldType(FieldType fieldType) throws FieldTypeNotFoundException, FieldTypeUpdateException, RepositoryException; 
    
    /**
     * Gets a {@link FieldType} from the repository
     * @return a {@link FieldType} object 
     * @throws FieldTypeNotFoundException when no fieldType with id and version exists
     * @throws RepositoryException when an unexpected exception occurs on the repository
     */
    FieldType getFieldTypeById(String id) throws FieldTypeNotFoundException, RepositoryException;

    FieldType getFieldTypeByName(QName name);
    
    // Value Types
    
    /**
     * A new {@link PrimitiveValueType} should be registered by calling this method before it can be used.
     */
    void registerPrimitiveValueType(PrimitiveValueType primitiveValueType);

    /**
     * This method should be called to get a {@link ValueType} instance 
     * @param primitiveValueTypeName the name of the {@link PrimitiveValueType} to be encapsulated by this {@link ValueType}
     * @param multiValue if this {@link ValueType} should represent a multi value field or not
     * @param hierarchical if this{@link ValueType} should represent a {@link HierarchyPath} field or not
     */
    ValueType getValueType(String primitiveValueTypeName, boolean multiValue, boolean hierarchical);

   
}
