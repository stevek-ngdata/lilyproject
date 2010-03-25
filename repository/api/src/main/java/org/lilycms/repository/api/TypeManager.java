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
    /**
     * Creates a new {@link RecordType} object.
     */
    RecordType newRecordType(String recordTypeId) throws RepositoryException;

    /**
     * Creates a new {@link FieldDescriptor} object.
     */
    FieldDescriptor newFieldDescriptor(String fieldDescriptorId, ValueType valueType, boolean mandatory, boolean versionable) throws RepositoryException;
    
    /**
     * Creates a new {@link FieldDescriptor} object.
     */
    FieldDescriptor newFieldDescriptor(String fieldDescriptorId, Long version, ValueType valueType, boolean mandatory, boolean versionable) throws RepositoryException;
    
    /**
     * Creates a {@link RecordType} on the repository with the properties defined in the {@link RecordType} object.
     */
    void createRecordType(RecordType recordType) throws RepositoryException;

    /**
     * Retrieves the latest version of a {@link RecordType} from the repository.
     */
    RecordType getRecordType(String recordTypeId) throws RepositoryException;

    /**
     * Retrieves a specific version of a {@link RecordType} from the repository.
     * If no version is given, the latest version is retrieved.
     */
    RecordType getRecordType(String recordTypeId, Long recordTypeVersion) throws RepositoryException;

    /**
     * A new version of the {@link RecordType} is created. The new verion number
     * is placed in the recordType object. If a {@link FieldDescriptor} should
     * be deleted it should be left out of the {@link RecordType}'s list of
     * {@link FieldDescriptor}s.
     */
    void updateRecordType(RecordType recordType) throws RepositoryException;
    
    /**
     * This method should be called to get a {@link ValueType} instance 
     * @param primitiveValueTypeName the name of the {@link PrimitiveValueType} to be encapsulated by this {@link ValueType}
     * @param multiValue if this {@link ValueType} should represent a multi value field or not
     * @param hierarchical if this{@link ValueType} should represent a {@link HierarchyPath} field or not
     */
    ValueType getValueType(String primitiveValueTypeName, boolean multiValue, boolean hierarchical);
    
    /**
     * A new {@link PrimitiveValueType} should be registered by calling this method before it can be used.
     */
    void registerPrimitiveValueType(PrimitiveValueType primitiveValueType);
}
