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


/**
 * Repository is the API for all CRUD operations on {@link RecordType}.
 * 
 * <p>
 */
public interface TypeManager {
    /**
     * Creates a new {@link RecordType} object.
     */
    RecordType newRecordType(String recordTypeId);

    /**
     * Creates a {@link RecordType} on the repository with the properties defined in the {@link RecordType} object.
     * @throws RecordTypeExistsException when a recordType with the same id already exists on the repository 
     * @throws FieldGroupNotFoundException when the recordType refers to a non-existing {@link FieldGroup}
     * @throws RepositoryException when an unexpected exception occurs on the repository
     */
    RecordType createRecordType(RecordType recordType) throws RecordTypeExistsException, FieldGroupNotFoundException, RepositoryException;
    
    /**
     * Retrieves the latest version of a {@link RecordType} from the repository.
     * @throws RecordTypeNotFoundException when the recordType does not exist
     * @throws RepositoryException when an unexpected exception occurs on the repository
     */
    RecordType getRecordType(String recordTypeId, Long recordTypeVersion) throws RecordTypeNotFoundException, RepositoryException;

    /**
     * A new version of the {@link RecordType} is created. The new verion number
     * is placed in the recordType object. If a {@link FieldDescriptor} should
     * be deleted it should be left out of the {@link RecordType}'s list of
     * {@link FieldDescriptor}s.
     * @throws RecordTypeNotFoundException when the recordType to be updated does not exist 
     * @throws FieldGroupNotFoundException when a {@link FieldGroup} referred to by the recordType does not exist
     * @throws RepositoryException when an unexpected exception occurs on the repository
     */
    RecordType updateRecordType(RecordType recordType) throws  RecordTypeNotFoundException, FieldGroupNotFoundException, RepositoryException;
    
    /**
     * Removes fieldGroups from the recordType. If no fieldGroup existed it is ignored.
     * @param recordTypeId the id of the {@link RecordType}
     * @param nonVersionable if the non-versionable fieldGroup should be removed
     * @param versionable if the versionable fieldGroup should be removed
     * @param versionableMutable if the versionable-mutable fieldGroup should be removed
     * @return a {@link RecordType} with an updated version number and the requested fieldGroups removed
     * @throws RecordTypeNotFoundException when the recordType does not exist
     * @throws RepositoryException when an unexpected exception occurs on the repository
     */
    RecordType removeFieldGroups(String recordTypeId, boolean nonVersionable, boolean versionable, boolean versionableMutable) throws RecordTypeNotFoundException, RepositoryException;
    
    /**
     * Creates a new {@link FieldGroup} object.
     */
    FieldGroup newFieldGroup(String id);
    
    FieldGroupEntry newFieldGroupEntry(String fieldDescriptorId, Long fieldDescriptorVersion, boolean mandatory, String alias);
    
    /**
     * Creates a {@link FieldGroup} on the repository with the properties defined in the {@link FieldGroup} object.
     * @return a {@link FieldGroup} object containing the updated version number of the fieldGroup
     * @throws FieldGroupExistsException when a fieldGroup with the same id already exists on the repository 
     * @throws FieldDescriptorNotFoundException when the fieldGroup refers to a non-existing {@link FieldDescriptor} 
     * @throws RepositoryException when an unexpected exception occurs on the repository
     */
    FieldGroup createFieldGroup(FieldGroup fieldGroup) throws FieldGroupExistsException, FieldDescriptorNotFoundException, RepositoryException;
    
    /**
     * Updates a {@link FieldGroup} on the repository with the properties defined in the {@link FieldGroup} object.
     * @return a {@link FieldGroup} object containing the updated version number of the fieldGroup
     * @throws FieldGroupNotFoundException when the fieldGroup to update does not exist
     * @throws FieldDescriptorNotFoundException when the updated fieldGroup refers to a non-existing {@link FieldDescriptor}
     * @throws RepositoryException when an unexpected exception occurs on the repository
     */
    FieldGroup updateFieldGroup(FieldGroup fieldGroup) throws FieldGroupNotFoundException, FieldDescriptorNotFoundException, RepositoryException;
    
    /**
     * Removes fieldDescriptors from fieldGroup. This operation is performed towards the latest version of the fieldGroup.
     * FieldDescriptors that are not known by the fieldGroup are ignored.
     * @param fieldGroupId the id of the {@link FieldGroup} to perform this operation on
     * @param fieldDescriptorIds a list of {@link FieldDescriptors} to remove
     * @return a new {@link FieldGroup} with an updated version number and the remaining fieldDescriptors
     * @throws FieldGroupNotFoundException when the fieldGroup does not exist
     * @throws RepositoryException when an unexpected exception occurs in the repository
     */
    FieldGroup removeFieldDescriptors(String fieldGroupId, List<String> fieldDescriptorIds) throws FieldGroupNotFoundException, RepositoryException;
    
    /**
     * Gets a {@link FieldGroup} from the repository
     * @param version is the version of the {@link FieldGroup} to get. If null, the latest existing version is taken.
     * @return a {@link FieldGroup} object 
     * @throws FieldGroupNotFoundException when no fieldGroup with id and version exists
     * @throws RepositoryException when an unexpected exception occurs on the repository
     */
    FieldGroup getFieldGroup(String id, Long version) throws FieldGroupNotFoundException, RepositoryException;
    
    // Field Descriptors
    /**
     * Creates a new {@link FieldDescriptor} object.
     */
    FieldDescriptor newFieldDescriptor(String id, ValueType valueType, String globalName);
    
    /**
     * Creates a {@link FieldDescriptor} on the repository with the properties defined in the {@link FieldDescriptor} object.
     * @return a {@link FieldDescriptor} object containing the updated version number of the fieldDescriptor
     * @throws FieldDescriptorExistsException if a fieldDescriptor with the same id already exists on the repository 
     * @throws RepositoryException when an unexpected exception occurs on the repository
     */
    FieldDescriptor createFieldDescriptor(FieldDescriptor fieldDescriptor) throws FieldDescriptorExistsException, RepositoryException;

    /**
     * Updates a {@link FieldDescriptor} on the repository with the properties defined in the {@link FieldDescriptor} object.
     * The version number in the given fieldDescriptor is ignored, a new version number will be assigned.
     * @return a {@link FieldDescriptor} object containing the new version number of the fieldDescriptor
     * @throws FieldDescriptorNotFoundException when no fieldDescriptor with id and version exists
     * @throws FieldDescriptorUpdateException an exception occured while updating the FieldDescriptor 
     * @throws RepositoryException when an unexpected exception occurs on the repository
     */
    FieldDescriptor updateFieldDescriptor(FieldDescriptor fieldDescriptor) throws FieldDescriptorNotFoundException, FieldDescriptorUpdateException, RepositoryException; 
    
    /**
     * Gets a {@link FieldDescriptor} from the repository
     * @param version is the version of the {@link FieldDescriptor} to get. If null, the latest existing version is taken.
     * @return a {@link FieldDescriptor} object 
     * @throws FieldDescriptorNotFoundException when no fieldDescriptor with id and version exists
     * @throws RepositoryException when an unexpected exception occurs on the repository
     */
    FieldDescriptor getFieldDescriptor(String id, Long version) throws FieldDescriptorNotFoundException, RepositoryException;

    
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
