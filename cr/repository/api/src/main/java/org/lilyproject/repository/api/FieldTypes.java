package org.lilyproject.repository.api;

/**
 * The FieldTypes contains a snapshot of the field types in the Lily Schema. 
 * 
 * <p> To be used when a consistent shapshot is needed while performing CRUD operations.
 */
public interface FieldTypes {
    /**
     * Gets a FieldType from the FieldTypes.
     *
     * @throws FieldTypeNotFoundException when no fieldType with the given ID exists
     * @throws RepositoryException when an unexpected exception occurs on the repository
     */
    FieldType getFieldTypeById(String id) throws FieldTypeNotFoundException, TypeException, InterruptedException;
    
    /**
     * Gets a FieldType from the FieldTypes.
     *
     * @throws FieldTypeNotFoundException when no fieldType with the given ID exists
     * @throws RepositoryException when an unexpected exception occurs on the repository
     */
    FieldType getFieldTypeById(byte[] id) throws FieldTypeNotFoundException, TypeException, InterruptedException;

    /**
     * Gets a FieldType from the FieldTypes.
     *
     * @throws FieldTypeNotFoundException when no fieldType with the given name exists
     * @throws RepositoryException when an unexpected exception occurs on the repository
     */
    FieldType getFieldTypeByName(QName name) throws FieldTypeNotFoundException, TypeException, InterruptedException;

}
