package org.lilyproject.repository.api;

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

    /**
     * Gets a FieldType from the FieldTypes.
     * <p>
     * Does not throw a FieldTypeNotFoundException when the field type is not
     * known, but returns null instead.
     * 
     * @return the field type or null if not known
     */
    FieldType getFieldTypeByNameReturnNull(QName name);

    /**
     * Checks if a field type is known by the FieldTypes
     * 
     * @return true if the field type is known
     */
    boolean fieldTypeExists(QName name);


}
