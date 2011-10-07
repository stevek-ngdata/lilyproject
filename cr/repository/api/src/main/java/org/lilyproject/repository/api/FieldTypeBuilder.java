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
