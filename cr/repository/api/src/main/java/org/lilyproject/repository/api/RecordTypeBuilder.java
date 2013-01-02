/*
 * Copyright 2011 Outerthought bvba
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

/**
 * The RecordTypeBuilder is an alternative API for creating or updating record
 * types. It makes use of method-chaining to define a sort of mini internal DSL
 * / fluent API. The intention is to have easier to read and write Java code,
 * functionally it offers nothing more than the core {@link TypeManager} API.
 * <p/>
 * <p>Alternatively you should consider declaring the schema in an external
 * file and importing that, as is provided by Lily's JSON import tool.</p>
 * <p/>
 * <p>A new RecordTypeBuilder object can be obtained by calling {@link TypeManager#recordTypeBuilder()}
 * <p/>
 * <p>Each method either returns a new RecordTypeBuilder object on which the next method can
 * be called, or returns an actual RecordType object.
 * <p/>
 * <p>For a tutorial on using this builder, see the Lily documentation.
 */
public interface RecordTypeBuilder {

    /**
     * Sets the default namespace for all names set via this builder, when not
     * using a full QName. The default namespace can be switched at any time,
     * only affecting later set names.
     */
    RecordTypeBuilder defaultNamespace(String namespace);

    /**
     * Sets the default scope for any fields created via this builder.
     * The default default scope is {@link Scope#NON_VERSIONED}.
     */
    RecordTypeBuilder defaultScope(Scope scope);

    /**
     * Sets the name of the record type, using the namespace set through
     * {@link #defaultNamespace(String)}.
     */
    RecordTypeBuilder name(String name);

    /**
     * Sets the name of the record type.
     */
    RecordTypeBuilder name(String namespace, String name);

    /**
     * Sets the name of the record type.
     */
    RecordTypeBuilder name(QName name);

    /**
     * Sets the id of the record type. This is only relevant when you want
     * to update an existing record type, and even then the ID is only
     * needed if you want to change the name of the record type, or when
     * you prefer to rely on the unchangeable ID to identify the record type.
     */
    RecordTypeBuilder id(SchemaId id);

    /**
     * Adds a field type to the record type.
     * <p/>
     * <p>Alternatively, you can use the dedicated builder provided through
     * {@link #fieldEntry()} which offers more possibilities.
     *
     * @param id        SchemaId of the field type
     * @param mandatory true if it is a mandatory field
     */
    RecordTypeBuilder field(SchemaId id, boolean mandatory);

    /**
     * Gives a builder for adding field type entries, also allowing on-the-fly
     * creation of new field types. When done, call {@link FieldEntryBuilder#add}
     * to return to the record type builder.
     */
    FieldEntryBuilder fieldEntry();

    /**
     * Gives a builder for adding mixins. When done, call {@link MixinBuilder#add()}
     * to return to the record type builder.
     */
    MixinBuilder mixin();

    /**
     * Clears all data from the builder. This allows to reuse the builder object,
     * though you could as well create a new one.
     */
    RecordTypeBuilder reset() throws TypeException;

    /**
     * Creates a new record type on the repository with the properties
     * that were added to the builder.
     * <p/>
     * <p>This will give an exception if a record type with the given
     * name already exists, use {@link #createOrUpdate()} to dynamically
     * switch between create and update.
     *
     * @return the created record type
     * @see {@link TypeManager#createRecordType(RecordType)}
     */
    RecordType create() throws RepositoryException, InterruptedException;

    /**
     * Performs a createOrUpdateRecordType operation on {@link TypeManager}.
     * <p/>
     * <p>This method is interesting in case you don't know if the type </p>
     *
     * @return the created or updated record type
     * @see {@link TypeManager#createOrUpdateRecordType(RecordType)}
     */
    RecordType createOrUpdate() throws RepositoryException, InterruptedException;

    /**
     * Updates a record type on the repository with the properties
     * that were added to the builder..
     *
     * @return the updated record type
     * @see {@link TypeManager#updateRecordType(RecordType)}
     */
    RecordType update() throws RepositoryException, InterruptedException;

    /**
     * Returns a RecordType object containing the properties that were added
     * to the builder without actually creating it on the repository.
     *
     * @return the record type
     */
    RecordType build();

    /**
     * A builder for adding field type entries to a record type. You can identify
     * the field type in different ways: by id, by name, by FieldType object, or
     * by creating a new field type on the fly.
     */
    interface FieldEntryBuilder {
        /**
         * Identify the field type by id, if you use this method you do not
         * have to set the name and vice-versa.
         */
        FieldEntryBuilder id(SchemaId id);

        /**
         * Identify the field type by name, the namespace is taken from
         * the one set using {@link RecordTypeBuilder#defaultNamespace(String)}.
         */
        FieldEntryBuilder name(String name) throws RepositoryException, InterruptedException;

        /**
         * Identify the field type by name.
         */
        FieldEntryBuilder name(String namespace, String name) throws RepositoryException, InterruptedException;

        /**
         * Identify the field type by name.
         */
        FieldEntryBuilder name(QName name) throws RepositoryException, InterruptedException;

        /**
         * Identify the field type by the supplied FieldType object.
         */
        FieldEntryBuilder use(FieldType fieldType);

        /**
         * Gives a builder for creating a new field type. When using this method,
         * afterwards you obviously do *not* have to set the name or id properties
         * anymore.
         */
        RecordTypeBuilder.FieldTypeBuilder defineField();

        /**
         * Sets the mandatory flag on (by default it is off).
         */
        FieldEntryBuilder mandatory();

        /**
         * Adds a new field type entry to the record type and returns the
         * {@link RecordTypeBuilder}.
         */
        RecordTypeBuilder add();
    }

    /**
     * A builder for creating field types on the fly as part of creating
     * record types.
     */
    interface FieldTypeBuilder {
        /**
         * Sets the name for the new field type, the namespace is taken from
         * the one set using {@link RecordTypeBuilder#defaultNamespace(String)}.
         */
        FieldTypeBuilder name(String name);

        /**
         * Sets the name for the new field type.
         */
        FieldTypeBuilder name(QName name);

        /**
         * Sets the name for the new field type.
         */
        FieldTypeBuilder name(String namespace, String name);

        /**
         * Sets the value type for the new field type.
         */
        FieldTypeBuilder type(String valueType) throws RepositoryException, InterruptedException;

        /**
         * Sets the value type for the new field type.
         */
        FieldTypeBuilder type(ValueType valueType) throws RepositoryException, InterruptedException;

        /**
         * Sets the scope for the new field type.
         */
        FieldTypeBuilder scope(Scope scope);

        /**
         * Creates the new field type and returns to the {@link FieldEntryBuilder}.
         */
        FieldEntryBuilder create() throws RepositoryException, InterruptedException;

        /**
         * Creates or updates the new field type and returns to the {@link FieldEntryBuilder}.
         *
         * @see {@link TypeManager#createOrUpdateFieldType(FieldType)}
         */
        FieldEntryBuilder createOrUpdate() throws RepositoryException, InterruptedException;
    }

    /**
     * A builder for adding mixins to a record type. Identify the record type to be
     * mixed by either supplying the id, name or RecordType object. Then optionally
     * set the version. Finally, call {@link #add()} to return to the {@link RecordTypeBuilder}.
     */
    interface MixinBuilder {
        /**
         * Identify the mixin record type by id, if you use this you do not
         * have to use any of the other methods to identify the field type,
         * and vice-versa.
         */
        MixinBuilder id(SchemaId id);

        /**
         * Identify the mixin record type by name, the namespace is taken from
         * the one set using {@link RecordTypeBuilder#defaultNamespace(String)}.
         */
        MixinBuilder name(String name) throws RepositoryException, InterruptedException;

        /**
         * Identify the mixin record type by name.
         */
        MixinBuilder name(String namespace, String name) throws RepositoryException, InterruptedException;

        /**
         * Identify the mixin record type by name.
         */
        MixinBuilder name(QName name) throws RepositoryException, InterruptedException;

        /**
         * Identify the mixin record type by the supplied RecordType object. This will use
         * both the ID and the version from the supplied RecordType.
         */
        MixinBuilder use(RecordType recordType);

        /**
         * Sets the version of the record type. This is optional, by default the current latest
         * version will be used.
         */
        MixinBuilder version(long version);

        /**
         * Adds a new mixin to the record type and returns the {@link RecordTypeBuilder}.
         */
        RecordTypeBuilder add();
    }
}
