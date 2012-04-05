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
package org.lilyproject.repository.impl;

import org.lilyproject.repository.api.*;

public class FieldTypeBuilderImpl implements FieldTypeBuilder {
    private SchemaId id;
    private QName name;
    private ValueType valueType;
    private Scope scope;
    private TypeManager typeManager;

    public FieldTypeBuilderImpl(TypeManager typeManager) {
        this.typeManager = typeManager;
    }

    @Override
    public FieldTypeBuilder id(SchemaId id) {
        this.id = id;
        return this;
    }

    @Override
    public FieldTypeBuilder name(String namespace, String name) {
        this.name = new QName(namespace, name);
        return this;
    }

    @Override
    public FieldTypeBuilder name(QName name) {
        this.name = name;
        return this;
    }

    @Override
    public FieldTypeBuilder type(String valueType) throws RepositoryException, InterruptedException {
        this.valueType = typeManager.getValueType(valueType);
        return this;
    }

    @Override
    public FieldTypeBuilder type(ValueType valueType) throws RepositoryException, InterruptedException {
        this.valueType = valueType;
        return this;
    }

    @Override
    public FieldTypeBuilder scope(Scope scope) {
        this.scope = scope;
        return this;
    }

    private FieldType buildFieldType() throws RepositoryException, InterruptedException {
        FieldType fieldType = typeManager.newFieldType(valueType, name, scope);
        fieldType.setId(id);
        return fieldType;
    }

    @Override
    public FieldType create() throws RepositoryException, InterruptedException {
        FieldType fieldType = buildFieldType();

        // Apply defaults
        if (fieldType.getValueType() == null) {
            fieldType.setValueType(typeManager.getValueType("STRING"));
        }

        if (fieldType.getScope() == null) {
            fieldType.setScope(Scope.NON_VERSIONED);
        }

        return typeManager.createFieldType(fieldType);
    }

    @Override
    public FieldType createOrUpdate() throws RepositoryException, InterruptedException {
        return typeManager.createOrUpdateFieldType(buildFieldType());
    }

    @Override
    public FieldType update() throws RepositoryException, InterruptedException {
        return typeManager.updateFieldType(buildFieldType());
    }

    @Override
    public FieldType build() throws RepositoryException, InterruptedException {
        return buildFieldType();
    }
}
