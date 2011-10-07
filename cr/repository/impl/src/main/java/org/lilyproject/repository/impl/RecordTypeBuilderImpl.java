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
package org.lilyproject.repository.impl;

import org.lilyproject.repository.api.*;

public class RecordTypeBuilderImpl implements RecordTypeBuilder {

    private final TypeManager typeManager;
    private RecordType recordType;
    private String defaultNamespace;
    private Scope defaultScope = Scope.NON_VERSIONED;
    
    public RecordTypeBuilderImpl(TypeManager typeManager) throws TypeException {
        this.typeManager = typeManager;
        recordType = typeManager.newRecordType(null);
    }

    @Override
    public RecordTypeBuilder defaultNamespace(String namespace) {
        this.defaultNamespace = namespace;
        return this;
    }

    @Override
    public RecordTypeBuilder defaultScope(Scope scope) {
        this.defaultScope = scope;
        return this;
    }

    @Override
    public RecordTypeBuilder name(String name) {
        recordType.setName(resolveNamespace(name));
        return this;
    }

    @Override
    public RecordTypeBuilder name(String namespace, String name) {
        recordType.setName(new QName(namespace, name));
        return this;
    }

    @Override
    public RecordTypeBuilder name(QName name) {
        recordType.setName(name);
        return this;
    }

    @Override
    public RecordTypeBuilder id(SchemaId id) {
        recordType.setId(id);
        return this;
    }
    
    @Override
    public RecordTypeBuilder field(SchemaId id, boolean mandatory) {
        recordType.addFieldTypeEntry(id, mandatory);
        return this;
    }

    @Override
    public MixinBuilder mixin() {
        return new MixinBuilderImpl();
    }

    @Override
    public RecordTypeBuilder reset() throws TypeException {
        recordType = typeManager.newRecordType(null);
        return this;
    }

    @Override
    public RecordType build() {
        return recordType;
    }

    @Override
    public RecordType create() throws RepositoryException, InterruptedException {
        return typeManager.createRecordType(recordType);
    }

    @Override
    public RecordType createOrUpdate() throws RepositoryException, InterruptedException {
        return typeManager.createOrUpdateRecordType(recordType);
    }

    @Override
    public RecordType update() throws RepositoryException, InterruptedException {
        return typeManager.updateRecordType(recordType);
    }

    @Override
    public FieldEntryBuilder fieldEntry() {
        return new FieldEntryBuilderImpl();
    }

    private QName resolveNamespace(String name) {
        if (defaultNamespace != null)
            return new QName(defaultNamespace, name);

        QName recordTypeName = recordType.getName();
        if (recordTypeName != null)
            return new QName(recordTypeName.getNamespace(), name);

        throw new IllegalStateException("Namespace could not be resolved for name '" + name +
                "' since no default namespace was given and no record type name is set.");
    }

    public class FieldEntryBuilderImpl implements FieldEntryBuilder {
        private SchemaId id;
        private boolean mandatory;

        @Override
        public FieldEntryBuilder id(SchemaId id) {
            this.id = id;
            return this;
        }

        @Override
        public FieldEntryBuilder name(String name) throws RepositoryException, InterruptedException {
            this.id = typeManager.getFieldTypeByName(resolveNamespace(name)).getId();
            return this;
        }

        @Override
        public FieldEntryBuilder name(String namespace, String name) throws RepositoryException, InterruptedException {
            this.id = typeManager.getFieldTypeByName(new QName(namespace, name)).getId();
            return this;
        }

        @Override
        public FieldEntryBuilder name(QName name) throws RepositoryException, InterruptedException {
            this.id = typeManager.getFieldTypeByName(name).getId();
            return this;
        }

        @Override
        public FieldEntryBuilder use(FieldType fieldType) {
            this.id = fieldType.getId();
            return this;
        }

        @Override
        public FieldEntryBuilder mandatory() {
            mandatory = true;
            return this;
        }

        @Override
        public FieldTypeBuilder defineField() {
            return new FieldTypeBuilderImpl();
        }

        @Override
        public RecordTypeBuilder add() {
            recordType.addFieldTypeEntry(id, mandatory);
            return RecordTypeBuilderImpl.this;
        }

        public class FieldTypeBuilderImpl implements RecordTypeBuilder.FieldTypeBuilder {
            private QName name;
            private ValueType valueType;
            private Scope scope;

            @Override
            public FieldTypeBuilder name(String name) {
                this.name = resolveNamespace(name);
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
                if (name == null) {
                    throw new IllegalStateException("Cannot create field type: name not set.");
                }

                // Apply defaults
                if (valueType == null) {
                    valueType = typeManager.getValueType("STRING");
                }
                if (scope == null) {
                    scope = defaultScope;
                }

                return typeManager.newFieldType(valueType, name, scope);
            }

            @Override
            public FieldEntryBuilder create() throws RepositoryException, InterruptedException {
                FieldType fieldType = typeManager.createFieldType(buildFieldType());
                FieldEntryBuilderImpl.this.id = fieldType.getId();
                return FieldEntryBuilderImpl.this;
            }

            @Override
            public FieldEntryBuilder createOrUpdate() throws RepositoryException, InterruptedException {
                FieldType fieldType = typeManager.createOrUpdateFieldType(buildFieldType());
                FieldEntryBuilderImpl.this.id = fieldType.getId();
                return FieldEntryBuilderImpl.this;
            }
        }
    }

    public class MixinBuilderImpl implements MixinBuilder {
        private SchemaId id;
        private Long version;

        @Override
        public MixinBuilder id(SchemaId id) {
            this.id = id;
            return this;
        }

        @Override
        public MixinBuilder name(String name) throws RepositoryException, InterruptedException {
            this.id = typeManager.getRecordTypeByName(resolveNamespace(name), null).getId();
            return this;
        }

        @Override
        public MixinBuilder name(String namespace, String name) throws RepositoryException, InterruptedException {
            this.id = typeManager.getRecordTypeByName(new QName(namespace, name), null).getId();
            return this;
        }

        @Override
        public MixinBuilder name(QName name) throws RepositoryException, InterruptedException {
            this.id = typeManager.getRecordTypeByName(name, null).getId();
            return this;
        }

        @Override
        public MixinBuilder version(long version) {
            this.version = version;
            return this;
        }

        @Override
        public MixinBuilder use(RecordType recordType) {
            this.id = recordType.getId();
            this.version = recordType.getVersion();
            return this;
        }

        @Override
        public RecordTypeBuilder add() {
            if (id == null) {
                throw new IllegalStateException("Cannot add mixin: record type not set.");
            }
            recordType.addMixin(id, version);
            return RecordTypeBuilderImpl.this;
        }
    }

}
