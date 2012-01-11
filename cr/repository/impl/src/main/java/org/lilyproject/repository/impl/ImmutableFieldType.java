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

import javax.naming.OperationNotSupportedException;

import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.ValueType;

/**
 * Immutable FieldType implementation
 */
public class ImmutableFieldType extends FieldTypeImpl implements FieldType {

    public ImmutableFieldType(SchemaId id, ValueType valueType, QName name, Scope scope) {
        super(id, valueType, name, scope);
    }

    public FieldType immutable() {
        return this;
    }

    @Override
    public void setId(SchemaId id) {
        throw new UnsupportedOperationException("This is an immutable implementation of FieldType");
    }

    @Override
    public void setName(QName name) {
        throw new UnsupportedOperationException("This is an immutable implementation of FieldType");
    }

    @Override
    public void setScope(Scope scope) {
        throw new UnsupportedOperationException("This is an immutable implementation of FieldType");
    }

    @Override
    public void setValueType(ValueType valueType) {
        throw new UnsupportedOperationException("This is an immutable implementation of FieldType");
    }
}
