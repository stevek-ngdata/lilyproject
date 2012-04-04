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
package org.lilyproject.repository.impl;

import org.apache.hadoop.hbase.util.Bytes;
import org.lilyproject.repository.api.*;
import org.lilyproject.util.hbase.LilyHBaseSchema.RecordColumn;

public class FieldTypeImpl implements FieldType, Cloneable {

    private SchemaId id;
    private byte[] idQualifier; // Storing throug lazy initialization to avoid to re-create it each time.
    private ValueType valueType;
    private QName name;
    private Scope scope;

    /**
     * This constructor should not be called directly.
     * @use {@link TypeManager#newFieldType} instead
     */
    public FieldTypeImpl(SchemaId id, ValueType valueType, QName name, Scope scope) {
        this.id = id;
        this.valueType = valueType;
        this.name = name;
        this.scope = scope;
    }

    private FieldTypeImpl(){}
    
    @Override
    public QName getName() {
        return name;
    }

    @Override
    public SchemaId getId() {
        return id;
    }
    
    public byte[] getQualifier() {
        if (idQualifier == null) {
            this.idQualifier = Bytes.add(new byte[]{RecordColumn.DATA_PREFIX}, id.getBytes());
        }
        return idQualifier;
    }

    @Override
    public ValueType getValueType() {
        return valueType;
    }

    @Override
    public Scope getScope() {
        return scope;
    }

    @Override
    public void setId(SchemaId id) {
        this.id = id;
    }
    
    @Override
    public void setName(QName name) {
        this.name = name;
    }

    @Override
    public void setValueType(ValueType valueType) {
        this.valueType = valueType;
    }

    @Override
    public void setScope(Scope scope) {
        this.scope = scope;
    }
    
    @Override
    public FieldType clone() {
        FieldTypeImpl newFieldType = new FieldTypeImpl();
        newFieldType.id = this.id;
        newFieldType.idQualifier = this.idQualifier;
        newFieldType.valueType = this.valueType;
        newFieldType.name = this.name;
        newFieldType.scope = this.scope;
        return newFieldType;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((valueType == null) ? 0 : valueType.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        FieldTypeImpl other = (FieldTypeImpl) obj;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        if (valueType == null) {
            if (other.valueType != null)
                return false;
        } else if (!valueType.equals(other.valueType))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "FieldTypeImpl [id=" + id + ", name=" + name
                        + ", valueType=" + valueType + "]";
    }
}
