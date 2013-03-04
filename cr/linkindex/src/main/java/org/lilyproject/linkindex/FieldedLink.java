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
package org.lilyproject.linkindex;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.lilyproject.repository.api.AbsoluteRecordId;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.SchemaId;

/**
 * A link to some record occurring in some field.
 */
public class FieldedLink {
    private final AbsoluteRecordId absRecordId;
    private final SchemaId fieldTypeId;
    private final int hash;
    
    public FieldedLink(AbsoluteRecordId absRecordId, SchemaId fieldTypeId) {
        this.absRecordId = absRecordId;
        this.fieldTypeId = fieldTypeId;
        int hash = 17;
        hash = 37 * hash + absRecordId.toString().hashCode();
        hash = 37 * hash + fieldTypeId.hashCode();
        this.hash = hash;
    }
    
    
    public AbsoluteRecordId getAbsoluteRecordId() {
        return absRecordId;
    }

    public RecordId getRecordId() {
        return absRecordId.getRecordId();
    }

    public SchemaId getFieldTypeId() {
        return fieldTypeId;
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof FieldedLink) {
            FieldedLink otherLink = (FieldedLink)obj;
            return absRecordId.equals(otherLink.absRecordId) && fieldTypeId.equals(otherLink.fieldTypeId);
        }
        return false;
    }
    
    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
}
