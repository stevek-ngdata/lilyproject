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
package org.lilycms.repository.impl;

import java.util.Arrays;

import org.apache.hadoop.hbase.util.Bytes;
import org.lilycms.repository.api.Field;

public class FieldImpl implements Field {
    private String fieldId;
    private byte[] value;

    /**
     * This constructor should not be called directly.
     * @use {@link Repository#newField} instead
     */
    public FieldImpl(String fieldId, byte[] value) {
        this.fieldId = fieldId;
        this.value = value;
    }
    
    public String getId() {
        return fieldId;
    }
    
    public byte[] getValue() {
        return value;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((fieldId == null) ? 0 : fieldId.hashCode());
        result = prime * result + Arrays.hashCode(value);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        FieldImpl other = (FieldImpl) obj;
        if (fieldId == null) {
            if (other.fieldId != null)
                return false;
        } else if (!fieldId.equals(other.fieldId))
            return false;
        if (!Arrays.equals(value, other.value))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "["+fieldId+","+ Bytes.toString(value)+"]";
    }
}
