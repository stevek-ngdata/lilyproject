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
package org.lilyproject.repository.impl.primitivevaluetype;

import java.util.HashSet;
import java.util.Set;

import org.lilyproject.bytes.api.DataOutput;
import org.lilyproject.bytes.impl.DataOutputImpl;
import org.lilyproject.repository.api.ValueType;

public abstract class AbstractValueType {

    public abstract void write(Object value, DataOutput dataOutput);
    
    public abstract String getName();
    
    public abstract ValueType getBaseValueType();
    
    public ValueType getNestedValueType() {
        return null;
    }
    
    public byte[] toBytes(Object value) {
        DataOutput dataOutput = new DataOutputImpl();
        write(value, dataOutput);
        return dataOutput.toByteArray();
    }
    
    public String getFullName() {
        return getName();
    }
    
    public int getNestingLevel() {
        return 0;
    }
    
    public Set<Object> getValues(Object value) {
        Set<Object> result = new HashSet<Object>();
        result.add(value);
        return result;
    }
    
    public String getTypeParams() {
        return null;
    }
    
    public boolean isMultiValue() {
        return false;
    }
    
    public boolean isHierarchical() {
        return false;
    }
}
 