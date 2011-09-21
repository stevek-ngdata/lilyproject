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
package org.lilyproject.repository.impl.valuetype;

import java.util.Comparator;

import org.lilyproject.bytes.api.DataInput;
import org.lilyproject.bytes.api.DataOutput;
import org.lilyproject.repository.api.ValueType;
import org.lilyproject.repository.api.ValueTypeFactory;

public class IntegerValueType extends AbstractValueType implements ValueType {

    public static final String NAME = "INTEGER";

    private static final Comparator<Integer> COMPARATOR = new Comparator<Integer>() {
        @Override
        public int compare(Integer o1, Integer o2) {
            return o1.compareTo(o2);
        }
    };

    public String getSimpleName() {
        return NAME;
    }

    public ValueType getBaseValueType() {
        return this;
    }

    @SuppressWarnings("unchecked")
    public Integer read(DataInput dataInput) {
        return dataInput.readInt();
    }

    public void write(Object value, DataOutput dataOutput) {
        dataOutput.writeInt((Integer)value);
    }

    public Class getType() {
        return Integer.class;
    }

    @Override
    public Comparator getComparator() {
        return COMPARATOR;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((NAME == null) ? 0 : NAME.hashCode());
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
        IntegerValueType other = (IntegerValueType) obj;
        if (NAME == null) {
            if (other.NAME != null)
                return false;
        } else if (!NAME.equals(other.NAME))
            return false;
        return true;
    }

    //
    // Factory
    //
    public static ValueTypeFactory factory() {
        return new IntegerValueTypeFactory();
    }
    
    public static class IntegerValueTypeFactory implements ValueTypeFactory {
        private static IntegerValueType instance = new IntegerValueType();
        
        @Override
        public ValueType getValueType(String typeParams) {
            return instance;
        }
        
        @Override
        public ValueType getValueType(DataInput dataInput) {
            return instance;
        }
    }
}
