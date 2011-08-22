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
package org.lilyproject.repository.impl.primitivevaluetype;

import java.util.Comparator;

import org.lilyproject.bytes.api.DataInput;
import org.lilyproject.bytes.api.DataOutput;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.ValueType;
import org.lilyproject.repository.api.ValueTypeFactory;

public class DoubleValueType extends AbstractValueType implements ValueType {

    public final static String NAME = "DOUBLE";

    private static final Comparator<Double> COMPARATOR = new Comparator<Double>() {
        @Override
        public int compare(Double o1, Double o2) {
            return o1.compareTo(o2);
        }
    };

    public String getName() {
        return NAME;
    }
    
    public ValueType getBaseValueType() {
        return this;
    }

    @SuppressWarnings("unchecked")
    public Double read(DataInput dataInput, Repository repository) {
        return dataInput.readDouble();
    }

    public void write(Object value, DataOutput dataOutput) {
        dataOutput.writeDouble((Double)value);
    }

    public Class getType() {
        return Long.class;
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
        DoubleValueType other = (DoubleValueType) obj;
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
        return new DoubleValueTypeFactory();
    }
    
    public static class DoubleValueTypeFactory implements ValueTypeFactory {
        private static DoubleValueType instance = new DoubleValueType();
        
        @Override
        public ValueType getValueType(String typeParams) {
            return instance;
        }
    }
}
