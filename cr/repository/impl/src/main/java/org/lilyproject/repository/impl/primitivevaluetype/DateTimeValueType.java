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

import org.joda.time.DateTime;
import org.lilyproject.bytes.api.DataInput;
import org.lilyproject.bytes.api.DataOutput;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.ValueType;
import org.lilyproject.repository.api.ValueTypeFactory;

public class DateTimeValueType extends AbstractValueType implements ValueType {

    public final static String NAME = "DATETIME";

    private static final Comparator<DateTime> COMPARATOR = new Comparator<DateTime>() {
        @Override
        public int compare(DateTime o1, DateTime o2) {
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
    public DateTime read(DataInput dataInput, Repository repository) {
        // Read the encoding version byte, but ignore it for the moment since there is only one encoding
        dataInput.readByte();
        return new DateTime(dataInput.readLong());
    }

    public void write(Object value, DataOutput dataOutput) {
        dataOutput.writeByte((byte)1); // Encoding version 1
        // Currently we only store the millis, not the chronology.
        dataOutput.writeLong(((DateTime)value).getMillis());
    }

    public Class getType() {
        return DateTime.class;
    }

    @Override
    public Comparator getComparator() {
        return COMPARATOR;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + NAME.hashCode();
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
        return true;
    }

    //
    // Factory
    //
    public static ValueTypeFactory factory() {
        return new DateTimeValueTypeFactory();
    }
    
    public static class DateTimeValueTypeFactory implements ValueTypeFactory {
        private static DateTimeValueType instance = new DateTimeValueType();
        
        @Override
        public ValueType getValueType(String typeParams) {
            return instance;
        }
    }
}
