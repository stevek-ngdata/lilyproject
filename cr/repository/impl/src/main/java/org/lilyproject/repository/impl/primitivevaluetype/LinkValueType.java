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
import org.lilyproject.repository.api.*;

/**
 *
 */
public class LinkValueType extends AbstractValueType implements ValueType {
    
    public final static String NAME = "LINK";
    private final IdGenerator idGenerator;
    private static TypeManager typeManager;

    public LinkValueType(IdGenerator idGenerator) {
        this.idGenerator = idGenerator;
    }
    
    public String getName() {
        return NAME;
    }
    
    public ValueType getBaseValueType() {
        return this;
    }

    @SuppressWarnings("unchecked")
    public Link read(DataInput dataInput, Repository repository) {
        // Read the encoding version byte, but ignore it for the moment since there is only one encoding
        dataInput.readByte();
        return Link.read(dataInput, idGenerator);
    }

    public void write(Object value, DataOutput dataOutput) {
        dataOutput.writeByte((byte)1); // Encoding version 1
        ((Link)value).write(dataOutput);
    }

    public Class getType() {
        return RecordId.class;
    }

    @Override
    public Comparator getComparator() {
        return null;
    }

    //
    // Factory
    //
    public static ValueTypeFactory factory(IdGenerator idGenerator) {
        return new LinkValueTypeFactory(idGenerator);
    }
    
    public static class LinkValueTypeFactory implements ValueTypeFactory {
        private static LinkValueType instance;

        LinkValueTypeFactory(IdGenerator idGenerator){
            instance = new LinkValueType(idGenerator);
        }
        
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
