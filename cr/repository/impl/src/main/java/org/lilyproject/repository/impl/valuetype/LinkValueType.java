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
import java.util.IdentityHashMap;

import org.lilyproject.bytes.api.DataInput;
import org.lilyproject.bytes.api.DataOutput;
import org.lilyproject.repository.api.*;

public class LinkValueType extends AbstractValueType implements ValueType {
    
    public final static String NAME = "LINK";
    private String fullName;

    private final IdGenerator idGenerator;

    public LinkValueType(IdGenerator idGenerator, TypeManager typeManager, String recordTypeName) throws IllegalArgumentException, RepositoryException, InterruptedException {
        this.idGenerator = idGenerator;
        if (recordTypeName != null) {
            this.fullName = NAME+"<"+recordTypeName+">"; 
        }
        else 
            fullName = NAME;
    }
    
    @Override
    public String getBaseName() {
        return NAME;
    }
    
    @Override
    public String getName() {
        return fullName;
    }
    
    @Override
    public ValueType getDeepestValueType() {
        return this;
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public Link read(DataInput dataInput) {
        // Read the encoding version byte, but ignore it for the moment since there is only one encoding
        dataInput.readByte();
        return Link.read(dataInput, idGenerator);
    }

    @Override
    public void write(Object value, DataOutput dataOutput, IdentityHashMap<Record, Object> parentRecords) {
        // We're not storing any recordType information together with the data
        // The recordType information is only available in the schema
        dataOutput.writeByte((byte)1); // Encoding version 1
        ((Link)value).write(dataOutput);
    }

    @Override
    public Class getType() {
        return RecordId.class;
    }

    @Override
    public Comparator getComparator() {
        return null;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + fullName.hashCode();
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
        return fullName.equals(((LinkValueType) obj).fullName);
    }

    //
    // Factory
    //
    public static ValueTypeFactory factory(IdGenerator idGenerator, TypeManager typeManager) {
        return new LinkValueTypeFactory(idGenerator, typeManager);
    }
    
    public static class LinkValueTypeFactory implements ValueTypeFactory {
        private final TypeManager typeManager;
        private final IdGenerator idGenerator;

        LinkValueTypeFactory(IdGenerator idGenerator, TypeManager typeManager){
            this.idGenerator = idGenerator;
            this.typeManager = typeManager;
        }
        
        @Override
        public ValueType getValueType(String recordTypeName) throws IllegalArgumentException, RepositoryException, InterruptedException {
            return new LinkValueType(idGenerator, typeManager, recordTypeName);
        }
    }
}
