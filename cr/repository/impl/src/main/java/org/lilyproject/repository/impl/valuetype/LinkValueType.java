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
import org.lilyproject.bytes.impl.DataOutputImpl;
import org.lilyproject.repository.api.*;
import org.lilyproject.repository.impl.SchemaIdImpl;

public class LinkValueType extends AbstractValueType implements ValueType {
    
    public final static String NAME = "LINK";

    private static final byte UNDEFINED = (byte)0;
    private static final byte DEFINED = (byte)1;

    private final IdGenerator idGenerator;
    private final TypeManager typeManager;
    private SchemaId recordTypeId = null;

    public LinkValueType(IdGenerator idGenerator, TypeManager typeManager, String recordTypeName) throws IllegalArgumentException, RepositoryException, InterruptedException {
        this.idGenerator = idGenerator;
        this.typeManager = typeManager;
        if (recordTypeName != null)
            this.recordTypeId = typeManager.getRecordTypeByName(QName.fromString(recordTypeName), null).getId();
    }
    
    public LinkValueType(IdGenerator idGenerator, TypeManager typeManager, DataInput dataInput) {
        this.idGenerator = idGenerator;
        this.typeManager = typeManager;
        if (dataInput.readByte() == DEFINED) {
            int length = dataInput.readVInt();
            recordTypeId = new SchemaIdImpl(dataInput.readBytes(length));
        }
    }
    
    public String getName() {
        return NAME;
    }
    
    public String getFullName() throws RepositoryException, InterruptedException {
        if (recordTypeId == null)
            return NAME;
        else {
            return NAME+"<"+typeManager.getRecordTypeById(recordTypeId, null).getName()+">";
        }
    }
    
    public ValueType getBaseValueType() {
        return this;
    }
    
    public void encodeTypeParams(DataOutput dataOutput) {
        if (recordTypeId == null) {
            dataOutput.writeByte(UNDEFINED);
        } else {
            dataOutput.writeByte(DEFINED);
            byte[] idBytes = recordTypeId.getBytes();
            dataOutput.writeVInt(idBytes.length);
            dataOutput.writeBytes(idBytes);
        }
    }
    
    @Override
    public byte[] getTypeParams() {
        DataOutput dataOutput = new DataOutputImpl();
        encodeTypeParams(dataOutput);
        return dataOutput.toByteArray();
    }

    @SuppressWarnings("unchecked")
    public Link read(DataInput dataInput, Repository repository) {
        // Read the encoding version byte, but ignore it for the moment since there is only one encoding
        dataInput.readByte();
        return Link.read(dataInput, idGenerator);
    }

    public void write(Object value, DataOutput dataOutput) {
        // We're not storing any recordType information together with the data
        // The recordType information is only available in the schema
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
        public ValueType getValueType(String recordName) throws IllegalArgumentException, RepositoryException, InterruptedException {
            return new LinkValueType(idGenerator, typeManager, recordName);
        }
        
        @Override
        public ValueType getValueType(DataInput dataInput) {
            return new LinkValueType(idGenerator, typeManager, dataInput);
        }
    }
}
