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

import java.util.*;
import java.util.Map.Entry;

import org.lilyproject.bytes.api.DataInput;
import org.lilyproject.bytes.api.DataOutput;
import org.lilyproject.bytes.impl.DataOutputImpl;
import org.lilyproject.repository.api.*;
import org.lilyproject.repository.impl.RecordImpl;
import org.lilyproject.repository.impl.RecordRvtImpl;
import org.lilyproject.repository.impl.SchemaIdImpl;

public class RecordValueType extends AbstractValueType implements ValueType {

    public static final String NAME = "RECORD";
    private String fullName;
    
    private static final byte ENCODING_VERSION = (byte)1;
    private static final byte UNDEFINED = (byte)0;
    private static final byte DEFINED = (byte)1;
    private static final byte DEFINED_IDENTICAL = (byte)2;
    
    private final TypeManager typeManager;
    private QName valueTypeRecordTypeName = null;

    public RecordValueType(TypeManager typeManager, String recordTypeName) throws IllegalArgumentException,
            RepositoryException, InterruptedException {
        this.typeManager = typeManager;
        if (recordTypeName != null) {
            this.valueTypeRecordTypeName = QName.fromString(recordTypeName);
            this.fullName = NAME + "<" + recordTypeName + ">";
        } else {
            this.fullName = NAME;
        }
    }
    
    public String getSimpleName() {
        return NAME;
    }
    
    public String getName() {
        return fullName;
    }
    
    public ValueType getBaseValueType() {
        return this;
    }
    
    @SuppressWarnings("unchecked")
    public Record read(byte[] data) throws RepositoryException, InterruptedException {
        return new RecordRvtImpl(data, this);
    }
    
    @SuppressWarnings("unchecked")
    public Record read(DataInput dataInput) throws RepositoryException, InterruptedException {
        Record record = new RecordImpl();
        dataInput.readByte(); // Ignore, there is currently only one encoding : 1
        int length = dataInput.readVInt();
        byte[] recordTypeId = dataInput.readBytes(length);
        Long recordTypeVersion = dataInput.readLong();
        RecordType recordType = typeManager.getRecordTypeById(new SchemaIdImpl(recordTypeId), recordTypeVersion);
        record.setRecordType(recordType.getName(), recordTypeVersion);
        List<FieldType> fieldTypes = getSortedFieldTypes(recordType);
        for (FieldType fieldType : fieldTypes) {
            byte readByte = dataInput.readByte();
            if (DEFINED == readByte) {
                Object value = fieldType.getValueType().read(dataInput);
                record.setField(fieldType.getName(), value);
            } else if (DEFINED_IDENTICAL == readByte) { // The record is nested in itself
                record.setField(fieldType.getName(), record);
            }
        }
        return record;
    }
    
    @Override
    public byte[] toBytes(Object value) throws RepositoryException, InterruptedException {
        if (value instanceof RecordRvtImpl) {
            byte[] bytes = ((RecordRvtImpl)value).getBytes();
            if (bytes != null) 
                return bytes;
        }
        DataOutput dataOutput = new DataOutputImpl();
        encodeData(value, dataOutput);
        return dataOutput.toByteArray();
    }

    public void write(Object value, DataOutput dataOutput) throws RepositoryException, InterruptedException {
        if (value instanceof RecordRvtImpl) {
            byte[] bytes = ((RecordRvtImpl)value).getBytes();
            if (bytes != null) { 
                dataOutput.writeBytes(bytes);
                return;
            }
        }
        encodeData(value, dataOutput);
    }
    
    private void encodeData(Object value, DataOutput dataOutput) throws RepositoryException, InterruptedException {
        Record record = (Record)value;
        
        RecordType recordType;
        QName recordRecordTypeName = record.getRecordTypeName();
        if (recordRecordTypeName != null) {
            if (valueTypeRecordTypeName != null) {
                // Validate the same record type is being used
                if (!valueTypeRecordTypeName.equals(recordRecordTypeName))
                    throw new RecordException("The record's Record Type '"+ recordRecordTypeName +
                            "' does not match the record value type's record type '" + valueTypeRecordTypeName + "'");
            }
            recordType = typeManager.getRecordTypeByName(recordRecordTypeName, null);
        } else if (valueTypeRecordTypeName != null) {
                recordType = typeManager.getRecordTypeByName(valueTypeRecordTypeName, null);
        } else {
            throw new RecordException("The record '" + record + "' should specify a record type");
        }
        
        // Get and sort the field type entries that should be in the record
        List<FieldType> fieldTypes = getSortedFieldTypes(recordType);
        
        Map<QName, Object> recordFields = record.getFields();
        List<QName> expectedFields = new ArrayList<QName>();
        
        // Write the record type information
        // Encoding:
        // - encoding version : byte (1)
        // - nr of bytes in recordtype id : vInt
        // - recordtype id : bytes
        // - recordtype version : long
        dataOutput.writeByte(ENCODING_VERSION);
        byte[] recordIdBytes = recordType.getId().getBytes();
        dataOutput.writeVInt(recordIdBytes.length);
        dataOutput.writeBytes(recordIdBytes);
        dataOutput.writeLong(recordType.getVersion());
        
        // Write the content of the fields
        // Encoding: for each field :
        // - if not present in the record : undefined marker : byte (0)
        // - if present in the record : defined marker : byte (1)
        //      - fieldValue : bytes
        for (FieldType fieldType : fieldTypes) {
            QName name = fieldType.getName();
            expectedFields.add(name);
            Object fieldValue = recordFields.get(name);
            if (fieldValue == null) 
                dataOutput.writeByte(UNDEFINED);
            else if (fieldValue == record) {
                dataOutput.writeByte(DEFINED_IDENTICAL); // The record is nested in itself, avoid recursion
            }
            else {
                dataOutput.writeByte(DEFINED);
                fieldType.getValueType().write(fieldValue, dataOutput);
            }
        }

        // Check if the record does contain fields that are not defined in the record type  
        if (!expectedFields.containsAll(recordFields.keySet())) {
            throw new InvalidRecordException("Record contains fields not part of the record type '" +
                    recordType.getName() + "'", record.getId());
        }
    }

    private List<FieldType> getSortedFieldTypes(RecordType recordType) throws RepositoryException,
            InterruptedException {
        Collection<FieldTypeEntry> fieldTypeEntries = getFieldTypeEntries(recordType);
        List<FieldType> fieldTypes = new ArrayList<FieldType>();
        for (FieldTypeEntry fieldTypeEntry : fieldTypeEntries) {
            fieldTypes.add(typeManager.getFieldTypeById(fieldTypeEntry.getFieldTypeId()));
        }
        return fieldTypes;
    }
    
    private Collection<FieldTypeEntry> getFieldTypeEntries(RecordType recordType)
            throws RepositoryException, InterruptedException {

        Collection<FieldTypeEntry> fieldTypeEntries = recordType.getFieldTypeEntries();
        Map<SchemaId, Long> mixins = recordType.getMixins();
        for (Entry<SchemaId, Long> mixinEntry: mixins.entrySet()) {
            RecordType mixinRecordType = typeManager.getRecordTypeById(mixinEntry.getKey(), mixinEntry.getValue());
            fieldTypeEntries.addAll(getFieldTypeEntries(mixinRecordType));
        }
        return fieldTypeEntries;
    }

    public Class getType() {
        return Record.class;
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
        return fullName.equals(((RecordValueType) obj).fullName);
    }

    //
    // Factory
    //
    public static ValueTypeFactory factory(TypeManager typeManager) {
        return new RecordValueTypeFactory(typeManager);
    }
    
    public static class RecordValueTypeFactory implements ValueTypeFactory {
        private TypeManager typeManager;
        
        public RecordValueTypeFactory(TypeManager typeManager) {
            this.typeManager = typeManager;
        }
        
        @Override
        public ValueType getValueType(String recordName) throws IllegalArgumentException, RepositoryException,
                InterruptedException {
            return new RecordValueType(typeManager, recordName);
        }
    }
}
