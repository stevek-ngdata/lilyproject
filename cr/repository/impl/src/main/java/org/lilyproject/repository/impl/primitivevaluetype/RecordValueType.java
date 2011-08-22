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

import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.util.Bytes.ByteArrayComparator;
import org.lilyproject.bytes.api.DataInput;
import org.lilyproject.bytes.api.DataOutput;
import org.lilyproject.repository.api.*;
import org.lilyproject.repository.impl.SchemaIdImpl;

public class RecordValueType extends AbstractValueType implements ValueType {

    private static final byte ENCODING_VERSION = (byte)1;
    private static final byte UNDEFINED = (byte)0;
    private static final byte DEFINED = (byte)1;
    public static final String NAME = "RECORD";
    private final ByteArrayComparator byteArrayComparator = new ByteArrayComparator();
    private final TypeManager typeManager;
    private String recordTypeName;
    private QName recordTypeQName;

    public RecordValueType(TypeManager typeManager, String recordTypeName) throws IllegalArgumentException, RepositoryException, InterruptedException {
        this.typeManager = typeManager;
        this.recordTypeName = recordTypeName;
        if (recordTypeName != null)
            this.recordTypeQName = QName.fromString(recordTypeName);
    }
    
    public String getName() {
        return NAME;
    }
    
    public String getTypeParams() {
        return recordTypeName;
    }
    
    public ValueType getBaseValueType() {
        return this;
    }
    
    @SuppressWarnings("unchecked")
    public Record read(DataInput dataInput, Repository repository) throws RepositoryException, InterruptedException {
        RecordBuilder recordBuilder = repository.recordBuilder();
        dataInput.readByte(); // Ignore, there is currently only one encoding : 1
        int length = dataInput.readVInt();
        byte[] recordTypeId = dataInput.readBytes(length);
        Long recordTypeVersion = dataInput.readLong();
        RecordType recordType = typeManager.getRecordTypeById(new SchemaIdImpl(recordTypeId), recordTypeVersion);
        recordBuilder.recordType(recordType.getName(), recordTypeVersion);
        List<FieldType> fieldTypes = getSortedFieldTypes(recordType);
        for (FieldType fieldType : fieldTypes) {
            if (DEFINED == dataInput.readByte()) {
                Object value = fieldType.getValueType().read(dataInput, repository);
                recordBuilder.field(fieldType.getName(), value);
            }
        }
        return recordBuilder.newRecord();
    }
    
    public void write(Object value, DataOutput dataOutput) throws RepositoryException, InterruptedException {
        Record record = (Record)value;
        
        // Validate the record type
        RecordType recordType = null;
        if (record.getRecordTypeName() != null && recordTypeName != null && !recordTypeQName.equals(record.getRecordTypeName()))
            throw new RecordException("The record's Record Type '"+record.getRecordTypeName()+"' does not match the record value type's record type '" + recordTypeName + "'");
        if (record.getRecordTypeName() != null) {
            recordType = typeManager.getRecordTypeByName(record.getRecordTypeName(), record.getVersion());
        } else  if (recordTypeName != null) {
            recordType = typeManager.getRecordTypeByName(recordTypeQName, null);
        } else {
            throw new RecordException("The record '" + record + "' should specify a record type");
        }
        
        // Get and sort the field type entries that should be in the record
        List<FieldType> fieldTypes = getSortedFieldTypes(recordType);
        
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
        Map<QName, Object> recordFields = record.getFields();
        // Taking a copy since we don't want to manipulate the fields of the record itself.
        List<QName> givenFields = new ArrayList<QName>();
        for (QName qName : recordFields.keySet()) {
            givenFields.add(qName);
        }
        for (FieldType fieldType : fieldTypes) {
            Object fieldValue = recordFields.get(fieldType.getName());
            if (fieldValue == null) 
                dataOutput.writeByte(UNDEFINED);
            else {
                dataOutput.writeByte(DEFINED);
                fieldType.getValueType().write(fieldValue, dataOutput);
                givenFields.remove(fieldType.getName());
            }
        }
        if (!givenFields.isEmpty())
            throw new InvalidRecordException("Record contains fields not part of the record type '" + recordType + "'", record.getId());
    }

    private List<FieldType> getSortedFieldTypes(RecordType recordType) throws RepositoryException,
            InterruptedException {
        Collection<FieldTypeEntry> fieldTypeEntries = getFieldTypeEntries(recordType);
        List<FieldType> fieldTypes = new ArrayList<FieldType>();
        for (FieldTypeEntry fieldTypeEntry : fieldTypeEntries) {
            fieldTypes.add(typeManager.getFieldTypeById(fieldTypeEntry.getFieldTypeId()));
        }
        Collections.sort(fieldTypes, new Comparator<FieldType>() {
            @Override
            public int compare(FieldType fieldType1, FieldType fieldType2) {
                return byteArrayComparator.compare(fieldType1.getId().getBytes(), fieldType2.getId().getBytes());
            }
        });
        return fieldTypes;
    }
    
    private Collection<FieldTypeEntry> getFieldTypeEntries(RecordType recordType) throws RepositoryException, InterruptedException {
        Collection<FieldTypeEntry> fieldTypeEntries = recordType.getFieldTypeEntries();
        Map<SchemaId, Long> mixins = recordType.getMixins();
        for (Entry<SchemaId, Long> mixinEntry: mixins.entrySet()) {
            fieldTypeEntries.addAll(getFieldTypeEntries(typeManager.getRecordTypeById(mixinEntry.getKey(), mixinEntry.getValue())));
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
        public ValueType getValueType(String recordName) throws IllegalArgumentException, RepositoryException, InterruptedException {
            return new RecordValueType(typeManager, recordName);
        }
    }
}
