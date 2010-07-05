package org.lilycms.repository.avro;

import org.apache.avro.util.Utf8;
import org.lilycms.repository.api.*;

public class AvroTypeManagerImpl implements AvroTypeManager {

    private final TypeManager typeManager;
    private AvroConverter converter;

    public AvroTypeManagerImpl(TypeManager serverTypeManager, AvroConverter converter) {
        this.typeManager = serverTypeManager;
        this.converter = converter;
    }
    
    public AvroFieldType createFieldType(AvroFieldType avroFieldType)
            throws AvroFieldTypeExistsException, AvroTypeException {

        try {
            return converter.convert(typeManager.createFieldType(converter.convert(avroFieldType)));
        } catch (FieldTypeExistsException e) {
            throw converter.convert(e);
        } catch (TypeException e) {
            throw converter.convert(e);
        }
    }

    public AvroRecordType createRecordType(AvroRecordType avroRecordType) throws AvroRecordTypeExistsException,
            AvroRecordTypeNotFoundException, AvroFieldTypeNotFoundException, AvroTypeException {

        try {
            return converter.convert(typeManager.createRecordType(converter.convert(avroRecordType)));
        } catch (RecordTypeExistsException e) {
            throw converter.convert(e);
        } catch (RecordTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (FieldTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (TypeException e) {
            throw converter.convert(e);
        }
    }

    public AvroRecordType getRecordType(Utf8 id, long avroVersion)
            throws AvroRecordTypeNotFoundException, AvroTypeException {
        try {
            Long version = null;
            if (avroVersion != -1) {
                version = avroVersion;
            }
            return converter.convert(typeManager.getRecordType(id.toString(), version));
        } catch (RecordTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (TypeException e) {
            throw converter.convert(e);
        }
    }

    public AvroRecordType updateRecordType(AvroRecordType recordType)
            throws AvroRecordTypeNotFoundException, AvroFieldTypeNotFoundException, AvroTypeException {

        try {
            return converter.convert(typeManager.updateRecordType(converter.convert(recordType)));
        } catch (RecordTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (FieldTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (TypeException e) {
            throw converter.convert(e);
        }
    }

    public AvroFieldType updateFieldType(AvroFieldType fieldType)
            throws AvroFieldTypeNotFoundException, AvroFieldTypeUpdateException, AvroTypeException {

        try {
            return converter.convert(typeManager.updateFieldType(converter.convert(fieldType)));
        } catch (FieldTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (FieldTypeUpdateException e) {
            throw converter.convert(e);
        } catch (TypeException e) {
            throw converter.convert(e);
        }
    }

    public AvroFieldType getFieldTypeById(Utf8 id) throws AvroFieldTypeNotFoundException, AvroTypeException {
        try {
            return converter.convert(typeManager.getFieldTypeById(id.toString()));
        } catch (FieldTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (TypeException e) {
            throw converter.convert(e);
        }
    }

    public AvroFieldType getFieldTypeByName(AvroQName name) throws AvroFieldTypeNotFoundException, AvroTypeException {
        try {
            return converter.convert(typeManager.getFieldTypeByName(converter.convert(name)));
        } catch (FieldTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (TypeException e) {
            throw converter.convert(e);
        }
    }
}
