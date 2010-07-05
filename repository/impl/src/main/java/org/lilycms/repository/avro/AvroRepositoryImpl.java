package org.lilycms.repository.avro;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.util.Utf8;
import org.lilycms.repository.api.*;

public class AvroRepositoryImpl implements AvroRepository {

    private final Repository repository;
    private AvroConverter converter;

    public AvroRepositoryImpl(Repository repository, AvroConverter converter) {
        this.repository = repository;
        this.converter = converter;
    }
    
    public AvroRecord create(AvroRecord record) throws AvroRecordExistsException, AvroRecordNotFoundException,
            AvroInvalidRecordException, AvroRecordTypeNotFoundException, AvroFieldTypeNotFoundException,
            AvroRecordException, AvroTypeException {
        try {
            return converter.convert(repository.create(converter.convert(record)));
        } catch (RecordExistsException e) {
            throw converter.convert(e);
        } catch (RecordNotFoundException e) {
            throw converter.convert(e);
        } catch (InvalidRecordException e) {
            throw converter.convert(e);
        } catch (RecordTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (FieldTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (RecordException e) {
            throw converter.convert(e);
        } catch (TypeException e) {
            throw converter.convert(e);
        }
    }

    public Void delete(Utf8 recordId) throws AvroRecordException {
        try {
            repository.delete(repository.getIdGenerator().fromString(recordId.toString()));
        } catch (RecordException e) {
            throw converter.convert(e);
        }
        return null;
    }

    public AvroRecord read(Utf8 recordId, long avroVersion, GenericArray<AvroQName> avroFieldNames)
            throws AvroRecordTypeNotFoundException, AvroFieldTypeNotFoundException, AvroRecordNotFoundException,
            AvroVersionNotFoundException, AvroRecordException, AvroTypeException {
        List<QName> fieldNames = null;
        if (avroFieldNames != null) {
            fieldNames = new ArrayList<QName>();
            for (AvroQName avroQName : avroFieldNames) {
                fieldNames.add(converter.convert(avroQName));
            }
        }
        try {
            Long version = null;
            if (avroVersion != -1) {
                version = avroVersion;
            }
            return converter.convert(repository.read(repository.getIdGenerator().fromString(recordId.toString()), version, fieldNames));
        } catch (RecordNotFoundException e) {
            throw converter.convert(e);
        } catch (RecordTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (FieldTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (VersionNotFoundException e) {
            throw converter.convert(e);
        } catch (RecordException e) {
            throw converter.convert(e);
        } catch (TypeException e) {
            throw converter.convert(e);
        }
    }

    public AvroRecord update(AvroRecord record) throws AvroRecordNotFoundException, AvroInvalidRecordException,
            AvroRecordTypeNotFoundException, AvroFieldTypeNotFoundException, AvroVersionNotFoundException,
            AvroRecordException, AvroTypeException {
        try {
            return converter.convert(repository.update(converter.convert(record)));
        } catch (RecordNotFoundException e) {
            throw converter.convert(e);
        } catch (InvalidRecordException e) {
            throw converter.convert(e);
        } catch (RecordTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (FieldTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (VersionNotFoundException e) {
            throw converter.convert(e);
        } catch (RecordException e) {
            throw converter.convert(e);
        } catch (TypeException e) {
            throw converter.convert(e);
        }
    }

    public AvroRecord updateMutableFields(AvroRecord record) throws AvroRecordNotFoundException,
            AvroInvalidRecordException, AvroRecordTypeNotFoundException, AvroFieldTypeNotFoundException,
            AvroVersionNotFoundException, AvroRecordException, AvroTypeException {
        try {
            return converter.convert(repository.updateMutableFields(converter.convert(record)));
        } catch (InvalidRecordException e) {
            throw converter.convert(e);
        } catch (RecordNotFoundException e) {
            throw converter.convert(e);
        } catch (RecordTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (FieldTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (VersionNotFoundException e) {
            throw converter.convert(e);
        } catch (RecordException e) {
            throw converter.convert(e);
        } catch (TypeException e) {
            throw converter.convert(e);
        }
    }

}
