package org.lilycms.repository.impl;

import java.io.IOException;
import java.net.SocketAddress;

import org.apache.avro.ipc.AvroRemoteException;
import org.apache.avro.ipc.SocketTransceiver;
import org.apache.avro.specific.SpecificRequestor;
import org.apache.avro.util.Utf8;
import org.lilycms.repository.api.*;
import org.lilycms.repository.api.FieldTypeExistsException;
import org.lilycms.repository.api.FieldTypeNotFoundException;
import org.lilycms.repository.api.FieldTypeUpdateException;
import org.lilycms.repository.api.RecordTypeExistsException;
import org.lilycms.repository.api.RecordTypeNotFoundException;
import org.lilycms.repository.api.RepositoryException;
import org.lilycms.repository.avro.AvroConverter;
import org.lilycms.repository.avro.AvroFieldType;
import org.lilycms.repository.avro.AvroFieldTypeExistsException;
import org.lilycms.repository.avro.AvroFieldTypeNotFoundException;
import org.lilycms.repository.avro.AvroFieldTypeUpdateException;
import org.lilycms.repository.avro.AvroRecordTypeExistsException;
import org.lilycms.repository.avro.AvroRecordTypeNotFoundException;
import org.lilycms.repository.avro.AvroRepositoryException;
import org.lilycms.repository.avro.AvroTypeManager;

public class TypeManagerRemoteImpl extends AbstractTypeManager implements TypeManager {

    private AvroTypeManager typeManagerProxy;
    private AvroConverter converter;

    public TypeManagerRemoteImpl(SocketAddress address, AvroConverter converter, IdGenerator idGenerator)
            throws IOException {
        this.converter = converter;
        //TODO idGenerator should not be available or used in the remote implementation
        this.idGenerator = idGenerator;
        SocketTransceiver client = new SocketTransceiver(address);

        typeManagerProxy = (AvroTypeManager) SpecificRequestor.getClient(AvroTypeManager.class, client);
        initialize();
    }
    
    public RecordType createRecordType(RecordType recordType)
            throws RecordTypeExistsException, RecordTypeNotFoundException,
            FieldTypeNotFoundException, RepositoryException {
        try {
            return converter.convert(typeManagerProxy.createRecordType(converter.convert(recordType)));
        } catch (AvroRepositoryException repositoryException) {
            throw converter.convert(repositoryException);
        } catch (AvroRecordTypeExistsException recordTypeExistsException) {
            throw converter.convert(recordTypeExistsException);
        } catch (AvroRecordTypeNotFoundException recordTypeNotFoundException) {
            throw converter.convert(recordTypeNotFoundException);
        } catch (AvroFieldTypeNotFoundException fieldTypeNotFoundException) {
            throw converter.convert(fieldTypeNotFoundException);
        } catch (AvroRemoteException remoteException) {
            throw converter.convert(remoteException);
        }
    }

    public RecordType getRecordType(String id, Long version)
            throws RecordTypeNotFoundException, RepositoryException {
        try {
            long avroVersion;
            if (version == null) {
                avroVersion = -1;
            } else {
                avroVersion = version;
            }
            return converter.convert(typeManagerProxy.getRecordType(new Utf8(id), avroVersion));
        } catch (AvroRecordTypeNotFoundException recordTypeNotFoundException) {
            throw converter.convert(recordTypeNotFoundException);
        } catch (AvroRepositoryException repositoryException) {
            throw converter.convert(repositoryException);
        } catch (AvroRemoteException remoteException) {
            throw converter.convert(remoteException);
        }
    }

    public RecordType updateRecordType(RecordType recordType)
            throws RecordTypeNotFoundException, FieldTypeNotFoundException,
            RepositoryException {
        try {
            return converter.convert(typeManagerProxy
                    .updateRecordType(converter.convert(recordType)));
        } catch (AvroRecordTypeNotFoundException recordTypeNotFoundException) {
            throw converter.convert(recordTypeNotFoundException);
        } catch (AvroFieldTypeNotFoundException fieldTypeNotFoundExeption) {
            throw converter.convert(fieldTypeNotFoundExeption);
        } catch (AvroRepositoryException repositoryException) {
            throw converter.convert(repositoryException);
        } catch (AvroRemoteException remoteException) {
            throw converter.convert(remoteException);
        }
    }

    public FieldType createFieldType(FieldType fieldType)
            throws FieldTypeExistsException, RepositoryException {
        try {
            AvroFieldType avroFieldType = converter.convert(fieldType);
            AvroFieldType createFieldType = typeManagerProxy.createFieldType(avroFieldType);
            FieldType resultFieldType = converter.convert(createFieldType);
            return resultFieldType;
        } catch (AvroRepositoryException repositoryException) {
            throw converter.convert(repositoryException);
        } catch (AvroFieldTypeExistsException fieldTypeExistsException) {
            throw converter.convert(fieldTypeExistsException);
        } catch (AvroRemoteException remoteException) {
            throw converter.convert(remoteException);
        }
    }

    public FieldType updateFieldType(FieldType fieldType)
            throws FieldTypeNotFoundException, FieldTypeUpdateException,
            RepositoryException {
        try {
            return converter.convert(typeManagerProxy.updateFieldType(converter.convert(fieldType)));
        } catch (AvroFieldTypeNotFoundException fieldTypeNotFoundException) {
            throw converter.convert(fieldTypeNotFoundException);
        } catch (AvroFieldTypeUpdateException fieldTypeUpdateException) {
            throw converter.convert(fieldTypeUpdateException);
        } catch (AvroRepositoryException repositoryException) {
            throw converter.convert(repositoryException);
        } catch (AvroRemoteException remoteException) {
            throw converter.convert(remoteException);
        }
    }

    public FieldType getFieldTypeById(String id) throws FieldTypeNotFoundException, RepositoryException {
        try {
            return converter.convert(typeManagerProxy.getFieldTypeById(new Utf8(id)));
        } catch (AvroFieldTypeNotFoundException fieldTypeNotFoundException) {
            throw converter.convert(fieldTypeNotFoundException);
        } catch (AvroRepositoryException repositoryException) {
            throw converter.convert(repositoryException);
        } catch (AvroRemoteException remoteException) {
            throw converter.convert(remoteException);
        }
    }

    public FieldType getFieldTypeByName(QName name) throws FieldTypeNotFoundException {
        try {
            return converter.convert(typeManagerProxy.getFieldTypeByName(converter.convert(name)));
        } catch (AvroRemoteException remoteException) {
            // TODO define what we do here
            throw new FieldTypeNotFoundException(null, null);
        }
    }

}
