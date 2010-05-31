package org.lilycms.repository.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.ipc.AvroRemoteException;
import org.apache.avro.ipc.SocketTransceiver;
import org.apache.avro.specific.SpecificRequestor;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang.NotImplementedException;
import org.lilycms.repository.api.*;
import org.lilycms.repository.api.BlobNotFoundException;
import org.lilycms.repository.api.FieldTypeNotFoundException;
import org.lilycms.repository.api.InvalidRecordException;
import org.lilycms.repository.api.RecordExistsException;
import org.lilycms.repository.api.RecordNotFoundException;
import org.lilycms.repository.api.RecordTypeNotFoundException;
import org.lilycms.repository.api.RepositoryException;
import org.lilycms.repository.avro.AvroConverter;
import org.lilycms.repository.avro.AvroFieldTypeNotFoundException;
import org.lilycms.repository.avro.AvroInvalidRecordException;
import org.lilycms.repository.avro.AvroQName;
import org.lilycms.repository.avro.AvroRecordExistsException;
import org.lilycms.repository.avro.AvroRecordNotFoundException;
import org.lilycms.repository.avro.AvroRecordTypeNotFoundException;
import org.lilycms.repository.avro.AvroRepository;
import org.lilycms.repository.avro.AvroRepositoryException;
import org.lilycms.util.ArgumentValidator;

public class RepositoryRemoteImpl implements Repository {

	
	private AvroRepository repositoryProxy;
	private final AvroConverter converter;
	private IdGenerator idGenerator;
	private final TypeManager typeManager;

	public RepositoryRemoteImpl(SocketAddress address, AvroConverter converter, TypeManagerRemoteImpl typeManager, IdGenerator idGenerator)
			throws IOException {
		this.converter = converter;
		this.typeManager = typeManager;
		//TODO idGenerator should not be available or used in the remote implementation
		this.idGenerator = idGenerator;
		SocketTransceiver client = new SocketTransceiver(address);

		repositoryProxy = (AvroRepository) SpecificRequestor.getClient(
				AvroRepository.class, client);
	}
	
	public TypeManager getTypeManager() {
		return typeManager;
	}
	
	public Record newRecord() {
        return new RecordImpl();
    }

    public Record newRecord(RecordId recordId) {
        ArgumentValidator.notNull(recordId, "recordId");
        return new RecordImpl(recordId);
    }
	
	public IdGenerator getIdGenerator() {
		return idGenerator;
	}
	
	public Record create(Record record) throws RecordExistsException, RecordNotFoundException, InvalidRecordException, RecordTypeNotFoundException, FieldTypeNotFoundException,
	        RepositoryException {
		try {
	        return converter.convert(repositoryProxy.create(converter.convert(record)));
        } catch (AvroRecordExistsException recordExistsException) {
        	throw converter.convert(recordExistsException);
        } catch (AvroRecordNotFoundException recordNotFoundException) {
        	throw converter.convert(recordNotFoundException);
        } catch (AvroInvalidRecordException invalidRecordException) {
        	throw converter.convert(invalidRecordException);
        } catch (AvroRecordTypeNotFoundException recordTypeNotFoundException) {
        	throw converter.convert(recordTypeNotFoundException);
        } catch (AvroFieldTypeNotFoundException fieldTypeNotFoundException) {
        	throw converter.convert(fieldTypeNotFoundException);
        } catch (AvroRepositoryException repositoryException) {
        	throw converter.convert(repositoryException);
        } catch (AvroRemoteException remoteException) {
        	throw converter.convert(remoteException);
        }
	}

	public void delete(RecordId recordId) throws RepositoryException {
		try {
	        repositoryProxy.delete(new Utf8(recordId.toString()));
        } catch (AvroRemoteException remoteException) {
        	throw converter.convert(remoteException);
        }

	}

	public Record read(RecordId recordId) throws RecordNotFoundException, RecordTypeNotFoundException, FieldTypeNotFoundException, RepositoryException {
		return read(recordId, null, null);
	}

	public Record read(RecordId recordId, List<QName> fieldNames) throws RecordNotFoundException, RecordTypeNotFoundException, FieldTypeNotFoundException, RepositoryException {
		return read(recordId, null, fieldNames);
	}

	public Record read(RecordId recordId, Long version) throws RecordNotFoundException, RecordTypeNotFoundException, FieldTypeNotFoundException, RepositoryException {
		return read(recordId, version, null);
	}

	public Record read(RecordId recordId, Long version, List<QName> fieldNames) throws RecordNotFoundException, RecordTypeNotFoundException, FieldTypeNotFoundException,
	        RepositoryException {
		try {
			long avroVersion;
			if (version == null) {
				avroVersion = -1L;
			} else {
				avroVersion = version;
			}
			GenericArray<AvroQName> avroFieldNames = null;
			if (fieldNames != null) {
	        	avroFieldNames = new GenericData.Array<AvroQName>(fieldNames.size(), Schema.createArray(AvroQName.SCHEMA$));
	        	for (QName fieldName : fieldNames) {
	                avroFieldNames.add(converter.convert(fieldName));
                }
	        }
			return converter.convert(repositoryProxy.read(new Utf8(recordId.toString()), avroVersion, avroFieldNames));
        } catch (AvroRecordNotFoundException recordNotFoundException) {
        	throw converter.convert(recordNotFoundException);
        } catch (AvroRecordTypeNotFoundException recordTypeNotFoundException) {
        	throw converter.convert(recordTypeNotFoundException);
        } catch (AvroFieldTypeNotFoundException fieldTypeNotFoundException) {
        	throw converter.convert(fieldTypeNotFoundException);
        } catch (AvroRepositoryException repositoryException) {
        	throw converter.convert(repositoryException);
        } catch (AvroRemoteException remoteException) {
        	throw converter.convert(remoteException);
        }
	}

	public Record update(Record record) throws RecordNotFoundException, InvalidRecordException, RecordTypeNotFoundException, FieldTypeNotFoundException, RepositoryException {
		try {
	        return converter.convert(repositoryProxy.update(converter.convert(record)));
        } catch (AvroRecordNotFoundException recordNotFoundException) {
        	throw converter.convert(recordNotFoundException);
        } catch (AvroInvalidRecordException invalidRecordException) {
        	throw converter.convert(invalidRecordException);
        } catch (AvroRecordTypeNotFoundException recordTypeNotFoundException) {
        	throw converter.convert(recordTypeNotFoundException);
        } catch (AvroFieldTypeNotFoundException fieldTypeNotFoundException) {
        	throw converter.convert(fieldTypeNotFoundException);
        } catch (AvroRepositoryException repositoryException) {
        	throw converter.convert(repositoryException);
        } catch (AvroRemoteException remoteException) {
        	throw converter.convert(remoteException);
        }
	}
	
	public Record updateMutableFields(Record record) throws InvalidRecordException, RecordNotFoundException, RecordTypeNotFoundException, FieldTypeNotFoundException,
	RepositoryException {
		try {
	        return converter.convert(repositoryProxy.updateMutableFields(converter.convert(record)));
		} catch (AvroRecordNotFoundException recordNotFoundException) {
        	throw converter.convert(recordNotFoundException);
        } catch (AvroInvalidRecordException invalidRecordException) {
        	throw converter.convert(invalidRecordException);
        } catch (AvroRecordTypeNotFoundException recordTypeNotFoundException) {
        	throw converter.convert(recordTypeNotFoundException);
        } catch (AvroFieldTypeNotFoundException fieldTypeNotFoundException) {
        	throw converter.convert(fieldTypeNotFoundException);
        } catch (AvroRepositoryException repositoryException) {
        	throw converter.convert(repositoryException);
        } catch (AvroRemoteException remoteException) {
        	throw converter.convert(remoteException);
        }
	}

	public void registerBlobStoreAccess(BlobStoreAccess blobStoreAccess) {
	    throw new NotImplementedException();
	}
	
	
	public void delete(Blob blob) throws BlobNotFoundException, RepositoryException {
	    throw new NotImplementedException();
	}
	
	public InputStream getInputStream(Blob blob) throws BlobNotFoundException, RepositoryException {
	    throw new NotImplementedException();
	}
	
	public OutputStream getOutputStream(Blob blob) throws RepositoryException {
	    throw new NotImplementedException();
	}

	public Set<RecordId> getVariants(RecordId recordId) throws RepositoryException {
	    throw new NotImplementedException();
    }

	public IdRecord readWithIds(RecordId recordId, Long version, List<String> fieldIds) throws RecordNotFoundException, RecordTypeNotFoundException, FieldTypeNotFoundException,
            RepositoryException {
	    throw new NotImplementedException();
    }
}

