package org.lilycms.repository.avro;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.ipc.AvroRemoteException;
import org.apache.avro.util.Utf8;
import org.lilycms.repository.api.*;

public class AvroRepositoryImpl implements AvroRepository {

	private final Repository repository;
	private AvroConverter converter;

	public AvroRepositoryImpl(Repository repository, AvroConverter converter) {
		this.repository = repository;
		this.converter = converter;
    }
	
	public AvroRecord create(AvroRecord record) throws AvroRemoteException, AvroRecordExistsException, AvroRecordNotFoundException, AvroInvalidRecordException,
	        AvroRecordTypeNotFoundException, AvroFieldTypeNotFoundException, AvroRepositoryException {
		try {
	        return converter.convert(repository.create(converter.convert(record)));
        } catch (RecordExistsException recordExistsException) {
        	throw converter.convert(recordExistsException);
        } catch (RecordNotFoundException recordNotFoundException) {
        	throw converter.convert(recordNotFoundException);
        } catch (InvalidRecordException invalidRecordException) {
        	throw converter.convert(invalidRecordException);
        } catch (RecordTypeNotFoundException recordTypeNotFoundException) {
        	throw converter.convert(recordTypeNotFoundException);
        } catch (FieldTypeNotFoundException fieldTypeNotFoundException) {
        	throw converter.convert(fieldTypeNotFoundException);
        } catch (RepositoryException repositoryException) {
        	throw converter.convert(repositoryException);
        }
	}

	public Void delete(Utf8 recordId) throws AvroRemoteException {
		try {
	        repository.delete(repository.getIdGenerator().fromString(recordId.toString()));
        } catch (RepositoryException repositoryException) {
        	throw converter.convert(repositoryException);
        }
        return null;
	}

	public AvroRecord read(Utf8 recordId, long avroVersion, GenericArray<AvroQName> avroFieldNames) throws AvroRemoteException, AvroRecordNotFoundException,
	        AvroRecordTypeNotFoundException, AvroFieldTypeNotFoundException, AvroRepositoryException {
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
        } catch (RecordNotFoundException recordNotFoundException) {
        	throw converter.convert(recordNotFoundException);
        } catch (RecordTypeNotFoundException recordTypeNotFoundException) {
        	throw converter.convert(recordTypeNotFoundException);
        } catch (FieldTypeNotFoundException fieldTypeNotFoundException) {
        	throw converter.convert(fieldTypeNotFoundException);
        } catch (RepositoryException repositoryException) {
        	throw converter.convert(repositoryException);
        }
	}

	public AvroRecord update(AvroRecord record) throws AvroRemoteException, AvroRecordNotFoundException, AvroInvalidRecordException, AvroRecordTypeNotFoundException,
	        AvroFieldTypeNotFoundException, AvroRepositoryException {
		try {
	        return converter.convert(repository.update(converter.convert(record)));
        } catch (RecordNotFoundException recordNotFoundException) {
        	throw converter.convert(recordNotFoundException);
        } catch (InvalidRecordException invalidRecordException) {
        	throw converter.convert(invalidRecordException);
        } catch (RecordTypeNotFoundException recordTypeNotFoundException) {
        	throw converter.convert(recordTypeNotFoundException);
        } catch (FieldTypeNotFoundException fieldTypeNotFoundException) {
        	throw converter.convert(fieldTypeNotFoundException);
        } catch (RepositoryException repositoryException) {
        	throw converter.convert(repositoryException);
        }
	}

	public AvroRecord updateMutableFields(AvroRecord record) throws AvroRemoteException, AvroRecordNotFoundException, AvroInvalidRecordException, AvroRecordTypeNotFoundException,
	        AvroFieldTypeNotFoundException, AvroRepositoryException {
		try {
	        return converter.convert(repository.updateMutableFields(converter.convert(record)));
        } catch (InvalidRecordException invalidRecordException) {
        	throw converter.convert(invalidRecordException);
        } catch (RecordNotFoundException recordNotFoundException) {
        	throw converter.convert(recordNotFoundException);
        } catch (RecordTypeNotFoundException recordTypeNotFoundException) {
        	throw converter.convert(recordTypeNotFoundException);
        } catch (FieldTypeNotFoundException fieldTypeNotFoundException) {
        	throw converter.convert(fieldTypeNotFoundException);
        } catch (RepositoryException repositoryException) {
        	throw converter.convert(repositoryException);
        }
	}

}
