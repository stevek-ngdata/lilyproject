package org.lilycms.repository.avro;

import org.apache.avro.util.Utf8;
import org.lilycms.repository.api.TypeManager;
import org.lilycms.repository.api.exception.FieldTypeExistsException;
import org.lilycms.repository.api.exception.FieldTypeNotFoundException;
import org.lilycms.repository.api.exception.FieldTypeUpdateException;
import org.lilycms.repository.api.exception.RecordTypeExistsException;
import org.lilycms.repository.api.exception.RecordTypeNotFoundException;
import org.lilycms.repository.api.exception.RepositoryException;

public class AvroTypeManagerImpl implements AvroTypeManager {

	private final TypeManager typeManager;
	private AvroConverter converter;

	public AvroTypeManagerImpl(TypeManager serverTypeManager, AvroConverter converter) {
		this.typeManager = serverTypeManager;
		this.converter = converter;
	}
	
	public AvroFieldType createFieldType(AvroFieldType avroFieldType)
			throws AvroFieldTypeExistsException, AvroRepositoryException {
		try {
			return converter.convert(typeManager.createFieldType(converter.convert(avroFieldType)));
		} catch (FieldTypeExistsException fieldTypeExistsException) {
			throw converter.convert(fieldTypeExistsException);
		} catch (RepositoryException repositoryException) {
			throw converter.convert(repositoryException);
		}
	}

	public AvroRecordType createRecordType(AvroRecordType avroRecordType)
			throws AvroRepositoryException,
			AvroRecordTypeExistsException, AvroRecordTypeNotFoundException,
			AvroFieldTypeNotFoundException {
		try {
			return converter.convert(typeManager.createRecordType(converter.convert(avroRecordType)));
		} catch (RecordTypeExistsException recordTypeExistsException) {
			throw converter.convert(recordTypeExistsException);
		} catch (RecordTypeNotFoundException recordTypeNotFoundException) {
			throw converter.convert(recordTypeNotFoundException);
		} catch (FieldTypeNotFoundException fieldTypeNotFoundException) {
			throw converter.convert(fieldTypeNotFoundException);
		} catch (RepositoryException repositoryException) {
			throw converter.convert(repositoryException);
		}
	}

	public AvroRecordType getRecordType(Utf8 id, long avroVersion)
			throws AvroRecordTypeNotFoundException,
			AvroRepositoryException {
		try {
			Long version = null;
			if (avroVersion != -1) {
				version = avroVersion;
			}
			return converter.convert(typeManager.getRecordType(id.toString(), version));
		} catch (RecordTypeNotFoundException recordTypeNotFoundException) {
			throw converter.convert(recordTypeNotFoundException);
		} catch (RepositoryException repositoryException) {
			throw converter.convert(repositoryException);
		}
	}

	public AvroRecordType updateRecordType(AvroRecordType recordType)
			throws AvroRecordTypeNotFoundException,
			AvroFieldTypeNotFoundException, AvroRepositoryException {
		try {
			return converter.convert(typeManager.updateRecordType(converter.convert(recordType)));
		} catch (RecordTypeNotFoundException recordTypeNotFoundException) {
			throw converter.convert(recordTypeNotFoundException);
		} catch (FieldTypeNotFoundException fieldTypeNotFoundException) {
			throw converter.convert(fieldTypeNotFoundException);
		} catch (RepositoryException repositoryException) {
			throw converter.convert(repositoryException);
		}
	}

	public AvroFieldType updateFieldType(AvroFieldType fieldType)
			throws AvroFieldTypeNotFoundException,
			AvroFieldTypeUpdateException, AvroRepositoryException {
		try {
			return converter.convert(typeManager.updateFieldType(converter.convert(fieldType)));
		} catch (FieldTypeNotFoundException fieldTypeNotFoundException) {
			throw converter.convert(fieldTypeNotFoundException);
		} catch (FieldTypeUpdateException fieldTypeUpdateException) {
			throw converter.convert(fieldTypeUpdateException);
		} catch (RepositoryException repositoryException) {
			throw converter.convert(repositoryException);
		}
	}

	public AvroFieldType getFieldTypeById(Utf8 id) throws AvroFieldTypeNotFoundException, AvroRepositoryException {
		try {
			return converter.convert(typeManager.getFieldTypeById(id.toString()));
		} catch (FieldTypeNotFoundException fieldTypeNotFoundException) {
			throw converter.convert(fieldTypeNotFoundException);
		} catch (RepositoryException repositoryException) {
			throw converter.convert(repositoryException);
		}
	}

	public AvroFieldType getFieldTypeByName(AvroQName name) throws AvroFieldTypeNotFoundException, AvroRepositoryException {
		try {
	        return converter.convert(typeManager.getFieldTypeByName(converter.convert(name)));
        } catch (FieldTypeNotFoundException fieldTypeNotFoundException) {
	        throw converter.convert(fieldTypeNotFoundException);
		}
	}
}
