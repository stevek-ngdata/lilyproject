package org.lilycms.repository.avro;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.ipc.AvroRemoteException;
import org.apache.avro.util.Utf8;
import org.lilycms.repository.api.*;

public class AvroConverter {

	private TypeManager typeManager;
	private Repository repository;

	public AvroConverter() {
	}
	
	public void setRepository(Repository repository) {
	    this.repository = repository;
		this.typeManager = repository.getTypeManager();
    }
	
	public Record convert(AvroRecord avroRecord) throws RepositoryException, FieldTypeNotFoundException {
		Record record = repository.newRecord();
		// Id
		if (avroRecord.id != null) {
			record.setId(repository.getIdGenerator().fromString(avroRecord.id.toString()));
		}
		if (avroRecord.version != null) {
			record.setVersion(avroRecord.version);
		}
		// Record Types
		if (avroRecord.recordTypeId != null) {
			record.setRecordType(avroRecord.recordTypeId.toString(), avroRecord.version);
		}
		
		Map<Utf8, Utf8> scopeRecordTypeIds = avroRecord.scopeRecordTypeIds;
		if (scopeRecordTypeIds != null) {
			for (Scope scope : Scope.values()) {
				Utf8 key = new Utf8(scope.name());
				Utf8 recordTypeId = scopeRecordTypeIds.get(key);
				if (recordTypeId != null) {
					record.setRecordType(scope, recordTypeId.toString(), avroRecord.scopeRecordTypeVersions.get(key));
				}
			}
		}
		// Fields
		Map<Utf8, ByteBuffer> fields = avroRecord.fields;
		if (fields != null) {
			for (Entry<Utf8, ByteBuffer> field : fields.entrySet()) {
				QName name = decodeQName(convert(field.getKey()));
				Object value = typeManager.getFieldTypeByName(name).getValueType().fromBytes(field.getValue().array());
				record.setField(name, value);
			}
		}
		// FieldsToDelete
		GenericArray<Utf8> avroFieldsToDelete = avroRecord.fieldsToDelete;
		if (avroFieldsToDelete != null) {
			List<QName> fieldsToDelete = new ArrayList<QName>();
			for (Utf8 fieldToDelete : avroFieldsToDelete) {
				fieldsToDelete.add(decodeQName(convert(fieldToDelete)));
			}
			record.addFieldsToDelete(fieldsToDelete);
		}
		return record;
	}
	
	public AvroRecord convert(Record record) throws AvroFieldTypeNotFoundException, AvroRepositoryException {
		AvroRecord avroRecord = new AvroRecord();
		// Id
		RecordId id = record.getId();
		if (id != null) {
			avroRecord.id = new Utf8(id.toString());
		}
		if (record.getVersion() != null) {
			avroRecord.version = record.getVersion();
		} else { avroRecord.version = null; }
		// Record types
		if (record.getRecordTypeId() != null) {
			avroRecord.recordTypeId = new Utf8(record.getRecordTypeId());
		} else { avroRecord.recordTypeId = null;}
		if (record.getRecordTypeVersion() != null) {
			avroRecord.recordTypeVersion = record.getRecordTypeVersion();
		}
		avroRecord.scopeRecordTypeIds = new HashMap<Utf8, Utf8>();
		avroRecord.scopeRecordTypeVersions = new HashMap<Utf8, Long>();
		for (Scope scope : Scope.values()) {
			String recordTypeId = record.getRecordTypeId(scope);
			if (recordTypeId != null) {
				avroRecord.scopeRecordTypeIds.put(new Utf8(scope.name()), new Utf8(recordTypeId));
				Long version = record.getRecordTypeVersion(scope);
				if (version != null) {
					avroRecord.scopeRecordTypeVersions.put(new Utf8(scope.name()), version);
				}
			}
		}
		// Fields
		avroRecord.fields = new HashMap<Utf8, ByteBuffer>();
		for (Entry<QName, Object> field : record.getFields().entrySet()) {
			QName name = field.getKey();
			FieldType fieldType;
			try {
				fieldType = typeManager.getFieldTypeByName(name);
			} catch (FieldTypeNotFoundException fieldTypeNotFoundExeption) {
				throw convert(fieldTypeNotFoundExeption);
            }
			byte[] value = fieldType.getValueType().toBytes(field.getValue());
			ByteBuffer byteBuffer = ByteBuffer.allocate(value.length);
			byteBuffer.mark();
			byteBuffer.put(value);
			byteBuffer.reset();
			avroRecord.fields.put(new Utf8(encodeQName(name)), byteBuffer);
		}
		// FieldsToDelete
		List<QName> fieldsToDelete = record.getFieldsToDelete();
		avroRecord.fieldsToDelete = new GenericData.Array<Utf8>(fieldsToDelete.size(), Schema.createArray(Schema.create(Schema.Type.STRING)));
		for (QName fieldToDelete : fieldsToDelete) {
			avroRecord.fieldsToDelete.add(new Utf8(encodeQName(fieldToDelete)));
		}
		return avroRecord; 
	}

	// The key of a map can only be a string in avro
	private String encodeQName(QName qname) {
		StringBuilder stringBuilder = new StringBuilder();
		String namespace = qname.getNamespace();
		if (namespace != null) {
			stringBuilder.append(namespace);
		}
		stringBuilder.append(":");
		stringBuilder.append(qname.getName());
		return stringBuilder.toString();
	}

	// The key of a map can only be a string in avro
	private QName decodeQName(String string) {
		int separatorIndex = string.indexOf(":");
		String namespace = null;
		if (separatorIndex != 0) {
			namespace = string.substring(0, separatorIndex);
		}
		String name = string.substring(separatorIndex+1);
		return new QName(namespace, name);
	}
	
	
	public FieldType convert(AvroFieldType avroFieldType) {
		ValueType valueType = convert(avroFieldType.valueType);
		QName name = convert(avroFieldType.name);
		String id = convert(avroFieldType.id);
		if (id != null) {
			return typeManager.newFieldType(id, valueType, name, avroFieldType.scope);
		}
		return typeManager.newFieldType(valueType, name, avroFieldType.scope);
	}

	public AvroFieldType convert(FieldType fieldType) {
		AvroFieldType avroFieldType = new AvroFieldType();
		
		if (fieldType.getId() != null) {
			avroFieldType.id = new Utf8(fieldType.getId());
		} 
		avroFieldType.name = convert(fieldType.getName());
		avroFieldType.valueType = convert(fieldType.getValueType());
		avroFieldType.scope = fieldType.getScope();
		return avroFieldType;
	}

	public RecordType convert(AvroRecordType avroRecordType) {
		String recordTypeId = convert(avroRecordType.id);
		RecordType recordType = typeManager.newRecordType(recordTypeId);
		recordType.setVersion(avroRecordType.version);
		GenericArray<AvroFieldTypeEntry> fieldTypeEntries = avroRecordType.fieldTypeEntries;
		if (fieldTypeEntries != null) {
			for (AvroFieldTypeEntry avroFieldTypeEntry : fieldTypeEntries) {
				recordType.addFieldTypeEntry(convert(avroFieldTypeEntry));
			}
		}
		GenericArray<AvroMixin> mixins = avroRecordType.mixins;
		if (mixins != null) {
			for (AvroMixin avroMixin : mixins) {
				recordType.addMixin(convert(avroMixin.recordTypeId), avroMixin.recordTypeVersion);
			}
		}
		return recordType;
	}

	public AvroRecordType convert(RecordType recordType) {
		AvroRecordType avroRecordType = new AvroRecordType();
		avroRecordType.id = new Utf8(recordType.getId());
		Long version = recordType.getVersion();
		if (version != null) {
			avroRecordType.version = version;
		}
		Collection<FieldTypeEntry> fieldTypeEntries = recordType.getFieldTypeEntries();
		avroRecordType.fieldTypeEntries = new GenericData.Array<AvroFieldTypeEntry>(fieldTypeEntries.size(), Schema.createArray(AvroFieldTypeEntry.SCHEMA$));
		for (FieldTypeEntry fieldTypeEntry : fieldTypeEntries) {
			avroRecordType.fieldTypeEntries.add(convert(fieldTypeEntry));
		}
		Set<Entry<String,Long>> mixinEntries = recordType.getMixins().entrySet();
		avroRecordType.mixins = new GenericData.Array<AvroMixin>(mixinEntries.size(), Schema.createArray(AvroMixin.SCHEMA$));
		for (Entry<String, Long> mixinEntry : mixinEntries) {
			avroRecordType.mixins.add(convert(mixinEntry));
		}
		return avroRecordType;
	}

	public ValueType convert(AvroValueType valueType) {
		return typeManager.getValueType(convert(valueType.primitiveValueType), valueType.multivalue, valueType.hierarchical);
	}

	public AvroValueType convert(ValueType valueType) {
		AvroValueType avroValueType = new AvroValueType();
		avroValueType.primitiveValueType = new Utf8(valueType.getPrimitive().getName());
		avroValueType.multivalue = valueType.isMultiValue();
		avroValueType.hierarchical = valueType.isHierarchical();
		return avroValueType;
	}

	public QName convert(AvroQName name) {
		return new QName(convert(name.namespace), convert(name.name));
	}

	public AvroQName convert(QName name) {
		AvroQName avroQName = new AvroQName();
		if (name.getNamespace() != null) {
			avroQName.namespace = new Utf8(name.getNamespace());
		}
		avroQName.name = new Utf8(name.getName());
		return avroQName;
	}

	public AvroMixin convert(Entry<String, Long> mixinEntry) {
		AvroMixin avroMixin = new AvroMixin();
		avroMixin.recordTypeId = new Utf8(mixinEntry.getKey());
		Long version = mixinEntry.getValue();
		if (version != null) {
			avroMixin.recordTypeVersion = version;
		}
		return avroMixin;
	}

	public FieldTypeEntry convert(AvroFieldTypeEntry avroFieldTypeEntry) {
		return typeManager.newFieldTypeEntry(convert(avroFieldTypeEntry.id), avroFieldTypeEntry.mandatory);
	}

	public AvroFieldTypeEntry convert(FieldTypeEntry fieldTypeEntry) {
		AvroFieldTypeEntry avroFieldTypeEntry = new AvroFieldTypeEntry();
		avroFieldTypeEntry.id = new Utf8(fieldTypeEntry.getFieldTypeId());
		avroFieldTypeEntry.mandatory = fieldTypeEntry.isMandatory();
		return avroFieldTypeEntry;
	}


	public AvroRepositoryException convert(RepositoryException repositoryException) {
		AvroRepositoryException avroRepositoryException = new AvroRepositoryException();
		if (repositoryException.getMessage() != null) {
			avroRepositoryException.message = new Utf8(repositoryException.getMessage());
		}
		return avroRepositoryException;
	}

	// TODO how to cope with these kinds of errors
	public RepositoryException convert(AvroRemoteException remoteException) {
		return new RepositoryException("A remoteException occured", remoteException);
	}

	public AvroFieldTypeExistsException convert(FieldTypeExistsException fieldTypeExistsException) {
		AvroFieldTypeExistsException avroFieldTypeExistsException = new AvroFieldTypeExistsException();
		avroFieldTypeExistsException.fieldType = convert(fieldTypeExistsException.getFieldType());
		return avroFieldTypeExistsException;
	}

	public RepositoryException convert(AvroRepositoryException repositoryException) {
		return new RepositoryException(convert(repositoryException.message), null);
	}

	public FieldTypeExistsException convert(AvroFieldTypeExistsException fieldTypeExistsException) {
		return new FieldTypeExistsException(convert(fieldTypeExistsException.fieldType));
	}

	public AvroRecordTypeExistsException convert(RecordTypeExistsException recordTypeExistsException) {
		AvroRecordTypeExistsException avroRecordTypeExistsException = new AvroRecordTypeExistsException();
		avroRecordTypeExistsException.recordType = convert(recordTypeExistsException.getRecordType());
		return avroRecordTypeExistsException;
	}

	public AvroRecordTypeNotFoundException convert(RecordTypeNotFoundException recordTypeNotFoundException) {
		AvroRecordTypeNotFoundException avroRecordTypeNotFoundException = new AvroRecordTypeNotFoundException();
		avroRecordTypeNotFoundException.id = new Utf8(recordTypeNotFoundException.getId());
		Long version = recordTypeNotFoundException.getVersion();
		if (version != null) {
			avroRecordTypeNotFoundException.version = version;
		}
		return avroRecordTypeNotFoundException;
	}

	public AvroFieldTypeNotFoundException convert(FieldTypeNotFoundException fieldTypeNotFoundException) {
		AvroFieldTypeNotFoundException avroFieldTypeNotFoundException = new AvroFieldTypeNotFoundException();
		avroFieldTypeNotFoundException.id = new Utf8(fieldTypeNotFoundException.getId());
		Long version = fieldTypeNotFoundException.getVersion();
		if (version != null) {
			avroFieldTypeNotFoundException.version = version;
		}
		return avroFieldTypeNotFoundException;
	}

	public RecordTypeExistsException convert(AvroRecordTypeExistsException avroRecordTypeExistsException) {
		return new RecordTypeExistsException(convert(avroRecordTypeExistsException.recordType));
	}

	public RecordTypeNotFoundException convert(AvroRecordTypeNotFoundException avroRecordTypeNotFoundException) {
		return new RecordTypeNotFoundException(convert(avroRecordTypeNotFoundException.id), avroRecordTypeNotFoundException.version);
	}

	public FieldTypeNotFoundException convert(AvroFieldTypeNotFoundException avroFieldTypeNotFoundException) {
		return new FieldTypeNotFoundException(convert(avroFieldTypeNotFoundException.id), avroFieldTypeNotFoundException.version);
	}

	public FieldTypeUpdateException convert(AvroFieldTypeUpdateException fieldTypeUpdateException) {
		return new FieldTypeUpdateException(convert(fieldTypeUpdateException.message));
	}

	public AvroFieldTypeUpdateException convert(FieldTypeUpdateException fieldTypeUpdateException) {
		AvroFieldTypeUpdateException avroFieldTypeUpdateException = new AvroFieldTypeUpdateException();
		if (fieldTypeUpdateException.getMessage() != null) {
			avroFieldTypeUpdateException.message = new Utf8(fieldTypeUpdateException.getMessage());
		}
		return avroFieldTypeUpdateException;
	}

	public AvroRecordExistsException convert(RecordExistsException recordExistsException) throws AvroRepositoryException, AvroFieldTypeNotFoundException {
	    AvroRecordExistsException avroRecordExistsException = new AvroRecordExistsException();
	    avroRecordExistsException.record = convert(recordExistsException.getRecord());
	    return avroRecordExistsException;
	    
    }

	public AvroRecordNotFoundException convert(RecordNotFoundException recordNotFoundException) throws AvroRepositoryException, AvroFieldTypeNotFoundException {
		AvroRecordNotFoundException avroRecordNotFoundException = new AvroRecordNotFoundException();
		avroRecordNotFoundException.record = convert(recordNotFoundException.getRecord());
		return avroRecordNotFoundException;
    }

	public AvroInvalidRecordException convert(InvalidRecordException invalidRecordException) throws AvroRepositoryException, AvroFieldTypeNotFoundException {
		AvroInvalidRecordException avroInvalidRecordException = new AvroInvalidRecordException();
		avroInvalidRecordException.record = convert(invalidRecordException.getRecord());
		if (invalidRecordException.getMessage() != null) {
			avroInvalidRecordException.message = new Utf8(invalidRecordException.getMessage());
		}
		return avroInvalidRecordException;
    }

	public RecordExistsException convert(AvroRecordExistsException recordExistsException) throws RepositoryException, FieldTypeNotFoundException {
	    return new RecordExistsException(convert(recordExistsException.record));
    }

	public RecordNotFoundException convert(AvroRecordNotFoundException recordNotFoundException) throws RepositoryException, FieldTypeNotFoundException {
		return new RecordNotFoundException(convert(recordNotFoundException.record));
    }

	public InvalidRecordException convert(AvroInvalidRecordException invalidRecordException) throws RepositoryException, FieldTypeNotFoundException {
		return new InvalidRecordException(convert(invalidRecordException.record), convert(invalidRecordException.message));
    }
	
	public String convert(Utf8 utf8) {
		if (utf8 == null) return null;
		return utf8.toString();
	}
}
