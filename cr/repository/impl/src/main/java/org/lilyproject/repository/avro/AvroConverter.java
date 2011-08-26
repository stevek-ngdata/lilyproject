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
package org.lilyproject.repository.avro;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.util.Utf8;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.bytes.impl.DataInputImpl;
import org.lilyproject.repository.api.*;
import org.lilyproject.repository.impl.IdRecordImpl;
import org.lilyproject.repository.impl.SchemaIdImpl;
import org.lilyproject.util.repo.SystemFields;

public class AvroConverter {
    private Log log = LogFactory.getLog(getClass());
    
    private TypeManager typeManager;
    private Repository repository;

    public AvroConverter() {
    }
    
    public void setRepository(Repository repository) {
        this.repository = repository;
        this.typeManager = repository.getTypeManager();
    }
    
    public Record convert(AvroRecord avroRecord) throws RepositoryException, InterruptedException {
        Record record = repository.newRecord();
        // Id
        if (avroRecord.id != null) {
            record.setId(convertAvroRecordId(avroRecord.id));
        }
        if (avroRecord.version != null) {
            record.setVersion(avroRecord.version);
        }
        // Record Types
        QName recordTypeName = null;
        if (avroRecord.recordTypeName != null) {
            recordTypeName = convert(avroRecord.recordTypeName);
        }
        record.setRecordType(recordTypeName, avroRecord.recordTypeVersion);
         
        Map<CharSequence, AvroQName> scopeRecordTypeNames = avroRecord.scopeRecordTypeNames;
        if (scopeRecordTypeNames != null) {
            for (Scope scope : Scope.values()) {
                Utf8 key = new Utf8(scope.name());
                AvroQName scopeRecordTypeName = scopeRecordTypeNames.get(key);
                if (scopeRecordTypeName != null) {
                    record.setRecordType(scope, convert(scopeRecordTypeName), avroRecord.scopeRecordTypeVersions.get(key));
                }
            }
        }

        // Fields
        if (avroRecord.fields != null) {
            for (AvroField field : avroRecord.fields) {
                QName name = convert(field.name);
                ByteBuffer typeParams = field.typeParams;
                ValueType valueType = null;
                if (typeParams == null)
                    valueType = typeManager.getValueType(convert(field.valueType));
                else 
                    valueType = typeManager.getValueType(convert(field.valueType), new DataInputImpl(typeParams.array()));
                Object value = valueType.read(field.value.array());
                record.setField(name, value);
            }
        }

        // FieldsToDelete
        List<AvroQName> avroFieldsToDelete = avroRecord.fieldsToDelete;
        if (avroFieldsToDelete != null) {
            List<QName> fieldsToDelete = new ArrayList<QName>();
            for (AvroQName fieldToDelete : avroFieldsToDelete) {
                fieldsToDelete.add(convert(fieldToDelete));
            }
            record.addFieldsToDelete(fieldsToDelete);
        }

        record.setResponseStatus(convert(avroRecord.responseStatus));
        
        return record;
    }

    public ResponseStatus convert(AvroResponseStatus status) {
        return status == null ? null : ResponseStatus.values()[status.ordinal()];
    }
    
    public IdRecord convert(AvroIdRecord avroIdRecord) throws RepositoryException, InterruptedException {
        Map<SchemaId, QName> idToQNameMapping = null;
        // Using two arrays here since avro does only support strings in the keys of a map
        List<AvroSchemaId> avroIdToQNameMappingIds = avroIdRecord.idToQNameMappingIds;
        List<AvroQName> avroIdToQNameMappingNames = avroIdRecord.idToQNameMappingNames;
        if (avroIdToQNameMappingIds != null) {
            idToQNameMapping = new HashMap<SchemaId, QName>();
            int i = 0;
            for (AvroSchemaId avroSchemaId : avroIdToQNameMappingIds) {
                idToQNameMapping.put(convert(avroSchemaId), convert(avroIdToQNameMappingNames.get(i)));
                i++;
            }
        }
        Map<Scope, SchemaId> recordTypeIds = null;
        if (avroIdRecord.scopeRecordTypeIds == null) {
            recordTypeIds = new EnumMap<Scope, SchemaId>(Scope.class);
            Map<CharSequence, AvroSchemaId> avroRecordTypeIds = avroIdRecord.scopeRecordTypeIds;
            for (Scope scope : Scope.values()) {
                recordTypeIds.put(scope, convert(avroRecordTypeIds.get(new Utf8(scope.name()))));
            }
        }
        return new IdRecordImpl(convert(avroIdRecord.record), idToQNameMapping, recordTypeIds);
    }
    
    public List<MutationCondition> convertFromAvro(List<AvroMutationCondition> avroConditions)
            throws RepositoryException, InterruptedException {

        if (avroConditions == null) {
            return null;
        }

        List<MutationCondition> conditions = new ArrayList<MutationCondition>(avroConditions.size());

        for (AvroMutationCondition avroCond : avroConditions) {
            QName name = convert(avroCond.name);

            // value is optional
            Object value = null;
            if (avroCond.value != null) {
                ValueType valueType = null;
                ByteBuffer typeParams = avroCond.typeParams;
                if (typeParams == null)
                    valueType = typeManager.getValueType(convert(avroCond.valueType));
                else
                    valueType = typeManager.getValueType(convert(avroCond.valueType), new DataInputImpl(typeParams.array()));
                value = valueType.read(avroCond.value.array());
            }

            CompareOp op = convert(avroCond.operator);
            boolean allowMissing = avroCond.allowMissing;

            conditions.add(new MutationCondition(name, op, value, allowMissing));
        }

        return conditions;
    }

    public CompareOp convert(AvroCompareOp op) {
        return op == null ? null : CompareOp.values()[op.ordinal()];
    }

    public AvroRecord convert(Record record) throws AvroRepositoryException, AvroInterruptedException {
        AvroRecord avroRecord = new AvroRecord();
        // Id
        RecordId id = record.getId();
        if (id != null) {
            avroRecord.id = convert(id);
        }
        if (record.getVersion() != null) {
            avroRecord.version = record.getVersion();
        } else { avroRecord.version = null; }
        // Record types
        if (record.getRecordTypeName() != null) {
            avroRecord.recordTypeName = convert(record.getRecordTypeName());
        }
        if (record.getRecordTypeVersion() != null) {
            avroRecord.recordTypeVersion = record.getRecordTypeVersion();
        }
        avroRecord.scopeRecordTypeNames = new HashMap<CharSequence, AvroQName>();
        avroRecord.scopeRecordTypeVersions = new HashMap<CharSequence, Long>();
        for (Scope scope : Scope.values()) {
            QName recordTypeName = record.getRecordTypeName(scope);
            if (recordTypeName != null) {
                avroRecord.scopeRecordTypeNames.put(new Utf8(scope.name()), convert(recordTypeName));
                Long version = record.getRecordTypeVersion(scope);
                if (version != null) {
                    avroRecord.scopeRecordTypeVersions.put(new Utf8(scope.name()), version);
                }
            }
        }

        // Fields
        avroRecord.fields = new ArrayList<AvroField>(record.getFields().size());
        for (Entry<QName, Object> field : record.getFields().entrySet()) {
            AvroField avroField = new AvroField();
            avroField.name = convert(field.getKey());

            FieldType fieldType;
            try {
                fieldType = typeManager.getFieldTypeByName(field.getKey());

                avroField.valueType = fieldType.getValueType().getName();
                byte[] typeParams = fieldType.getValueType().getTypeParams();
                if (typeParams != null)
                    avroField.typeParams = ByteBuffer.wrap(typeParams);
    
                byte[] value = fieldType.getValueType().toBytes(field.getValue());
                avroField.value = ByteBuffer.wrap(value);
            } catch (RepositoryException e) {
                throw convert(e);
            } catch (InterruptedException e) {
                throw convert(e);
            }

            avroRecord.fields.add(avroField);
        }

        // FieldsToDelete
        List<QName> fieldsToDelete = record.getFieldsToDelete();
        avroRecord.fieldsToDelete = new ArrayList<AvroQName>(fieldsToDelete.size());
        for (QName fieldToDelete : fieldsToDelete) {
            avroRecord.fieldsToDelete.add(convert(fieldToDelete));
        }

        // Response status
        avroRecord.responseStatus = convert(record.getResponseStatus());

        return avroRecord; 
    }

    public AvroResponseStatus convert(ResponseStatus status) {
        return status == null ? null : AvroResponseStatus.values()[status.ordinal()];
    }
    
    public AvroIdRecord convert(IdRecord idRecord) throws AvroRepositoryException, AvroInterruptedException {
        AvroIdRecord avroIdRecord = new AvroIdRecord();
        avroIdRecord.record = convert(idRecord.getRecord());
     // Fields
        Map<SchemaId, QName> fields = idRecord.getFieldIdToNameMapping();
        if (fields != null) {
            // Using two arrays here since avro does only support strings in the keys of a map
            avroIdRecord.idToQNameMappingIds = new ArrayList<AvroSchemaId>(fields.size());
            avroIdRecord.idToQNameMappingNames = new ArrayList<AvroQName>(fields.size());
            for (Entry<SchemaId, QName> fieldEntry : fields.entrySet()) {
                avroIdRecord.idToQNameMappingIds.add(convert(fieldEntry.getKey()));
                avroIdRecord.idToQNameMappingNames.add(convert(fieldEntry.getValue()));
            }
        }
     // Record types
        avroIdRecord.scopeRecordTypeIds = new HashMap<CharSequence, AvroSchemaId>();
        for (Scope scope : Scope.values()) {
            SchemaId recordTypeId = idRecord.getRecordTypeId(scope);
            if (recordTypeId != null) {
                avroIdRecord.scopeRecordTypeIds.put(new Utf8(scope.name()), convert(recordTypeId));
            }
        }
        return avroIdRecord;
    }

    public List<AvroMutationCondition> convert(List<MutationCondition> conditions)
            throws AvroRepositoryException, AvroInterruptedException {

        if (conditions == null) {
            return null;
        }

        List<AvroMutationCondition> avroConditions = new ArrayList<AvroMutationCondition>(conditions.size());

        SystemFields systemFields = SystemFields.getInstance(typeManager, repository.getIdGenerator());

        for (MutationCondition condition : conditions) {
            FieldType fieldType;
            try {
                if (systemFields.isSystemField(condition.getField())) {
                    fieldType = systemFields.get(condition.getField());
                } else {
                    fieldType = typeManager.getFieldTypeByName(condition.getField());
                }


                AvroMutationCondition avroCond = new AvroMutationCondition();
    
                avroCond.name = convert(condition.getField());
    
                if (condition.getValue() != null) {
                    avroCond.valueType = convert(fieldType.getValueType().getName());
                    byte[] typeParams = fieldType.getValueType().getTypeParams();
                    if (typeParams != null)
                        avroCond.typeParams = ByteBuffer.wrap(typeParams);
    
                    byte[] value = fieldType.getValueType().toBytes(condition.getValue());
                    avroCond.value = ByteBuffer.wrap(value);
                }
                avroCond.operator = convert(condition.getOp());
                avroCond.allowMissing = condition.getAllowMissing();
                
                avroConditions.add(avroCond);
            } catch (RepositoryException e) {
                throw convert(e);
            } catch (InterruptedException e) {
                throw convert(e);
            }

        }

        return avroConditions;
    }
    
    public AvroCompareOp convert(CompareOp op) {
        return op == null ? null : AvroCompareOp.values()[op.ordinal()];
    }

    public FieldType convert(AvroFieldType avroFieldType) throws RepositoryException, InterruptedException {
        ValueType valueType = convert(avroFieldType.valueType);
        QName name = convert(avroFieldType.name);
        SchemaId id = convert(avroFieldType.id);
        if (id != null) {
            return typeManager.newFieldType(id, valueType, name, convert(avroFieldType.scope));
        }
        return typeManager.newFieldType(valueType, name, convert(avroFieldType.scope));
    }

    public Scope convert(AvroScope scope) {
        return scope == null ? null : Scope.values()[scope.ordinal()];
    }

    public AvroFieldType convert(FieldType fieldType) {
        AvroFieldType avroFieldType = new AvroFieldType();
        
        avroFieldType.id = convert(fieldType.getId());
        avroFieldType.name = convert(fieldType.getName());
        avroFieldType.valueType = convert(fieldType.getValueType());
        avroFieldType.scope = convert(fieldType.getScope());
        return avroFieldType;
    }

    public AvroScope convert(Scope scope) {
        return scope == null ? null : AvroScope.values()[scope.ordinal()];
    }

    public RecordType convert(AvroRecordType avroRecordType) throws RepositoryException {
        SchemaId recordTypeId = convert(avroRecordType.id);
        QName recordTypeName = convert(avroRecordType.name);
        RecordType recordType = typeManager.newRecordType(recordTypeId, recordTypeName);
        recordType.setVersion(avroRecordType.version);
        List<AvroFieldTypeEntry> fieldTypeEntries = avroRecordType.fieldTypeEntries;
        if (fieldTypeEntries != null) {
            for (AvroFieldTypeEntry avroFieldTypeEntry : fieldTypeEntries) {
                recordType.addFieldTypeEntry(convert(avroFieldTypeEntry));
            }
        }
        List<AvroMixin> mixins = avroRecordType.mixins;
        if (mixins != null) {
            for (AvroMixin avroMixin : mixins) {
                recordType.addMixin(convert(avroMixin.recordTypeId), avroMixin.recordTypeVersion);
            }
        }
        return recordType;
    }

    public AvroRecordType convert(RecordType recordType) {
        AvroRecordType avroRecordType = new AvroRecordType();
        avroRecordType.id = convert(recordType.getId());
        avroRecordType.name = convert(recordType.getName());
        Long version = recordType.getVersion();
        if (version != null) {
            avroRecordType.version = version;
        }
        Collection<FieldTypeEntry> fieldTypeEntries = recordType.getFieldTypeEntries();
        avroRecordType.fieldTypeEntries = new ArrayList<AvroFieldTypeEntry>(fieldTypeEntries.size());
        for (FieldTypeEntry fieldTypeEntry : fieldTypeEntries) {
            avroRecordType.fieldTypeEntries.add(convert(fieldTypeEntry));
        }
        Set<Entry<SchemaId,Long>> mixinEntries = recordType.getMixins().entrySet();
        avroRecordType.mixins = new ArrayList<AvroMixin>(mixinEntries.size());
        for (Entry<SchemaId, Long> mixinEntry : mixinEntries) {
            avroRecordType.mixins.add(convert(mixinEntry));
        }
        return avroRecordType;
    }

    public ValueType convert(AvroValueType valueType) throws RepositoryException, InterruptedException {
        ByteBuffer typeParams = valueType.typeParams;
        if (typeParams == null)
            return typeManager.getValueType(convert(valueType.valueType));
        return typeManager.getValueType(convert(valueType.valueType), new DataInputImpl(typeParams.array()));
    }

    public AvroValueType convert(ValueType valueType) {
        AvroValueType avroValueType = new AvroValueType();
        avroValueType.valueType = valueType.getName();
        byte[] typeParams = valueType.getTypeParams();
        if (typeParams != null)
            avroValueType.typeParams = ByteBuffer.wrap(typeParams);
        return avroValueType;
    }

    public QName convert(AvroQName name) {
        if (name == null)
            return null;
        return new QName(convert(name.namespace), convert(name.name));
    }

    public AvroQName convert(QName name) {
        if (name == null)
            return null;

        AvroQName avroQName = new AvroQName();
        avroQName.namespace = name.getNamespace();
        avroQName.name = name.getName();
        return avroQName;
    }

    public AvroMixin convert(Entry<SchemaId, Long> mixinEntry) {
        AvroMixin avroMixin = new AvroMixin();
        avroMixin.recordTypeId = convert(mixinEntry.getKey());
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
        avroFieldTypeEntry.id = convert(fieldTypeEntry.getFieldTypeId());
        avroFieldTypeEntry.mandatory = fieldTypeEntry.isMandatory();
        return avroFieldTypeEntry;
    }

    public RuntimeException convert(AvroGenericException avroException) {
        RuntimeException exception = new RuntimeException();
        restoreCauses(avroException.remoteCauses, exception);
        return exception;
    }

    public AvroGenericException convertOtherException(Throwable throwable) {
        AvroGenericException avroException = new AvroGenericException();
        avroException.remoteCauses = buildCauses(throwable);
        return avroException;
    }

    public RemoteException convert(AvroRemoteException exception) {
        return new RemoteException(exception.getMessage(), exception);
    }
    
    public AvroRepositoryException convert(RepositoryException exception) {
        AvroRepositoryException avroRepositoryException = new AvroRepositoryException();
        avroRepositoryException.message = exception.getMessage();
        avroRepositoryException.remoteCauses = buildCauses(exception);
        avroRepositoryException.exceptionClass = exception.getClass().getName();
        Map<String, String> params = exception.getState();
        if (params != null) {
            avroRepositoryException.params = new HashMap<CharSequence, CharSequence>();
            avroRepositoryException.params.putAll(params);
        }
        return avroRepositoryException;
    }
    
    public RepositoryException convert(AvroRepositoryException avroException) {
        try {
            Class exceptionClass = Class.forName(convert(avroException.exceptionClass));
            Constructor constructor = exceptionClass.getConstructor(String.class, Map.class);
            RepositoryException repositoryException = (RepositoryException)constructor.newInstance(
                    convert(avroException.message), avroMapToStringMap(avroException.params));
            restoreCauses(avroException.remoteCauses, repositoryException);
            return repositoryException;
        } catch (Exception e) {
            log.error("Failure while converting remote exception", e);
        }
        RepositoryException repositoryException = new RepositoryException(convert(avroException.message));
        restoreCauses(avroException.remoteCauses, repositoryException);
        return repositoryException;
    }

    private Map<String, String> avroMapToStringMap(Map<CharSequence, CharSequence> map) {
        if (map == null) {
            return null;
        }

        Map<String, String> result = new HashMap<String, String>();
        for (Map.Entry<CharSequence, CharSequence> entry : map.entrySet()) {
            result.put(convert(entry.getKey()), convert(entry.getValue()));
        }
        return result;
    }

    public AvroInterruptedException convert(InterruptedException exception) {
        AvroInterruptedException avroException = new AvroInterruptedException();
        avroException.message = exception.getMessage();
        return avroException;
    }


    public RecordId convertAvroRecordId(ByteBuffer recordId) {
        byte[] bytes = new byte[recordId.remaining()];
        recordId.get(bytes);
        return repository.getIdGenerator().fromBytes(bytes);
    }

    public ByteBuffer convert(RecordId recordId) {
        if (recordId == null) return null;
        return ByteBuffer.wrap(recordId.toBytes());
    }

    public String convert(CharSequence charSeq) {
        if (charSeq == null) return null;
        return charSeq.toString();
    }

    public Long convertAvroVersion(long avroVersion) {
        if (avroVersion == -1)
            return null;
        return avroVersion;
    }
    
    public long convertVersion(Long version) {
        if (version == null)
            return -1;
        return version;
    }

    private List<AvroExceptionCause> buildCauses(Throwable throwable) {
        List<AvroExceptionCause> causes = new ArrayList<AvroExceptionCause>();

        Throwable cause = throwable;

        while (cause != null) {
            causes.add(convertCause(cause));
            cause = cause.getCause();
        }

        return causes;
    }

    private AvroExceptionCause convertCause(Throwable throwable) {
        AvroExceptionCause cause = new AvroExceptionCause();
        cause.className = convert(throwable.getClass().getName());
        cause.message = convert(throwable.getMessage());

        StackTraceElement[] stackTrace = throwable.getStackTrace();

        cause.stackTrace = new ArrayList<AvroStackTraceElement>(stackTrace.length);

        for (StackTraceElement el : stackTrace) {
            cause.stackTrace.add(convert(el));
        }

        return cause;
    }

    private AvroStackTraceElement convert(StackTraceElement el) {
        AvroStackTraceElement result = new AvroStackTraceElement();
        result.className = convert(el.getClassName());
        result.methodName = convert(el.getMethodName());
        result.fileName = convert(el.getFileName());
        result.lineNumber = el.getLineNumber();
        return result;
    }

    private void restoreCauses(List<AvroExceptionCause> remoteCauses, Throwable throwable) {
        Throwable causes = restoreCauses(remoteCauses);
        if (causes != null) {
            throwable.initCause(causes);
        }
    }

    private Throwable restoreCauses(List<AvroExceptionCause> remoteCauses) {
        Throwable result = null;

        for (AvroExceptionCause remoteCause : remoteCauses) {
            List<StackTraceElement> stackTrace = new ArrayList<StackTraceElement>(remoteCause.stackTrace.size());

            for (AvroStackTraceElement el : remoteCause.stackTrace) {
                stackTrace.add(new StackTraceElement(convert(el.className), convert(el.methodName),
                        convert(el.fileName), el.lineNumber));
            }

            RestoredException cause = new RestoredException(convert(remoteCause.message),
                    convert(remoteCause.className), stackTrace);

            if (result == null) {
                result = cause;
            } else {
                result.initCause(cause);
                result = cause;
            }
        }

        return result;
    }

    public List<FieldType> convertAvroFieldTypes(List<AvroFieldType> avroFieldTypes) throws RepositoryException, InterruptedException {
        List<FieldType> fieldTypes = new ArrayList<FieldType>();
        for (AvroFieldType avroFieldType : avroFieldTypes) {
            fieldTypes.add(convert(avroFieldType));
        }
        return fieldTypes;
    }

    public List<RecordType> convertAvroRecordTypes(List<AvroRecordType> avroRecordTypes) throws RepositoryException, InterruptedException {
        List<RecordType> recordTypes = new ArrayList<RecordType>();
        for (AvroRecordType avroRecordType : avroRecordTypes) {
            recordTypes.add(convert(avroRecordType));
        }
        return recordTypes;
    }

    public List<AvroFieldType> convertFieldTypes(Collection<FieldType> fieldTypes) {
        List<AvroFieldType> avroFieldTypes = new ArrayList<AvroFieldType>(fieldTypes.size());
        for (FieldType fieldType : fieldTypes) {
            avroFieldTypes.add(convert(fieldType));
        }
        return avroFieldTypes;
    }

    public List<AvroRecordType> convertRecordTypes(Collection<RecordType> recordTypes) {
        List<AvroRecordType> avroRecordTypes = new ArrayList<AvroRecordType>(recordTypes.size());
        for (RecordType recordType : recordTypes) {
            avroRecordTypes.add(convert(recordType));
        }
        return avroRecordTypes;
    }
    
    public List<Record> convertAvroRecords(List<AvroRecord> avroRecords) throws RepositoryException, InterruptedException {
        List<Record> records = new ArrayList<Record>();
        for(AvroRecord avroRecord : avroRecords) {
            records.add(convert(avroRecord));
        }
        return records;
    }

    public List<AvroRecord> convertRecords(Collection<Record> records) throws AvroRepositoryException, AvroInterruptedException {
        List<AvroRecord> avroRecords = new ArrayList<AvroRecord>(records.size());
        for (Record record: records) {
            avroRecords.add(convert(record));
        }
        return avroRecords;
    }
    
    public Set<RecordId> convertAvroRecordIds(List<CharSequence> avroRecordIds) {
        Set<RecordId> recordIds = new HashSet<RecordId>();
        IdGenerator idGenerator = repository.getIdGenerator();
        for (CharSequence avroRecordId : avroRecordIds) {
            recordIds.add(idGenerator.fromString(convert(avroRecordId)));
        }
        return recordIds;
    }
    
    public List<CharSequence> convert(Set<RecordId> recordIds) {
        List<CharSequence> avroRecordIds = new ArrayList<CharSequence>(recordIds.size());
        for (RecordId recordId: recordIds) {
            avroRecordIds.add(recordId.toString());
        }
        return avroRecordIds;
    }

    public SchemaId convert(AvroSchemaId avroSchemaId) {
        if (avroSchemaId == null)
            return null;
        byte[] idBytes = null;
        if (avroSchemaId.idBytes != null)
            idBytes = avroSchemaId.idBytes.array();
        return new SchemaIdImpl(idBytes);
    }
    
    public AvroSchemaId convert(SchemaId schemaId) {
        if (schemaId == null)
            return null;
        AvroSchemaId avroSchemaId = new AvroSchemaId();
        if (schemaId.getBytes() != null) 
            avroSchemaId.idBytes = ByteBuffer.wrap(schemaId.getBytes());
        return avroSchemaId;
    }
    
    public QName[] convert(List<AvroQName> avroNames) {
        QName[] names = null;
        if (avroNames != null) {
            names = new QName[avroNames.size()];
            int i = 0;
            for (AvroQName avroQName : avroNames) {
                names[i++] = convert(avroQName);
            }
        }
        return names;
    }
}
