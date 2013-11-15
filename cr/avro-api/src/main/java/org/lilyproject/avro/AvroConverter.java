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
package org.lilyproject.avro;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.avro.AvroRemoteException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.avro.repository.RecordAsBytesConverter;
import org.lilyproject.bytes.impl.DataInputImpl;
import org.lilyproject.repository.api.CompareOp;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.FieldTypeEntry;
import org.lilyproject.repository.api.IdRecord;
import org.lilyproject.repository.api.IdentityRecordStack;
import org.lilyproject.repository.api.LRepository;
import org.lilyproject.repository.api.MutationCondition;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.RemoteException;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.TableCreateDescriptor;
import org.lilyproject.repository.api.TypeBucket;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.api.ValueType;
import org.lilyproject.repository.impl.id.SchemaIdImpl;
import org.lilyproject.util.Pair;
import org.lilyproject.util.repo.SystemFields;

public class AvroConverter {
    protected Log log = LogFactory.getLog(getClass());

    public AvroConverter() {
    }


    private byte[] asArray(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes, 0, bytes.length);
        return bytes;
    }

    public ByteBuffer convert(Record record, LRepository repository) throws AvroRepositoryException,
            AvroInterruptedException, RepositoryException, InterruptedException {
        return ByteBuffer.wrap(RecordAsBytesConverter.write(record, repository));
    }

    public ByteBuffer convert(IdRecord idRecord, LRepository repository) throws AvroRepositoryException,
            AvroInterruptedException, RepositoryException, InterruptedException {
        return ByteBuffer.wrap(RecordAsBytesConverter.writeIdRecord(idRecord, repository));
    }

    public Record convertRecord(ByteBuffer recordData, LRepository repository)
            throws RepositoryException, InterruptedException {
        return RecordAsBytesConverter.read(new DataInputImpl(asArray(recordData)), repository);
    }

    public IdRecord convertIdRecord(ByteBuffer avroIdRecord, LRepository repository)
            throws RepositoryException, InterruptedException {
        return RecordAsBytesConverter.readIdRecord(new DataInputImpl(asArray(avroIdRecord)), repository);
    }

    public List<AvroMutationCondition> convert(Record parentRecord, List<MutationCondition> conditions,
            LRepository repository) throws AvroRepositoryException, AvroInterruptedException {

        if (conditions == null) {
            return null;
        }

        List<AvroMutationCondition> avroConditions = new ArrayList<AvroMutationCondition>(conditions.size());

        SystemFields systemFields = SystemFields.getInstance(repository.getTypeManager(), repository.getIdGenerator());

        IdentityRecordStack parentRecords = new IdentityRecordStack(parentRecord);

        for (MutationCondition condition : conditions) {
            FieldType fieldType;
            try {
                if (systemFields.isSystemField(condition.getField())) {
                    fieldType = systemFields.get(condition.getField());
                } else {
                    fieldType = repository.getTypeManager().getFieldTypeByName(condition.getField());
                }

                AvroMutationCondition avroCond = new AvroMutationCondition();

                avroCond.setName(convert(condition.getField()));

                if (condition.getValue() != null) {
                    ValueType valueType = fieldType.getValueType();
                    avroCond.setValueType(valueType.getName());

                    byte[] value = valueType.toBytes(condition.getValue(), parentRecords);
                    avroCond.setValue(ByteBuffer.wrap(value));
                }
                avroCond.setOperator(convert(condition.getOp()));
                avroCond.setAllowMissing(condition.getAllowMissing());

                avroConditions.add(avroCond);
            } catch (RepositoryException e) {
                throw convert(e);
            } catch (InterruptedException e) {
                throw convert(e);
            }

        }

        return avroConditions;
    }

    public List<Record> convertAvroRecords(List<ByteBuffer> avroRecords, LRepository repository)
            throws RepositoryException, InterruptedException {
        List<Record> records = new ArrayList<Record>();
        for (ByteBuffer avroRecord : avroRecords) {
            records.add(convertRecord(avroRecord, repository));
        }
        return records;
    }

    public List<ByteBuffer> convertRecords(Collection<Record> records, LRepository repository)
            throws AvroRepositoryException, AvroInterruptedException, RepositoryException, InterruptedException {
        List<ByteBuffer> avroRecords = new ArrayList<ByteBuffer>(records.size());
        for (Record record : records) {
            avroRecords.add(convert(record, repository));
        }
        return avroRecords;
    }


    public FieldType convert(AvroFieldType avroFieldType, TypeManager typeManager)
            throws RepositoryException, InterruptedException {
        ValueType valueType = convert(avroFieldType.getValueType(), typeManager);
        QName name = convert(avroFieldType.getName());
        SchemaId id = convert(avroFieldType.getId());
        if (id != null) {
            return typeManager.newFieldType(id, valueType, name, convert(avroFieldType.getScope()));
        }
        return typeManager.newFieldType(valueType, name, convert(avroFieldType.getScope()));
    }

    public Scope convert(AvroScope scope) {
        return scope == null ? null : Scope.values()[scope.ordinal()];
    }

    public AvroFieldType convert(FieldType fieldType) {
        AvroFieldType avroFieldType = new AvroFieldType();

        avroFieldType.setId(convert(fieldType.getId()));
        avroFieldType.setName(convert(fieldType.getName()));
        avroFieldType.setValueType(convert(fieldType.getValueType()));
        avroFieldType.setScope(convert(fieldType.getScope()));
        return avroFieldType;
    }

    public AvroScope convert(Scope scope) {
        return scope == null ? null : AvroScope.values()[scope.ordinal()];
    }

    public RecordType convert(AvroRecordType avroRecordType, TypeManager typeManager) throws RepositoryException {
        SchemaId recordTypeId = convert(avroRecordType.getId());
        QName recordTypeName = convert(avroRecordType.getName());
        RecordType recordType = typeManager.newRecordType(recordTypeId, recordTypeName);
        recordType.setVersion(avroRecordType.getVersion());
        List<AvroFieldTypeEntry> fieldTypeEntries = avroRecordType.getFieldTypeEntries();
        if (fieldTypeEntries != null) {
            for (AvroFieldTypeEntry avroFieldTypeEntry : fieldTypeEntries) {
                recordType.addFieldTypeEntry(convert(avroFieldTypeEntry, typeManager));
            }
        }
        List<AvroSupertype> supertypes = avroRecordType.getSupertypes();
        if (supertypes != null) {
            for (AvroSupertype avroSupertype : supertypes) {
                recordType.addSupertype(convert(avroSupertype.getRecordTypeId()), avroSupertype.getRecordTypeVersion());
            }
        }
        return recordType;
    }

    public AvroRecordType convert(RecordType recordType) {
        AvroRecordType avroRecordType = new AvroRecordType();
        avroRecordType.setId(convert(recordType.getId()));
        avroRecordType.setName(convert(recordType.getName()));
        Long version = recordType.getVersion();
        if (version != null) {
            avroRecordType.setVersion(version);
        }
        Collection<FieldTypeEntry> fieldTypeEntries = recordType.getFieldTypeEntries();
        avroRecordType.setFieldTypeEntries(new ArrayList<AvroFieldTypeEntry>(fieldTypeEntries.size()));
        for (FieldTypeEntry fieldTypeEntry : fieldTypeEntries) {
            avroRecordType.getFieldTypeEntries().add(convert(fieldTypeEntry));
        }
        Set<Entry<SchemaId, Long>> supertypeEntries = recordType.getSupertypes().entrySet();
        avroRecordType.setSupertypes(new ArrayList<AvroSupertype>(supertypeEntries.size()));
        for (Entry<SchemaId, Long> supertypeEntry : supertypeEntries) {
            avroRecordType.getSupertypes().add(convert(supertypeEntry));
        }
        return avroRecordType;
    }

    public AvroFieldAndRecordTypes convertFieldAndRecordTypes(Pair<List<FieldType>, List<RecordType>> types) {
        AvroFieldAndRecordTypes avroTypes = new AvroFieldAndRecordTypes();
        List<FieldType> fieldTypes = types.getV1();
        avroTypes.setFieldTypes(new ArrayList<AvroFieldType>(fieldTypes.size()));
        for (FieldType fieldType : fieldTypes) {
            avroTypes.getFieldTypes().add(convert(fieldType));
        }
        List<RecordType> recordTypes = types.getV2();
        avroTypes.setRecordTypes(new ArrayList<AvroRecordType>(recordTypes.size()));
        for (RecordType recordType : recordTypes) {
            avroTypes.getRecordTypes().add(convert(recordType));
        }
        return avroTypes;
    }

    public Pair<List<FieldType>, List<RecordType>> convertAvroFieldAndRecordTypes(AvroFieldAndRecordTypes types,
            TypeManager typeManager) throws RepositoryException, InterruptedException {
        List<FieldType> fieldTypes = new ArrayList<FieldType>(types.getFieldTypes().size());
        for (AvroFieldType avroFieldType : types.getFieldTypes()) {
            fieldTypes.add(convert(avroFieldType, typeManager));
        }
        List<RecordType> recordTypes = new ArrayList<RecordType>(types.getRecordTypes().size());
        for (AvroRecordType avroRecordType : types.getRecordTypes()) {
            recordTypes.add(convert(avroRecordType, typeManager));
        }
        return new Pair<List<FieldType>, List<RecordType>>(fieldTypes, recordTypes);
    }

    public AvroTypeBucket convertTypeBucket(TypeBucket typeBucket) {
        AvroTypeBucket avroTypeBucket = new AvroTypeBucket();
        avroTypeBucket.setBucketId(typeBucket.getBucketId());
        List<FieldType> fieldTypes = typeBucket.getFieldTypes();
        avroTypeBucket.setFieldTypes(new ArrayList<AvroFieldType>(fieldTypes.size()));
        for (FieldType fieldType : fieldTypes) {
            avroTypeBucket.getFieldTypes().add(convert(fieldType));
        }
        List<RecordType> recordTypes = typeBucket.getRecordTypes();
        avroTypeBucket.setRecordTypes(new ArrayList<AvroRecordType>(recordTypes.size()));
        for (RecordType recordType : recordTypes) {
            avroTypeBucket.getRecordTypes().add(convert(recordType));
        }
        return avroTypeBucket;
    }

    public TypeBucket convertAvroTypeBucket(AvroTypeBucket avroTypeBucket, TypeManager  typeManager)
            throws RepositoryException, InterruptedException {
        TypeBucket typeBucket = new TypeBucket(avroTypeBucket.getBucketId());
        for (AvroFieldType avroFieldType : avroTypeBucket.getFieldTypes()) {
            typeBucket.add(convert(avroFieldType, typeManager));
        }
        for (AvroRecordType avroRecordType : avroTypeBucket.getRecordTypes()) {
            typeBucket.add(convert(avroRecordType, typeManager));
        }
        return typeBucket;
    }

    public ValueType convert(AvroValueType valueType, TypeManager typeManager)
            throws RepositoryException, InterruptedException {
        return valueType == null ? null : typeManager.getValueType(valueType.getValueType());
    }

    public AvroValueType convert(ValueType valueType) {
        if (valueType == null) {
            return null;
        }

        AvroValueType avroValueType = new AvroValueType();
        avroValueType.setValueType(valueType.getName());
        return avroValueType;
    }

    public QName convert(AvroQName name) {
        if (name == null) {
            return null;
        }
        return new QName(name.getNamespace(), name.getName());
    }

    public AvroQName convert(QName name) {
        if (name == null) {
            return null;
        }

        AvroQName avroQName = new AvroQName();
        avroQName.setNamespace(name.getNamespace());
        avroQName.setName(name.getName());
        return avroQName;
    }

    public AvroSupertype convert(Entry<SchemaId, Long> supertypeEntry) {
        AvroSupertype avroSupertype = new AvroSupertype();
        avroSupertype.setRecordTypeId(convert(supertypeEntry.getKey()));
        Long version = supertypeEntry.getValue();
        if (version != null) {
            avroSupertype.setRecordTypeVersion(version);
        }
        return avroSupertype;
    }

    public FieldTypeEntry convert(AvroFieldTypeEntry avroFieldTypeEntry, TypeManager typeManager) {
        return typeManager.newFieldTypeEntry(convert(avroFieldTypeEntry.getId()),
                avroFieldTypeEntry.getMandatory());
    }

    public AvroFieldTypeEntry convert(FieldTypeEntry fieldTypeEntry) {
        AvroFieldTypeEntry avroFieldTypeEntry = new AvroFieldTypeEntry();
        avroFieldTypeEntry.setId(convert(fieldTypeEntry.getFieldTypeId()));
        avroFieldTypeEntry.setMandatory(fieldTypeEntry.isMandatory());
        return avroFieldTypeEntry;
    }

    public static AvroGenericException convertOtherException(Throwable throwable) {
        AvroGenericException avroException = new AvroGenericException();
        avroException.setMessage$(throwable.getMessage());
        avroException.setExceptionClass(throwable.getClass().getName());
        avroException.setRemoteCauses(buildCauses(throwable));
        return avroException;
    }

    public RuntimeException convert(AvroGenericException avroException) {
        try {
            // Attempt to restore the original exception: only for RuntimeExceptions which
            // have a constructor that takes a string argument.
            Class exceptionClass = Class.forName(avroException.getExceptionClass());
            if (RuntimeException.class.isAssignableFrom(exceptionClass)) {
                Constructor constructor = exceptionClass.getConstructor(String.class);
                constructor.setAccessible(true);
                RuntimeException runtimeException = (RuntimeException) constructor.newInstance(
                        avroException.getMessage$());
                restoreCauses(avroException.getRemoteCauses(), runtimeException);
                return runtimeException;
            }
        } catch (Exception e) {
            // cannot restore the original exception, will create a generic exception
        }

        RuntimeException exception = new RuntimeException(avroException.getExceptionClass() + ": "
                + avroException.getMessage());
        restoreCauses(avroException.getRemoteCauses(), exception);
        return exception;
    }

    public RemoteException convert(AvroRemoteException exception) {
        return new RemoteException(exception.getMessage(), exception);
    }

    public AvroRepositoryException convert(RepositoryException exception) {
        AvroRepositoryException avroRepositoryException = new AvroRepositoryException();
        avroRepositoryException.setMessage$(exception.getMessage());
        avroRepositoryException.setRemoteCauses(buildCauses(exception));
        avroRepositoryException.setExceptionClass(exception.getClass().getName());
        avroRepositoryException.setParams(exception.getState());
        return avroRepositoryException;
    }

    public RepositoryException convert(AvroRepositoryException avroException) {
        try {
            Class exceptionClass = Class.forName(avroException.getExceptionClass());
            Constructor constructor = exceptionClass.getConstructor(String.class, Map.class);
            RepositoryException repositoryException = (RepositoryException) constructor.newInstance(
                    avroException.getMessage$(), avroException.getParams());
            restoreCauses(avroException.getRemoteCauses(), repositoryException);
            return repositoryException;
        } catch (Exception e) {
            log.error("Failure while converting remote exception", e);

            RepositoryException repositoryException = new RepositoryException(avroException.getMessage$());
            restoreCauses(avroException.getRemoteCauses(), repositoryException);
            return repositoryException;
        }
    }

    public AvroInterruptedException convert(InterruptedException exception) {
        AvroInterruptedException avroException = new AvroInterruptedException();
        avroException.setMessage$(exception.getMessage());
        return avroException;
    }

    public AvroIOException convert(IOException exception) {
        AvroIOException avroIOException = new AvroIOException();
        avroIOException.setMessage$(exception.getMessage());
        avroIOException.setRemoteCauses(buildCauses(exception));
        avroIOException.setExceptionClass(exception.getClass().getName());
        return avroIOException;
    }

    public IOException convert(AvroIOException avroException) {
        try {
            Class exceptionClass = Class.forName(avroException.getExceptionClass());
            Constructor constructor = exceptionClass.getConstructor(String.class);
            IOException ioException = (IOException) constructor.newInstance(avroException.getMessage$());
            restoreCauses(avroException.getRemoteCauses(), ioException);
            return ioException;
        } catch (Exception e) {
            log.error("Failure while converting remote exception", e);

            IOException ioException = new IOException(avroException.getMessage$());
            restoreCauses(avroException.getRemoteCauses(), ioException);
            return ioException;
        }
    }

    public ByteBuffer convert(RecordId recordId) {
        if (recordId == null) {
            return null;
        }
        return ByteBuffer.wrap(recordId.toBytes());
    }

    public Long convertAvroVersion(long avroVersion) {
        if (avroVersion == -1) {
            return null;
        }
        return avroVersion;
    }

    public long convertVersion(Long version) {
        if (version == null) {
            return -1;
        }
        return version;
    }

    private static List<AvroExceptionCause> buildCauses(Throwable throwable) {
        List<AvroExceptionCause> causes = new ArrayList<AvroExceptionCause>();

        Throwable cause = throwable;

        while (cause != null) {
            causes.add(convertCause(cause));
            cause = cause.getCause();
        }

        return causes;
    }

    private static AvroExceptionCause convertCause(Throwable throwable) {
        AvroExceptionCause cause = new AvroExceptionCause();
        cause.setClassName(throwable.getClass().getName());
        cause.setMessage(throwable.getMessage());

        StackTraceElement[] stackTrace = throwable.getStackTrace();

        cause.setStackTrace(new ArrayList<AvroStackTraceElement>(stackTrace.length));

        for (StackTraceElement el : stackTrace) {
            cause.getStackTrace().add(convert(el));
        }

        return cause;
    }

    private static AvroStackTraceElement convert(StackTraceElement el) {
        AvroStackTraceElement result = new AvroStackTraceElement();
        result.setClassName(el.getClassName());
        result.setMethodName(el.getMethodName());
        result.setFileName(el.getFileName());
        result.setLineNumber(el.getLineNumber());
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
            List<StackTraceElement> stackTrace = new ArrayList<StackTraceElement>(remoteCause.getStackTrace().size());

            for (AvroStackTraceElement el : remoteCause.getStackTrace()) {
                stackTrace.add(new StackTraceElement(el.getClassName(), el.getMethodName(),
                        el.getFileName(), el.getLineNumber()));
            }

            RestoredException cause = new RestoredException(remoteCause.getMessage(),
                    remoteCause.getClassName(), stackTrace);

            if (result == null) {
                result = cause;
            } else {
                result.initCause(cause);
                result = cause;
            }
        }

        return result;
    }

    public List<FieldType> convertAvroFieldTypes(List<AvroFieldType> avroFieldTypes, TypeManager typeManager)
            throws RepositoryException, InterruptedException {
        List<FieldType> fieldTypes = new ArrayList<FieldType>();
        for (AvroFieldType avroFieldType : avroFieldTypes) {
            fieldTypes.add(convert(avroFieldType, typeManager));
        }
        return fieldTypes;
    }

    public List<RecordType> convertAvroRecordTypes(List<AvroRecordType> avroRecordTypes, TypeManager typeManager)
            throws RepositoryException, InterruptedException {
        List<RecordType> recordTypes = new ArrayList<RecordType>();
        for (AvroRecordType avroRecordType : avroRecordTypes) {
            recordTypes.add(convert(avroRecordType, typeManager));
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





    public SchemaId convert(AvroSchemaId avroSchemaId) {
        if (avroSchemaId == null) {
            return null;
        }
        byte[] idBytes = null;
        if (avroSchemaId.getIdBytes() != null) {
            idBytes = avroSchemaId.getIdBytes().array();
        }
        return new SchemaIdImpl(idBytes);
    }

    public AvroSchemaId convert(SchemaId schemaId) {
        if (schemaId == null) {
            return null;
        }
        AvroSchemaId avroSchemaId = new AvroSchemaId();
        if (schemaId.getBytes() != null) {
            avroSchemaId.setIdBytes(ByteBuffer.wrap(schemaId.getBytes()));
        }
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





    public List<String> convert(Set<RecordId> recordIds) {
        List<String> avroRecordIds = new ArrayList<String>(recordIds.size());
        for (RecordId recordId : recordIds) {
            avroRecordIds.add(recordId.toString());
        }
        return avroRecordIds;
    }

    public CompareOp convert(AvroCompareOp op) {
        return op == null ? null : CompareOp.values()[op.ordinal()];
    }



    public List<MutationCondition> convertFromAvro(List<AvroMutationCondition> avroConditions, LRepository repository)
            throws RepositoryException, InterruptedException {

        if (avroConditions == null) {
            return null;
        }

        List<MutationCondition> conditions = new ArrayList<MutationCondition>(avroConditions.size());

        for (AvroMutationCondition avroCond : avroConditions) {
            QName name = convert(avroCond.getName());

            // value is optional
            Object value = null;
            if (avroCond.getValue() != null) {
                ValueType valueType = repository.getTypeManager().getValueType(avroCond.getValueType());
                value = valueType.read(avroCond.getValue().array());
            }

            CompareOp op = convert(avroCond.getOperator());
            boolean allowMissing = avroCond.getAllowMissing();

            conditions.add(new MutationCondition(name, op, value, allowMissing));
        }

        return conditions;
    }



    public AvroTableCreateDescriptor convert(TableCreateDescriptor descriptor) {
        AvroTableCreateDescriptor avroDescriptor = new AvroTableCreateDescriptor();
        avroDescriptor.setName(descriptor.getName());
        byte[][] splitKeys = descriptor.getSplitKeys();
        if (splitKeys != null) {
            List<ByteBuffer> avroSplitKeys = new ArrayList<ByteBuffer>(splitKeys.length);
            for (byte[] splitKey : splitKeys) {
                avroSplitKeys.add(ByteBuffer.wrap(splitKey));
            }
            avroDescriptor.setSplitKeys(avroSplitKeys);
        }
        return avroDescriptor;
    }

    public TableCreateDescriptor convert(AvroTableCreateDescriptor avroDescriptor) {
        String name = avroDescriptor.getName();

        List<ByteBuffer> avroSplitKeys = avroDescriptor.getSplitKeys();
        byte[][] splitKeys = null;
        if (avroSplitKeys != null) {
            splitKeys = new byte[avroSplitKeys.size()][];
            for (int i = 0; i < avroSplitKeys.size(); i++) {
                splitKeys[i] = asArray(avroSplitKeys.get(i));
            }
        }

        return new TableCreateDescriptor(name, splitKeys);
    }



    public AvroCompareOp convert(CompareOp op) {
        return op == null ? null : AvroCompareOp.values()[op.ordinal()];
    }

    public RecordId convertAvroRecordId(ByteBuffer recordId, LRepository repository) {
        byte[] bytes = new byte[recordId.remaining()];
        recordId.get(bytes);
        return repository.getIdGenerator().fromBytes(bytes);
    }

    public Set<RecordId> convertAvroRecordIds(List<String> avroRecordIds, LRepository repository) {
        Set<RecordId> recordIds = new HashSet<RecordId>();
        for (String avroRecordId : avroRecordIds) {
            recordIds.add(repository.getIdGenerator().fromString(avroRecordId));
        }
        return recordIds;
    }
}
