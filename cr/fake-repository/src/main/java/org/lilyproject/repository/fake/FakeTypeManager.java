/*
 * Copyright 2013 NGDATA nv
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
package org.lilyproject.repository.fake;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.lilyproject.repository.api.*;
import org.lilyproject.repository.impl.FieldTypeBuilderImpl;
import org.lilyproject.repository.impl.FieldTypeEntryImpl;
import org.lilyproject.repository.impl.FieldTypeImpl;
import org.lilyproject.repository.impl.FieldTypesImpl;
import org.lilyproject.repository.impl.RecordTypeBuilderImpl;
import org.lilyproject.repository.impl.RecordTypeImpl;
import org.lilyproject.repository.impl.id.SchemaIdImpl;
import org.lilyproject.repository.impl.valuetype.BlobValueType;
import org.lilyproject.repository.impl.valuetype.BooleanValueType;
import org.lilyproject.repository.impl.valuetype.ByteArrayValueType;
import org.lilyproject.repository.impl.valuetype.DateTimeValueType;
import org.lilyproject.repository.impl.valuetype.DateValueType;
import org.lilyproject.repository.impl.valuetype.DecimalValueType;
import org.lilyproject.repository.impl.valuetype.DoubleValueType;
import org.lilyproject.repository.impl.valuetype.IntegerValueType;
import org.lilyproject.repository.impl.valuetype.LinkValueType;
import org.lilyproject.repository.impl.valuetype.ListValueType;
import org.lilyproject.repository.impl.valuetype.LongValueType;
import org.lilyproject.repository.impl.valuetype.PathValueType;
import org.lilyproject.repository.impl.valuetype.RecordValueType;
import org.lilyproject.repository.impl.valuetype.StringValueType;
import org.lilyproject.repository.impl.valuetype.UriValueType;
import org.lilyproject.util.ArgumentValidator;
import org.lilyproject.util.Pair;
import org.lilyproject.util.repo.VersionTag;

/**
 * Dummy type manager that keeps field &amp; record types in memory. No support for caching or snapshots since we don't
 * really
 * need this during tests.
 */
public class FakeTypeManager implements TypeManager {

    Map<SchemaId, FieldType> fieldTypes = new HashMap<SchemaId, FieldType>();
    Map<SchemaId, RecordType> recordTypes = new HashMap<SchemaId, RecordType>();
    Map<QName, FieldType> fieldTypesByName = new HashMap<QName, FieldType>();
    Map<QName, RecordType> recordTypeByName = new HashMap<QName, RecordType>();
    Map<String, ValueTypeFactory> valueTypeFactories = new HashMap<String, ValueTypeFactory>();
    private IdGenerator idGenerator;

    public FakeTypeManager(IdGenerator idGenerator) {
        this.registerDefaultValueTypes();
        this.idGenerator = idGenerator;

        try {
            FieldType fieldType = newFieldType(getValueType("LONG"), VersionTag.LAST, Scope.NON_VERSIONED);
            createFieldType(fieldType);
        } catch (FieldTypeExistsException e) {
            // ok
        } catch (ConcurrentUpdateTypeException e) {
            // ok, another lily-server is starting up and doing the same thing
        } catch (RepositoryException e) {
            // not going to happen here
            new RuntimeException(e);
        } catch (InterruptedException e) {
            // not going to happen here
            new RuntimeException(e);
        }

    }

    @Override
    public RecordType newRecordType(QName name) throws TypeException {
        return new RecordTypeImpl(null, name);
    }

    @Override
    public RecordType newRecordType(SchemaId recordTypeId, QName name) throws TypeException {
        return new RecordTypeImpl(recordTypeId, name);
    }

    @Override
    public RecordType getRecordTypeById(SchemaId id, Long version) throws RepositoryException, InterruptedException {
        RecordType type = recordTypes.get(id);
        if (type == null) {
            throw new RecordTypeNotFoundException(id, version);
        }
        return type;
    }

    @Override
    public RecordType getRecordTypeByName(QName name, Long version) throws RepositoryException, InterruptedException {
        if (name == null) {
            return null;
        }
        RecordType type = recordTypeByName.get(name);
        if (type == null) {
            throw new RecordTypeNotFoundException(name, version);
        }
        return type;
    }

    @Override
    public Collection<RecordType> getRecordTypes() throws RepositoryException, InterruptedException {
        return Lists.newArrayList(recordTypes.values());
    }

    @Override
    public FieldTypeEntry newFieldTypeEntry(SchemaId fieldTypeId, boolean mandatory) {
        return new FieldTypeEntryImpl(fieldTypeId, mandatory);
    }

    @Override
    public FieldType newFieldType(ValueType valueType, QName name, Scope scope) {
        return newFieldType(null, valueType, name, scope);
    }

    @Override
    public FieldType newFieldType(String valueType, QName name, Scope scope)
            throws RepositoryException, InterruptedException {
        return newFieldType(null, getValueType(valueType), name, scope);
    }

    @Override
    public FieldType newFieldType(SchemaId id, ValueType valueType, QName name, Scope scope) {
        return new FieldTypeImpl(id, valueType, name, scope);
    }

    @Override
    public RecordType createRecordType(RecordType recordType) throws RepositoryException, InterruptedException {
        return createOrUpdateRecordType(recordType);
    }

    @Override
    public RecordType updateRecordType(RecordType recordType) throws RepositoryException, InterruptedException {
        return createOrUpdateRecordType(recordType);
    }

    @Override
    public RecordType createOrUpdateRecordType(RecordType recordType) throws RepositoryException, InterruptedException {
        if (recordType.getId() == null) {
            recordType.setId(new SchemaIdImpl(UUID.randomUUID()));
        }
        Long version = recordType.getVersion();
        if (version == null) {
            version = new Long(0l);
        }
        recordType.setVersion(version + 1l);
        recordTypeByName.put(recordType.getName(), recordType);
        recordTypes.put(recordType.getId(), recordType);
        return recordType;
    }

    @Override
    public FieldType createFieldType(FieldType fieldType) throws RepositoryException, InterruptedException {
        return createOrUpdateFieldType(fieldType);
    }

    @Override
    public FieldType createFieldType(ValueType valueType, QName name, Scope scope)
            throws RepositoryException, InterruptedException {
        FieldType fieldType = newFieldType(valueType, name, scope);
        return createOrUpdateFieldType(fieldType);
    }

    @Override
    public FieldType createFieldType(String valueType, QName name, Scope scope)
            throws RepositoryException, InterruptedException {
        FieldType fieldType = newFieldType(valueType, name, scope);
        return createOrUpdateFieldType(fieldType);
    }

    @Override
    public FieldType updateFieldType(FieldType fieldType) throws RepositoryException, InterruptedException {
        return createOrUpdateFieldType(fieldType);
    }

    @Override
    public FieldType createOrUpdateFieldType(FieldType fieldType) throws RepositoryException, InterruptedException {
        if (fieldType.getId() == null) {
            fieldType.setId(new SchemaIdImpl(UUID.randomUUID()));
        }
        fieldTypesByName.put(fieldType.getName(), fieldType);
        fieldTypes.put(fieldType.getId(), fieldType);
        return fieldType;
    }

    @Override
    public Pair<List<FieldType>, List<RecordType>> getTypesWithoutCache()
            throws RepositoryException, InterruptedException {
        return new Pair<List<FieldType>, List<RecordType>>(getFieldTypesWithoutCache(), getRecordTypesWithoutCache());
    }

    @Override
    public FieldType getFieldTypeById(SchemaId id) throws RepositoryException, InterruptedException {
        FieldType type = fieldTypes.get(id);
        if (type == null) {
            throw new FieldTypeNotFoundException(id);
        }
        return type;
    }

    @Override
    public FieldType getFieldTypeByName(QName name) throws RepositoryException, InterruptedException {
        FieldType type = fieldTypesByName.get(name);
        if (type == null) {
            throw new FieldTypeNotFoundException(name);
        }
        return type;
    }

    @Override
    public Collection<FieldTypeEntry> getFieldTypesForRecordType(RecordType recordType, boolean includeSupertypes)
            throws RepositoryException, InterruptedException {
        if (!includeSupertypes) {
            return recordType.getFieldTypeEntries();
        } else {
            // Pairs of record type id and version
            Map<Pair<SchemaId, Long>, RecordType> recordSupertypeMap = Maps.newHashMap();
            collectRecordSupertypes(Pair.create(recordType.getId(), recordType.getVersion()), recordSupertypeMap);

            // We use a map of SchemaId to FieldTypeEntry so that we can let mandatory field type entries
            // for the same field type override non-mandatory versions
            Map<SchemaId, FieldTypeEntry> fieldTypeMap = Maps.newHashMap();

            for (Pair<SchemaId, Long> recordSuperTypePair : recordSupertypeMap.keySet()) {
                RecordType superRecordType = recordSupertypeMap.get(recordSuperTypePair);
                for (FieldTypeEntry fieldTypeEntry : superRecordType.getFieldTypeEntries()) {
                    SchemaId fieldTypeId = fieldTypeEntry.getFieldTypeId();
                    if (fieldTypeMap.containsKey(fieldTypeId)) {
                        // Only overwrite an existing entry if we have one that is mandatory
                        if (fieldTypeEntry.isMandatory()) {
                            fieldTypeMap.put(fieldTypeId, fieldTypeEntry);
                        }
                    } else {
                        fieldTypeMap.put(fieldTypeId, fieldTypeEntry);
                    }
                }
            }
            return fieldTypeMap.values();
        }
    }

    private void collectRecordSupertypes(Pair<SchemaId, Long> recordTypeAndVersion,
            Map<Pair<SchemaId, Long>, RecordType> recordSuperTypes)
            throws RecordTypeNotFoundException, TypeException, RepositoryException, InterruptedException {
        if (recordSuperTypes.containsKey(recordTypeAndVersion)) {
            return;
        }
        RecordType recordType = getRecordTypeById(recordTypeAndVersion.getV1(), recordTypeAndVersion.getV2());
        recordSuperTypes.put(recordTypeAndVersion, recordType);
        for (Map.Entry<SchemaId, Long> entry : recordType.getSupertypes().entrySet()) {
            collectRecordSupertypes(Pair.create(entry.getKey(), entry.getValue()), recordSuperTypes);
        }

    }

    @Override
    public Collection<FieldType> getFieldTypes() throws RepositoryException, InterruptedException {
        return Lists.newArrayList(fieldTypes.values());
    }

    //
    // Value types
    //
    @Override
    public void registerValueType(String valueTypeName, ValueTypeFactory valueTypeFactory) {
        valueTypeFactories.put(valueTypeName, valueTypeFactory);
    }

    @Override
    public ValueType getValueType(String valueTypeSpec) throws RepositoryException, InterruptedException {
        ValueType valueType;

        int indexOfParams = valueTypeSpec.indexOf("<");
        if (indexOfParams == -1) {
            ValueTypeFactory valueTypeFactory = valueTypeFactories.get(valueTypeSpec);
            if (valueTypeFactory == null) {
                throw new TypeException("Unkown value type: " + valueTypeSpec);
            }
            valueType = valueTypeFactory.getValueType(null);
        } else {
            if (!valueTypeSpec.endsWith(">")) {
                throw new IllegalArgumentException("Invalid value type string, no closing angle bracket: '" +
                        valueTypeSpec + "'");
            }

            String arg = valueTypeSpec.substring(indexOfParams + 1, valueTypeSpec.length() - 1);

            if (arg.length() == 0) {
                throw new IllegalArgumentException("Invalid value type string, type arg is zero length: '" +
                        valueTypeSpec + "'");
            }

            ValueTypeFactory valueTypeFactory = valueTypeFactories.get(valueTypeSpec.substring(0, indexOfParams));
            if (valueTypeFactory == null) {
                throw new TypeException("Unkown value type: " + valueTypeSpec);
            }
            valueType = valueTypeFactory.getValueType(arg);
        }

        return valueType;
    }

    // TODO get this from some configuration file
    protected void registerDefaultValueTypes() {
        //
        // Important:
        //
        // When adding a type below, please update the list of built-in
        // types in the javadoc of the method TypeManager.getValueType.
        //

        // TODO or rather use factories?
        registerValueType(StringValueType.NAME, StringValueType.factory());
        registerValueType(IntegerValueType.NAME, IntegerValueType.factory());
        registerValueType(LongValueType.NAME, LongValueType.factory());
        registerValueType(DoubleValueType.NAME, DoubleValueType.factory());
        registerValueType(DecimalValueType.NAME, DecimalValueType.factory());
        registerValueType(BooleanValueType.NAME, BooleanValueType.factory());
        registerValueType(DateValueType.NAME, DateValueType.factory());
        registerValueType(DateTimeValueType.NAME, DateTimeValueType.factory());
        registerValueType(LinkValueType.NAME, LinkValueType.factory(idGenerator, this));
        registerValueType(BlobValueType.NAME, BlobValueType.factory());
        registerValueType(UriValueType.NAME, UriValueType.factory());
        registerValueType(ListValueType.NAME, ListValueType.factory(this));
        registerValueType(PathValueType.NAME, PathValueType.factory(this));
        registerValueType(RecordValueType.NAME, RecordValueType.factory(this));
        registerValueType(ByteArrayValueType.NAME, ByteArrayValueType.factory());
    }

    @Override
    public RecordTypeBuilder recordTypeBuilder() throws TypeException {
        return new RecordTypeBuilderImpl(this);
    }

    @Override
    public FieldTypeBuilder fieldTypeBuilder() throws TypeException {
        return new FieldTypeBuilderImpl(this);
    }

    @Override
    public org.lilyproject.repository.api.FieldTypes getFieldTypesSnapshot() throws InterruptedException {
        return new FieldTypesImpl();
    }

    @Override
    public List<FieldType> getFieldTypesWithoutCache() throws RepositoryException, InterruptedException {
        return Lists.newArrayList(getFieldTypes());
    }

    @Override
    public List<RecordType> getRecordTypesWithoutCache() throws RepositoryException, InterruptedException {
        return Lists.newArrayList(getRecordTypes());
    }

    @Override
    public TypeBucket getTypeBucketWithoutCache(String bucketId) throws RepositoryException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void enableSchemaCacheRefresh() throws RepositoryException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void disableSchemaCacheRefresh() throws RepositoryException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void triggerSchemaCacheRefresh() throws RepositoryException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isSchemaCacheRefreshEnabled() throws RepositoryException, InterruptedException {
        return false;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public Set<SchemaId> findSubtypes(SchemaId schemaId) throws InterruptedException, RepositoryException {
        return findSubTypes(schemaId, true);
    }

    @Override
    public Set<SchemaId> findDirectSubtypes(SchemaId schemaId) throws InterruptedException, RepositoryException {
        return findSubTypes(schemaId, false);
    }

    private Set<SchemaId> findSubTypes(SchemaId recordTypeId, boolean recursive)
            throws InterruptedException, RepositoryException {
        ArgumentValidator.notNull(recordTypeId, "recordTypeId");

        // This is to validate the requested ID exists
        getRecordTypeById(recordTypeId, null);

        Set<SchemaId> result = new HashSet<SchemaId>();
        collectSubTypes(recordTypeId, result, recursive);

        return result;
    }

    private void collectSubTypes(SchemaId recordTypeId, Set<SchemaId> result, boolean recursive)
            throws InterruptedException, RepositoryException {
        collectSubTypes(recordTypeId, result, new ArrayDeque<SchemaId>(), recursive);
    }

    private void collectSubTypes(SchemaId recordTypeId, Set<SchemaId> result, Deque<SchemaId> parents,
            boolean recursive) throws InterruptedException, RepositoryException {
        // the parent-stack is to protect against endless loops in the type hierarchy. If a type is a subtype
        // of itself, it will not be included in the result. Thus if record type A extends (directly or indirectly)
        // from A, and we search the subtypes of A, then the resulting set will not include A.
        parents.push(recordTypeId);
        Set<SchemaId> subtypes = getRecordTypeById(recordTypeId, null).getSupertypes().keySet();
        for (SchemaId subtype : subtypes) {
            if (!parents.contains(subtype)) {
                result.add(subtype);
                if (recursive) {
                    collectSubTypes(subtype, result, parents, recursive);
                }
            } else {
                // Loop detected in type hierarchy
                throw new RepositoryException(
                        "Error while refreshing subtypes of record type " + recordTypeId.toString());
            }
        }
        parents.pop();
    }

    private Set<QName> findSubTypes(QName recordTypeName, boolean recursive)
            throws InterruptedException, RepositoryException {
        ArgumentValidator.notNull(recordTypeName, "recordTypeName");

        RecordType recordType = getRecordTypeByName(recordTypeName, null);
        Set<SchemaId> result = new HashSet<SchemaId>();
        collectSubTypes(recordType.getId(), result, recursive);

        // Translate schema id's to QName's
        Set<QName> names = new HashSet<QName>();
        for (SchemaId id : result) {
            try {
                names.add(getRecordTypeById(id, null).getName());
            } catch (RecordTypeNotFoundException e) {
                // skip, this should only occur in border cases, i.e. the schema is being modified while
                // this method is called
            }
        }

        return names;
    }

    @Override
    public Set<QName> findSubtypes(QName qName) throws InterruptedException, RepositoryException {
        return findSubTypes(qName, true);
    }

    @Override
    public Set<QName> findDirectSubtypes(QName qName) throws InterruptedException, RepositoryException {
        return findSubTypes(qName, false);
    }

    @Override
    public RecordType updateRecordType(RecordType recordType, boolean b)
            throws RepositoryException, InterruptedException {
        return updateRecordType(recordType, b, new ArrayDeque<SchemaId>());
    }

    public RecordType createOrUpdateRecordType(RecordType recordType, boolean refreshSubtypes)
            throws RepositoryException, InterruptedException {
        if (recordType.getId() != null) {
            return updateRecordType(recordType, refreshSubtypes);
        } else {
            if (recordType.getName() == null) {
                throw new IllegalArgumentException("No id or name specified in supplied record type.");
            }

            boolean exists = this.recordTypeByName.containsKey(recordType.getName());


            if (exists) {
                try {
                    return updateRecordType(recordType, refreshSubtypes);
                } catch (RecordTypeNotFoundException e) {
                    // record type was renamed in the meantime, retry
                    exists = false;
                }
            } else {
                try {
                    return createRecordType(recordType);
                } catch (RecordTypeExistsException e) {
                    // record type was created in the meantime, retry
                    exists = true;
                }
            }
        }
        throw new TypeException("Record type create-or-update failed");
    }

    private RecordType updateRecordType(RecordType recordType, boolean refreshSubtypes, Deque<SchemaId> parents)
            throws RepositoryException, InterruptedException {
        // First update the record type
        RecordType updatedRecordType = updateRecordType(recordType);

        if (!refreshSubtypes) {
            return updatedRecordType;
        }

        parents.push(updatedRecordType.getId());

        try {
            Set<SchemaId> subtypes = findDirectSubtypes(updatedRecordType.getId());

            for (SchemaId subtype : subtypes) {
                if (!parents.contains(subtype)) {
                    RecordType subRecordType = getRecordTypeById(subtype, null);
                    for (Map.Entry<SchemaId, Long> supertype : subRecordType.getSupertypes().entrySet()) {
                        if (supertype.getKey().equals(updatedRecordType.getId())) {
                            if (!supertype.getValue().equals(updatedRecordType.getVersion())) {
                                subRecordType.addSupertype(updatedRecordType.getId(), updatedRecordType.getVersion());
                                // Store the change, and recursively adjust the pointers in this record type's subtypes as well
                                updateRecordType(subRecordType, true, parents);
                            }
                            break;
                        }
                    }
                } else {
                    // Loop detected in type hierarchy, log a warning about this
                    throw new RepositoryException("Loop in the record hierarchy");
                }
            }
        } catch (RepositoryException e) {
            throw new RepositoryException("Error while refreshing subtypes of record type " + recordType.getName(), e);
        }

        parents.pop();

        return updatedRecordType;
    }
}
